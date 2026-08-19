package redi_test

import (
	"sync"
	"testing"
	"time"
)

func TestRBloomFilter(t *testing.T) {
	client := newTestClient(t)
	f := client.GetBloomFilter(uniqueKey(t, "bloom"))
	defer f.Delete(testCtx) //nolint:errcheck

	ok, err := f.TryInit(testCtx, 100, 0.01)
	if err != nil || !ok {
		t.Fatalf("TryInit = %v, %v", ok, err)
	}

	// Re-init on the same config key must be a no-op.
	ok, _ = f.TryInit(testCtx, 100, 0.01)
	if ok {
		t.Fatal("TryInit on initialized filter should return false")
	}

	iters, err := f.HashIterations(testCtx)
	if err != nil || iters != 7 { // floor(959/100*ln2+0.5) = 7
		t.Fatalf("hashIterations = %d, %v; want 7", iters, err)
	}

	if _, err := f.Add(testCtx, "apple"); err != nil {
		t.Fatal("Add:", err)
	}
	contains, err := f.Contains(testCtx, "apple")
	if err != nil || !contains {
		t.Fatalf("Contains(apple) = %v, %v; want true", contains, err)
	}
	contains, _ = f.Contains(testCtx, "banana")
	if contains {
		t.Log("banana false-positive (allowed with small filters)")
	}

	n, err := f.Count(testCtx)
	if err != nil || n < 1 {
		t.Fatalf("Count = %d, %v; want >= 1", n, err)
	}
}

func TestRIdGenerator(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "idgen")
	alloc := "{" + name + "}:allocation"
	interopCleanup(t, name, alloc)
	t.Cleanup(func() { interopCleanup(t, name, alloc) })
	g := client.GetIdGenerator(name)

	if _, err := g.TryInit(testCtx, 0, 5); err != nil {
		t.Fatal("TryInit:", err)
	}

	seen := make(map[int64]bool)
	for i := 0; i < 12; i++ {
		id, err := g.NextID(testCtx)
		if err != nil {
			t.Fatal("NextID:", err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
	}

	ids, err := g.NextIDs(testCtx, 7)
	if err != nil {
		t.Fatal("NextIDs:", err)
	}
	if len(ids) != 7 {
		t.Fatalf("NextIDs len = %d, want 7", len(ids))
	}
}

func TestRIdGeneratorConcurrent(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "idgen-concurrent")
	alloc := "{" + name + "}:allocation"
	interopCleanup(t, name, alloc)
	t.Cleanup(func() { interopCleanup(t, name, alloc) })
	g := client.GetIdGenerator(name)
	if ok, err := g.TryInit(testCtx, 0, 3); err != nil || !ok {
		t.Fatalf("TryInit = %v, %v", ok, err)
	}

	const workers = 32
	ids := make(chan int64, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := g.NextID(testCtx)
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[int64]struct{}, workers)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != workers {
		t.Fatalf("got %d ids, want %d", len(seen), workers)
	}
	if err := g.Delete(testCtx); err != nil {
		t.Fatal(err)
	}
	if n, err := client.Redis().Exists(testCtx, name, alloc).Result(); err != nil || n != 0 {
		t.Fatalf("Delete left %d keys, %v", n, err)
	}
}

func TestRMapCache_TTL(t *testing.T) {
	client := newTestClient(t)
	m := client.GetMapCache(uniqueKey(t, "mapcache"))
	defer m.Clear(testCtx) //nolint:errcheck

	if err := m.Put(testCtx, "ephemeral", "value", 300*time.Millisecond, 0); err != nil {
		t.Fatal("Put:", err)
	}
	if err := m.Put(testCtx, "stable", "value", 0, 0); err != nil {
		t.Fatal("Put:", err)
	}

	v, err := m.Get(testCtx, "ephemeral")
	if err != nil || v != "value" {
		t.Fatalf("Get before expiry = %v, %v", v, err)
	}

	rem, _ := m.RemainTTLForKey(testCtx, "ephemeral")
	if rem <= 0 || rem > 300*time.Millisecond {
		t.Fatalf("RemainTTLForKey = %v, want (0, 300ms]", rem)
	}

	if !eventual(t, 2*time.Second, func() bool {
		v, _ := m.Get(testCtx, "ephemeral")
		return v == nil
	}) {
		t.Fatal("entry did not expire")
	}

	v, _ = m.Get(testCtx, "stable")
	if v != "value" {
		t.Fatal("entry without TTL expired")
	}

	sz, err := m.Size(testCtx)
	if err != nil || sz != 1 {
		t.Fatalf("Size after expiry = %d, %v; want 1", sz, err)
	}
}

func TestRMapCache_RemainTTLMatchesJava(t *testing.T) {
	client := newTestClient(t)
	m := client.GetMapCache(uniqueKey(t, "mapcache-ttl"))
	defer m.Clear(testCtx) //nolint:errcheck

	missing, err := m.RemainTTLForKey(testCtx, "nope")
	if err != nil || missing != -2*time.Millisecond {
		t.Fatalf("missing RemainTTLForKey = %v, %v; want -2ms", missing, err)
	}

	if err := m.Put(testCtx, "stable", "v"); err != nil {
		t.Fatal(err)
	}
	none, err := m.RemainTTLForKey(testCtx, "stable")
	if err != nil || none != -time.Millisecond {
		t.Fatalf("no-expiry RemainTTLForKey = %v, %v; want -1ms", none, err)
	}

	if err := m.Put(testCtx, "idle", "v", 0, time.Minute); err != nil {
		t.Fatal(err)
	}
	idle, err := m.RemainTTLForKey(testCtx, "idle")
	if err != nil || idle <= 0 || idle > time.Minute {
		t.Fatalf("idle RemainTTLForKey = %v, %v; want (0, 1m]", idle, err)
	}

	if err := m.Put(testCtx, "ttl-only", "keep", time.Hour, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !eventual(t, 2*time.Second, func() bool {
		rem, _ := m.RemainTTLForKey(testCtx, "ttl-only")
		return rem == -2*time.Millisecond
	}) {
		t.Fatal("idle-expired RemainTTLForKey did not become -2ms")
	}
	still, err := m.GetWithTTLOnly(testCtx, "ttl-only")
	if err != nil || still != "keep" {
		t.Fatalf("GetWithTTLOnly after idle expiry = %v, %v; want keep", still, err)
	}
}
