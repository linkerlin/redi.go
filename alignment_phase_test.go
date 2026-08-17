package redi_test

import (
	"strings"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

func skipCmd(t *testing.T, err error, needles ...string) bool {
	t.Helper()
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	if !strings.Contains(s, "unknown command") {
		return false
	}
	for _, n := range needles {
		if strings.Contains(s, strings.ToLower(n)) {
			t.Skip(err)
			return true
		}
	}
	return false
}

func TestRJsonBucket_Core(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "jsonb")
	t.Cleanup(func() { interopCleanup(t, name) })
	b := client.GetJsonBucket(name)
	if err := b.Set(testCtx, map[string]any{"a": 1, "arr": []any{}}); err != nil {
		if skipCmd(t, err, "json.") {
			return
		}
		t.Fatal(err)
	}
	if _, err := b.SetPathNX(testCtx, "$.b", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ArrayAppend(testCtx, "$.arr", "i", "j"); err != nil {
		t.Fatal(err)
	}
	if n, err := b.ArrayLen(testCtx, "$.arr"); err != nil || n < 1 {
		t.Fatalf("ArrayLen = %d, %v", n, err)
	}
	if v, err := b.GetPath(testCtx, "$.a"); err != nil || v == nil {
		t.Fatalf("GetPath = %v, %v", v, err)
	}
	buckets := client.GetJsonBuckets()
	if err := buckets.Set(testCtx, map[string]any{name + ":2": map[string]any{"z": 1}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { interopCleanup(t, name+":2") })
}

func TestRVectorSet_Core(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "vset")
	t.Cleanup(func() { interopCleanup(t, name) })
	v := client.GetVectorSet(name)
	ok, err := v.Add(testCtx, "a", []float64{1, 0, 0})
	if err != nil {
		if skipCmd(t, err, "vadd", "vcard") {
			return
		}
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected add")
	}
	_, _ = v.Add(testCtx, "b", []float64{0.9, 0.1, 0})
	if n, err := v.Size(testCtx); err != nil || n < 1 {
		t.Fatalf("Size = %d, %v", n, err)
	}
	if dim, err := v.Dimensions(testCtx); err != nil || dim != 3 {
		t.Fatalf("Dimensions = %d, %v", dim, err)
	}
	if has, err := v.Contains(testCtx, "a"); err != nil || !has {
		t.Fatalf("Contains = %v, %v", has, err)
	}
	if _, err := v.SimilarByElement(testCtx, "a", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := v.SimilarByVector(testCtx, []float64{1, 0, 0}, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := v.GetVector(testCtx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Remove(testCtx, "b"); err != nil {
		t.Fatal(err)
	}
}

func TestRSearch_Core(t *testing.T) {
	client := newTestClient(t)
	idx := uniqueKey(t, "idx")
	prefix := uniqueKey(t, "doc:")
	t.Cleanup(func() {
		_ = client.GetSearch().DropIndex(testCtx, idx, true)
		interopCleanupPattern(t, prefix+"*")
	})
	s := client.GetSearch()
	err := s.CreateIndex(testCtx, idx, redi.IndexOptions{On: "HASH", Prefixes: []string{prefix}},
		redi.IndexField{Name: "title", Type: redi.IndexText})
	if err != nil {
		if skipCmd(t, err, "ft.") {
			return
		}
		t.Fatal(err)
	}
	doc := prefix + "1"
	if err := client.Redis().HSet(testCtx, doc, "title", "hello redis").Err(); err != nil {
		t.Fatal(err)
	}
	res, err := s.Search(testCtx, idx, "hello", redi.SearchOptions{LimitCount: 10})
	if err != nil {
		t.Fatal(err)
	}
	if res.Total < 1 {
		t.Fatalf("Search total = %d", res.Total)
	}
}

func TestRCircularBuffer_Core(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "acb")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	b := client.GetCircularBuffer(name)
	ok, err := b.TrySetCapacity(testCtx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("TrySetCapacity")
	}
	if err := b.AddAll(testCtx, "a", "b", "c"); err != nil {
		if skipCmd(t, err, "arring", "arlen") {
			return
		}
		t.Fatal(err)
	}
	if n, err := b.Size(testCtx); err != nil || n < 1 {
		t.Fatalf("Size = %d, %v", n, err)
	}
	if _, err := b.Get(testCtx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReadAll(testCtx); err != nil {
		t.Fatal(err)
	}
}

func TestRMaps_Set(t *testing.T) {
	client := newTestClient(t)
	a, b := uniqueKey(t, "maps-a"), uniqueKey(t, "maps-b")
	t.Cleanup(func() { interopCleanup(t, a, b) })
	if err := client.GetMaps().Set(testCtx, map[string]map[string]any{
		a: {"k": "v"},
		b: {"x": 1},
	}); err != nil {
		t.Fatal(err)
	}
	if v, err := client.GetMap(a).Get(testCtx, "k"); err != nil || v != "v" {
		t.Fatalf("map a = %v, %v", v, err)
	}
}

func TestE2E_Alignment_PhaseA(t *testing.T) {
	client := newTestClient(t)

	// RateLimiter keepAlive
	rl := client.GetRateLimiter(uniqueKey(t, "rl-ka"))
	t.Cleanup(func() { _ = rl.Delete(testCtx) })
	if err := rl.SetRateWithKeepAlive(testCtx, redi.RateTypeOverall, 5, time.Second, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	cfg, err := rl.GetConfig(testCtx)
	if err != nil || cfg.KeepAlive != 5*time.Second {
		t.Fatalf("KeepAlive = %#v, %v", cfg, err)
	}
	ttl, err := client.Redis().PTTL(testCtx, rl.Name()).Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("config PTTL = %v, %v", ttl, err)
	}

	// MultimapCache auto eviction
	mmc := client.GetSetMultimapCache(uniqueKey(t, "mmc-ev"))
	t.Cleanup(func() { interopCleanupPattern(t, "*"+mmc.Name()+"*") })
	mmc.StartAutoEviction(50 * time.Millisecond)
	if _, err := mmc.Put(testCtx, "k", "v"); err != nil {
		t.Fatal(err)
	}

	// LCM disable near cache
	lcm := client.GetLocalCachedMapWithOptions(uniqueKey(t, "lcm-off"), &redi.LocalCachedMapOptions{
		DisableNearCache: true,
	})
	t.Cleanup(func() { lcm.Destroy(); _ = lcm.Clear(testCtx) })
	if err := lcm.Put(testCtx, "a", "b"); err != nil {
		t.Fatal(err)
	}
	if keys := lcm.CachedKeys(); len(keys) != 0 {
		t.Fatalf("near cache should be empty: %v", keys)
	}

	// Spin / NonReentrant / AtomicDouble / BoundedBQ surface
	spin := client.GetSpinLock(uniqueKey(t, "spin"))
	t.Cleanup(func() { interopCleanup(t, spin.Name()) })
	if ok, err := spin.TryLock(testCtx, "h", time.Second); err != nil || !ok {
		t.Fatalf("spin = %v, %v", ok, err)
	}
	_ = spin.Unlock(testCtx, "h")

	nrl := client.GetNonReentrantLock(uniqueKey(t, "nrl2"))
	t.Cleanup(func() { interopCleanup(t, nrl.Name()) })
	if ok, err := nrl.TryLock(testCtx, "h", time.Second); err != nil || !ok {
		t.Fatal(err)
	}
	_ = nrl.Unlock(testCtx, "h")

	ad := client.GetAtomicDouble(uniqueKey(t, "ad2"))
	t.Cleanup(func() { interopCleanup(t, ad.Name()) })
	if _, err := ad.AddAndGet(testCtx, 1.25); err != nil {
		t.Fatal(err)
	}

	bq := client.GetBoundedBlockingQueue(uniqueKey(t, "bbq2"))
	t.Cleanup(func() { interopCleanupPattern(t, "*"+bq.Name()+"*") })
	if ok, err := bq.TrySetCapacity(testCtx, 2); err != nil || !ok {
		t.Fatalf("TrySetCapacity = %v, %v", ok, err)
	}
	if ok, err := bq.Offer(testCtx, "x"); err != nil || !ok {
		t.Fatalf("Offer = %v, %v", ok, err)
	}

	_ = client.ID()
	_ = client.Config()
	nodes := client.GetRedisNodes()
	if err := nodes.Ping(testCtx); err != nil {
		t.Fatal(err)
	}
	_ = client.GetQueueTransfer(uniqueKey(t, "qt"))
}
