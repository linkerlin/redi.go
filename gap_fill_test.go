package redi_test

import (
	"testing"
	"time"
)

func TestGapFill_RSetCache(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "set-cache")
	idleKey := "redisson__idle__set:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, idleKey) })
	cache := client.GetSetCache(name)
	rc := rawClient(t)

	for _, value := range []string{"a", "b", "c"} {
		if _, err := cache.Add(testCtx, value, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cache.Add(testCtx, "expired", 80*time.Millisecond, 0); err != nil {
		t.Fatal(err)
	}
	if !eventual(t, 2*time.Second, func() bool {
		values, _ := cache.ReadAll(testCtx)
		return len(values) == 3
	}) {
		t.Fatal("ReadAll retained an expired element")
	}

	if ok, err := cache.ContainsAll(testCtx, "a", "b"); err != nil || !ok {
		t.Fatalf("ContainsAll present = %v, %v", ok, err)
	}
	if ok, err := cache.ContainsAll(testCtx, "a", "missing"); err != nil || ok {
		t.Fatalf("ContainsAll missing = %v, %v", ok, err)
	}
	if values, err := cache.RandomN(testCtx, 10); err != nil || len(values) != 3 {
		t.Fatalf("RandomN = %v, %v", values, err)
	}
	if changed, err := cache.RemoveAll(testCtx, "b", "missing"); err != nil || !changed {
		t.Fatalf("RemoveAll = %v, %v", changed, err)
	}
	if changed, err := cache.RetainAll(testCtx, "a"); err != nil || !changed {
		t.Fatalf("RetainAll = %v, %v", changed, err)
	}
	if value, err := cache.RemoveRandom(testCtx); err != nil || value != "a" {
		t.Fatalf("RemoveRandom = %#v, %v", value, err)
	}
	if value, err := cache.Random(testCtx); err != nil || value != nil {
		t.Fatalf("Random empty = %#v, %v", value, err)
	}

	for _, value := range []string{"x", "y", "z"} {
		if _, err := cache.Add(testCtx, value, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	if values, err := cache.RemoveRandomN(testCtx, 2); err != nil || len(values) != 2 {
		t.Fatalf("RemoveRandomN = %v, %v", values, err)
	}
	if size, err := cache.Size(testCtx); err != nil || size != 1 {
		t.Fatalf("Size after RemoveRandomN = %d, %v", size, err)
	}

	if _, err := cache.Add(testCtx, "idle", time.Hour, time.Minute); err != nil {
		t.Fatal(err)
	}
	if n, _ := rc.ZCard(testCtx, idleKey).Result(); n != 1 {
		t.Fatalf("idle companion size = %d, want 1", n)
	}
	if err := cache.Clear(testCtx); err != nil {
		t.Fatal(err)
	}
	if n, err := rc.Exists(testCtx, name, idleKey).Result(); err != nil || n != 0 {
		t.Fatalf("Clear left cache keys: exists=%d err=%v", n, err)
	}
}

func TestGapFill_RBitSet(t *testing.T) {
	client := newTestClient(t)
	bits := client.GetBitSet(uniqueKey(t, "bitset"))
	defer bits.ClearAll(testCtx) //nolint:errcheck

	if err := bits.SetRange(testCtx, 2, 5); err != nil {
		t.Fatal(err)
	}
	values, err := bits.GetMany(testCtx, 1, 2, 3, 4, 5, 6)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{false, true, true, true, true, false}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("GetMany = %v, want %v", values, want)
		}
	}
	if err := bits.ClearRange(testCtx, 3, 4); err != nil {
		t.Fatal(err)
	}
	if err := bits.SetMany(testCtx, true, 0, 9); err != nil {
		t.Fatal(err)
	}
	if err := bits.SetMany(testCtx, false, 2); err != nil {
		t.Fatal(err)
	}
	if size, err := bits.Size(testCtx); err != nil || size != 10 {
		t.Fatalf("Size = %d, %v; want 10", size, err)
	}

	if previous, err := bits.BitFieldSet(testCtx, false, 4, 16, 10); err != nil || previous != 0 {
		t.Fatalf("BitFieldSet unsigned = %d, %v", previous, err)
	}
	if value, err := bits.BitFieldGet(testCtx, false, 4, 16); err != nil || value != 10 {
		t.Fatalf("BitFieldGet unsigned = %d, %v", value, err)
	}
	if value, err := bits.BitFieldIncrBy(testCtx, false, 4, 16, 3); err != nil || value != 13 {
		t.Fatalf("BitFieldIncrBy unsigned = %d, %v", value, err)
	}
	if _, err := bits.BitFieldSet(testCtx, true, 4, 24, -2); err != nil {
		t.Fatal(err)
	}
	if value, err := bits.BitFieldGet(testCtx, true, 4, 24); err != nil || value != -2 {
		t.Fatalf("BitFieldGet signed = %d, %v", value, err)
	}

	before, _ := bits.Get(testCtx, 0)
	if err := bits.Not(testCtx); err != nil {
		t.Fatal(err)
	}
	after, _ := bits.Get(testCtx, 0)
	if before == after {
		t.Fatal("Not didn't invert bit 0")
	}
}

func TestGapFill_RPermitExpirableSemaphore(t *testing.T) {
	client := newTestClient(t)
	semaphore := client.GetPermitExpirableSemaphore(uniqueKey(t, "permits"))
	defer semaphore.Delete(testCtx) //nolint:errcheck

	if ok, err := semaphore.TrySetPermits(testCtx, 2); err != nil || !ok {
		t.Fatalf("TrySetPermits = %v, %v", ok, err)
	}
	ids, err := semaphore.TryAcquireN(testCtx, 2, time.Minute)
	if err != nil || len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("TryAcquireN = %v, %v", ids, err)
	}
	if ids, err := semaphore.TryAcquireN(testCtx, 1, time.Minute); err != nil || len(ids) != 0 {
		t.Fatalf("TryAcquireN exhausted = %v, %v", ids, err)
	}

	start := time.Now()
	id, err := semaphore.TryAcquireWait(testCtx, time.Minute, 100*time.Millisecond)
	if err != nil || id != "" {
		t.Fatalf("TryAcquireWait exhausted = %q, %v", id, err)
	}
	if time.Since(start) < 75*time.Millisecond {
		t.Fatal("TryAcquireWait returned before its wait deadline")
	}
	if released, err := semaphore.ReleaseAll(testCtx, ids...); err != nil || released != 2 {
		t.Fatalf("ReleaseAll = %d, %v", released, err)
	}
	ids, err = semaphore.AcquireN(testCtx, 2, time.Minute)
	if err != nil || len(ids) != 2 {
		t.Fatalf("AcquireN = %v, %v", ids, err)
	}
}

func TestGapFill_RBloomFilterCollections(t *testing.T) {
	client := newTestClient(t)
	filter := client.GetBloomFilter(uniqueKey(t, "bloom"))
	defer filter.Delete(testCtx) //nolint:errcheck

	if ok, err := filter.TryInit(testCtx, 1000, 0.001); err != nil || !ok {
		t.Fatalf("TryInit = %v, %v", ok, err)
	}
	if added, err := filter.AddAll(testCtx, "a", "b", "a"); err != nil || added != 2 {
		t.Fatalf("AddAll = %d, %v; want 2", added, err)
	}
	if contained, err := filter.ContainsAll(testCtx, "a", "b", "missing"); err != nil || contained != 2 {
		t.Fatalf("ContainsAll = %d, %v; want 2", contained, err)
	}
	if added, err := filter.AddAll(testCtx, "a", "b"); err != nil || added != 0 {
		t.Fatalf("second AddAll = %d, %v; want 0", added, err)
	}
}

func TestGapFill_RRingBufferQueueSurface(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "ring")
	settings := "redisson_rb:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, settings) })
	ring := client.GetRingBuffer(name)
	rc := rawClient(t)

	if ok, err := ring.TrySetCapacity(testCtx, 3); err != nil || !ok {
		t.Fatalf("TrySetCapacity = %v, %v", ok, err)
	}
	for _, value := range []string{"a", "b", "c"} {
		if err := ring.Offer(testCtx, value); err != nil {
			t.Fatal(err)
		}
	}
	if value, err := ring.Peek(testCtx); err != nil || value != "a" {
		t.Fatalf("Peek = %#v, %v", value, err)
	}
	if value, err := ring.Poll(testCtx); err != nil || value != "a" {
		t.Fatalf("Poll = %#v, %v", value, err)
	}
	if values, err := ring.ReadAll(testCtx); err != nil ||
		len(values) != 2 || values[0] != "b" || values[1] != "c" {
		t.Fatalf("ReadAll = %v, %v", values, err)
	}
	if err := ring.Clear(testCtx); err != nil {
		t.Fatal(err)
	}
	if size, _ := ring.Size(testCtx); size != 0 {
		t.Fatalf("Size after Clear = %d", size)
	}
	if capacity, err := ring.Capacity(testCtx); err != nil || capacity != 3 {
		t.Fatalf("Capacity after Clear = %d, %v", capacity, err)
	}
	if exists, err := rc.Exists(testCtx, settings).Result(); err != nil || exists != 1 {
		t.Fatalf("capacity key after Clear = %d, %v", exists, err)
	}
}
