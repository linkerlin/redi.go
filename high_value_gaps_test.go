package redi_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

func TestHighValueGaps_MultimapCache(t *testing.T) {
	client := newTestClient(t)

	name := uniqueKey(t, "set-mm-cache")
	cache := client.GetSetMultimapCache(name)
	t.Cleanup(func() { _ = cache.Clear(testCtx) })
	_, _ = cache.Put(testCtx, "a", "one")
	_, _ = cache.Put(testCtx, "a", "two")
	_, _ = cache.Put(testCtx, "b", "expired")
	if ok, err := cache.ExpireKey(testCtx, "b", 30*time.Millisecond); err != nil || !ok {
		t.Fatalf("ExpireKey = %v, %v", ok, err)
	}
	if !eventual(t, time.Second, func() bool {
		entries, err := cache.Entries(testCtx)
		return err == nil && len(entries) == 2
	}) {
		t.Fatal("expired multimap entry remained visible")
	}

	old, err := cache.ReplaceValues(testCtx, "a", "two", "three", "three")
	if err != nil || len(old) != 2 {
		t.Fatalf("ReplaceValues old = %v, %v", old, err)
	}
	values, err := cache.Values(testCtx)
	if err != nil || len(values) != 2 {
		t.Fatalf("Values = %v, %v", values, err)
	}
	if keys, err := cache.KeySet(testCtx); err != nil || len(keys) != 1 {
		t.Fatalf("KeySet = %v, %v", keys, err)
	}

	id, err := client.Redis().HGet(testCtx, name, `"a"`).Result()
	if err != nil {
		t.Fatal("HGet multimap id:", err)
	}
	if ok, err := cache.Expire(testCtx, time.Minute); err != nil || !ok {
		t.Fatalf("Expire = %v, %v", ok, err)
	}
	for _, key := range []string{
		name,
		"{" + name + "}:redisson_set_multimap_ttl",
		"{" + name + "}:" + id,
	} {
		if ttl, err := client.Redis().PTTL(testCtx, key).Result(); err != nil || ttl <= 0 {
			t.Errorf("PTTL(%q) = %v, %v", key, ttl, err)
		}
	}

	list := client.GetListMultimapCache(uniqueKey(t, "list-mm-cache"))
	t.Cleanup(func() { _ = list.Clear(testCtx) })
	_, _ = list.PutAll(testCtx, "k", "old-1", "old-2")
	if _, err := list.ReplaceValues(testCtx, "k", "new", "new", "last"); err != nil {
		t.Fatal(err)
	}
	got, err := list.Get(testCtx, "k")
	if err != nil || !reflect.DeepEqual(got, []any{"new", "new", "last"}) {
		t.Fatalf("list values = %v, %v", got, err)
	}
}

func TestHighValueGaps_ScoredSortedSet(t *testing.T) {
	client := newTestClient(t)
	set := client.GetScoredSortedSet(uniqueKey(t, "scored"))
	t.Cleanup(func() { _ = set.Clear(testCtx) })

	if rank, err := set.AddAndGetRank(testCtx, "a", 1); err != nil || rank != 0 {
		t.Fatalf("AddAndGetRank(a) = %d, %v", rank, err)
	}
	if rank, err := set.AddAndGetRank(testCtx, "b", 2); err != nil || rank != 1 {
		t.Fatalf("AddAndGetRank(b) = %d, %v", rank, err)
	}
	added, err := set.AddAllIfAbsent(testCtx, map[any]float64{"a": 9, "c": 3})
	if err != nil || added != 1 {
		t.Fatalf("AddAllIfAbsent = %d, %v", added, err)
	}
	if ok, err := set.AddIfGreater(testCtx, "a", 0.5); err != nil || ok {
		t.Fatalf("AddIfGreater lower = %v, %v", ok, err)
	}
	if ok, err := set.AddIfGreater(testCtx, "a", 1.5); err != nil || !ok {
		t.Fatalf("AddIfGreater = %v, %v", ok, err)
	}
	if ok, err := set.AddIfLess(testCtx, "c", 4); err != nil || ok {
		t.Fatalf("AddIfLess higher = %v, %v", ok, err)
	}
	if rank, err := set.AddScoreAndGetRank(testCtx, "c", -2.5); err != nil || rank != 0 {
		t.Fatalf("AddScoreAndGetRank = %d, %v", rank, err)
	}
	first, err := set.FirstEntry(testCtx)
	if err != nil || first == nil || first.Member != "c" || first.Score != 0.5 {
		t.Fatalf("FirstEntry = %#v, %v", first, err)
	}
	last, err := set.LastEntry(testCtx)
	if err != nil || last == nil || last.Member != "b" || last.Score != 2 {
		t.Fatalf("LastEntry = %#v, %v", last, err)
	}
	if entries, err := set.EntryRange(testCtx, 0, -1); err != nil || len(entries) != 3 {
		t.Fatalf("EntryRange = %v, %v", entries, err)
	}
	if entries, err := set.PollFirstEntries(testCtx, 1); err != nil ||
		len(entries) != 1 || entries[0].Member != "c" {
		t.Fatalf("PollFirstEntries = %v, %v", entries, err)
	}
	if entries, err := set.PollLastEntries(testCtx, 2); err != nil ||
		len(entries) != 2 || entries[0].Member != "a" || entries[1].Member != "b" {
		t.Fatalf("PollLastEntries = %v, %v", entries, err)
	}
}

func TestHighValueGaps_StreamGroupControl(t *testing.T) {
	client := newTestClient(t)
	stream := client.GetStream(uniqueKey(t, "stream"))
	t.Cleanup(func() { _ = stream.Delete(testCtx) })

	id1, _ := stream.Add(testCtx, map[string]any{"v": "one"})
	id2, _ := stream.Add(testCtx, map[string]any{"v": "two"})
	if ok, err := stream.CreateGroup(testCtx, "g", "0"); err != nil || !ok {
		t.Fatalf("CreateGroup = %v, %v", ok, err)
	}
	if err := stream.UpdateGroupMessageID(testCtx, "g", id1); err != nil {
		t.Fatal("UpdateGroupMessageID:", err)
	}
	entries, err := stream.ReadGroup(testCtx, "g", "c1", 10, 0)
	if err != nil || len(entries) != 1 || entries[0].ID != id2 {
		t.Fatalf("ReadGroup after SETID = %v, %v", entries, err)
	}
	n, err := stream.Nack(testCtx, "g", redi.StreamNackFail, id2)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "unknown command") {
		t.Skip("Redis server doesn't support XNACK")
	}
	if err != nil || n != 1 {
		t.Fatalf("Nack = %d, %v", n, err)
	}
}

func TestHighValueGaps_MapSetAndSynchronizers(t *testing.T) {
	client := newTestClient(t)
	mapName := uniqueKey(t, "map")
	m := client.GetMap(mapName)
	t.Cleanup(func() { _ = m.Clear(testCtx) })
	if err := m.PutAll(testCtx, map[string]any{"one": "123", "two": 2}); err != nil {
		t.Fatal(err)
	}
	if size, err := m.ValueSize(testCtx, "one"); err != nil || size != 5 {
		t.Fatalf("ValueSize = %d, %v", size, err)
	}
	if keys, err := m.RandomKeys(testCtx, 2); err != nil || len(keys) != 2 {
		t.Fatalf("RandomKeys = %v, %v", keys, err)
	}
	if entries, err := m.RandomEntries(testCtx, 2); err != nil || len(entries) != 2 {
		t.Fatalf("RandomEntries = %v, %v", entries, err)
	}

	setName := uniqueKey(t, "set")
	set := client.GetSet(setName)
	other := client.GetSet(uniqueKey(t, "other-set"))
	t.Cleanup(func() {
		_ = set.Clear(testCtx)
		_ = other.Clear(testCtx)
	})
	if ok, err := set.TryAdd(testCtx, "a", "b"); err != nil || !ok {
		t.Fatalf("TryAdd = %v, %v", ok, err)
	}
	if ok, err := set.TryAdd(testCtx, "b", "c"); err != nil || ok {
		t.Fatalf("TryAdd overlap = %v, %v", ok, err)
	}
	if contains, _ := set.Contains(testCtx, "c"); contains {
		t.Fatal("TryAdd wasn't all-or-nothing")
	}
	_ = other.Add(testCtx, "b", "z")
	if count, err := set.CountIntersection(testCtx, other.Name()); err != nil || count != 1 {
		t.Fatalf("CountIntersection = %d, %v", count, err)
	}

	mapPrefix := strings.TrimSuffix(m.GetLock("one").Name(), "lock")
	if got := m.GetCountDownLatch("one").Name(); got != mapPrefix+"countdownlatch" {
		t.Fatalf("map latch synchronizer = %q", got)
	}
	setPrefix := strings.TrimSuffix(set.GetLock("a").Name(), "lock")
	if got := set.GetPermitExpirableSemaphore("a").Name(); got !=
		setPrefix+"permitexpirablesemaphore" {
		t.Fatalf("set permit synchronizer = %q", got)
	}
}

func TestHighValueGaps_TimeSeries(t *testing.T) {
	client := newTestClient(t)
	series := client.GetTimeSeries(uniqueKey(t, "series"))
	t.Cleanup(func() { _ = series.Delete(testCtx) })

	err := series.AddAll(testCtx,
		redi.TimeSeriesEntry{Timestamp: 10, Value: "a"},
		redi.TimeSeriesEntry{Timestamp: 20, Value: "b"},
		redi.TimeSeriesEntry{Timestamp: 30, Value: "c", Label: "label"},
		redi.TimeSeriesEntry{Timestamp: 40, Value: "d"},
	)
	if err != nil {
		t.Fatal("AddAll:", err)
	}
	if removed, err := series.RemoveRange(testCtx, 15, 25); err != nil || removed != 1 {
		t.Fatalf("RemoveRange = %d, %v", removed, err)
	}
	if value, err := series.GetAndRemove(testCtx, 10); err != nil || value != "a" {
		t.Fatalf("GetAndRemove = %v, %v", value, err)
	}
	entry, err := series.GetAndRemoveEntry(testCtx, 30)
	if err != nil || entry == nil || entry.Value != "c" || entry.Label != "label" {
		t.Fatalf("GetAndRemoveEntry = %#v, %v", entry, err)
	}
	if size, err := series.Size(testCtx); err != nil || size != 1 {
		t.Fatalf("Size = %d, %v", size, err)
	}
}

func TestHighValueGaps_Misc(t *testing.T) {
	client := newTestClient(t)

	sem := client.GetSemaphore(uniqueKey(t, "semaphore"))
	t.Cleanup(func() { _ = sem.Delete(testCtx) })
	if ok, err := sem.ReleaseIfExists(testCtx, 1); err != nil || ok {
		t.Fatalf("ReleaseIfExists missing = %v, %v", ok, err)
	}
	_, _ = sem.TrySetPermits(testCtx, 1)
	if ok, err := sem.ReleaseIfExists(testCtx, 2); err != nil || !ok {
		t.Fatalf("ReleaseIfExists = %v, %v", ok, err)
	}
	if permits, _ := sem.AvailablePermits(testCtx); permits != 3 {
		t.Fatalf("available permits = %d", permits)
	}

	rate := client.GetRateLimiter(uniqueKey(t, "rate"))
	t.Cleanup(func() { _ = rate.Delete(testCtx) })
	if ok, err := rate.UpdateRate(testCtx, redi.RateTypeOverall, 2, time.Minute); err != nil || ok {
		t.Fatalf("UpdateRate missing = %v, %v", ok, err)
	}
	if err := rate.SetRate(testCtx, redi.RateTypeOverall, 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	_, _ = rate.TryAcquire(testCtx, 1)
	if ok, err := rate.UpdateRate(testCtx, redi.RateTypeOverall, 3, time.Minute); err != nil || !ok {
		t.Fatalf("UpdateRate = %v, %v", ok, err)
	}
	if permits, err := rate.AvailablePermits(testCtx); err != nil || permits != 3 {
		t.Fatalf("rate permits = %d, %v", permits, err)
	}

	atomic := client.GetAtomicLong(uniqueKey(t, "atomic"))
	t.Cleanup(func() { _ = atomic.Delete(testCtx) })
	_, _ = atomic.Set(testCtx, 7)
	if ok, err := atomic.CompareAndDelete(testCtx, 6); err != nil || ok {
		t.Fatalf("CompareAndDelete mismatch = %v, %v", ok, err)
	}
	if ok, err := atomic.CompareAndDelete(testCtx, 7); err != nil || !ok {
		t.Fatalf("CompareAndDelete = %v, %v", ok, err)
	}

	deque := client.GetDeque(uniqueKey(t, "deque"))
	t.Cleanup(func() { _ = deque.Clear(testCtx) })
	if n, err := deque.AddFirstIfExists(testCtx, "missing"); err != nil || n != 0 {
		t.Fatalf("AddFirstIfExists missing = %d, %v", n, err)
	}
	_ = deque.AddLast(testCtx, "middle")
	_, _ = deque.AddFirstIfExists(testCtx, "first")
	_, _ = deque.AddLastIfExists(testCtx, "last")
	if values, err := deque.ReadAll(testCtx); err != nil ||
		!reflect.DeepEqual(values, []any{"first", "middle", "last"}) {
		t.Fatalf("deque = %v, %v", values, err)
	}
}

func TestHighValueGaps_MapCacheTTLOnlyAndBitSetOps(t *testing.T) {
	client := newTestClient(t)
	cache := client.GetMapCache(uniqueKey(t, "map-cache"))
	t.Cleanup(func() { _ = cache.Clear(testCtx) })
	_, _ = cache.FastPut(testCtx, "idle", "kept", 0, 30*time.Millisecond)
	_, _ = cache.FastPut(testCtx, "ttl", "gone", 30*time.Millisecond, 0)

	var values map[string]any
	if !eventual(t, time.Second, func() bool {
		var err error
		values, err = cache.GetAllWithTTLOnly(testCtx, "idle", "ttl")
		return err == nil && values["idle"] == "kept" && values["ttl"] == nil
	}) {
		t.Fatalf("GetAllWithTTLOnly = %v", values)
	}

	a := client.GetBitSet(uniqueKey(t, "bits-a"))
	b := client.GetBitSet(uniqueKey(t, "bits-b"))
	c := client.GetBitSet(uniqueKey(t, "bits-c"))
	t.Cleanup(func() {
		_ = a.ClearAll(testCtx)
		_ = b.ClearAll(testCtx)
		_ = c.ClearAll(testCtx)
	})
	reset := func() {
		_ = a.ClearAll(testCtx)
		_ = a.SetMany(testCtx, true, 1, 2, 4)
		_ = b.SetMany(testCtx, true, 2, 3, 4)
		_ = c.SetMany(testCtx, true, 4, 5)
	}
	reset()
	if _, err := a.Diff(testCtx, b.Name(), c.Name()); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "syntax") ||
			strings.Contains(strings.ToLower(err.Error()), "unknown") {
			t.Skip("Redis server doesn't support extended BITOP operations")
		}
		t.Fatal("Diff:", err)
	}
	if bits, _ := a.GetMany(testCtx, 1, 2, 3, 4, 5); !reflect.DeepEqual(
		bits, []bool{true, false, false, false, false},
	) {
		t.Fatalf("Diff bits = %v", bits)
	}
	reset()
	if _, err := a.AndOr(testCtx, b.Name(), c.Name()); err != nil {
		t.Fatal("AndOr:", err)
	}
	if bits, _ := a.GetMany(testCtx, 1, 2, 3, 4, 5); !reflect.DeepEqual(
		bits, []bool{false, true, false, true, false},
	) {
		t.Fatalf("AndOr bits = %v", bits)
	}
	reset()
	if _, err := a.SetExclusive(testCtx, b.Name(), c.Name()); err != nil {
		t.Fatal("SetExclusive:", err)
	}
	if bits, _ := a.GetMany(testCtx, 1, 2, 3, 4, 5); !reflect.DeepEqual(
		bits, []bool{true, false, true, false, true},
	) {
		t.Fatalf("SetExclusive bits = %v", bits)
	}
}
