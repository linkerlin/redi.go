package redi_test

import (
	"context"
	"testing"
	"time"
)

func TestSyncAPIGaps_Collections(t *testing.T) {
	client := newTestClient(t)

	t.Run("SetCacheConditions", func(t *testing.T) {
		cache := client.GetSetCache(uniqueKey(t, "setcache"))
		defer cache.Clear(testCtx) //nolint:errcheck

		if ok, err := cache.AddIfAbsent(testCtx, "a", time.Minute); err != nil || !ok {
			t.Fatalf("AddIfAbsent = %v, %v", ok, err)
		}
		if ok, err := cache.TryAdd(testCtx, "a", time.Minute); err != nil || ok {
			t.Fatalf("TryAdd existing = %v, %v", ok, err)
		}
		if ok, err := cache.AddIfLess(testCtx, "a", 2*time.Minute); err != nil || ok {
			t.Fatalf("AddIfLess longer = %v, %v", ok, err)
		}
		if ok, err := cache.AddIfLess(testCtx, "a", 30*time.Second); err != nil || !ok {
			t.Fatalf("AddIfLess shorter = %v, %v", ok, err)
		}
		if ok, err := cache.AddIfGreater(testCtx, "a", time.Minute); err != nil || !ok {
			t.Fatalf("AddIfGreater = %v, %v", ok, err)
		}
		if ok, err := cache.AddIfExists(testCtx, "missing", time.Minute); err != nil || ok {
			t.Fatalf("AddIfExists missing = %v, %v", ok, err)
		}
	})

	t.Run("MultimapFastAndEntries", func(t *testing.T) {
		m := client.GetListMultimap(uniqueKey(t, "multimap"))
		defer m.Clear(testCtx) //nolint:errcheck
		_, _ = m.Put(testCtx, "a", "shared")
		_, _ = m.Put(testCtx, "a", "other")
		_, _ = m.Put(testCtx, "b", "shared")

		entries, err := m.Entries(testCtx)
		if err != nil || len(entries) != 3 {
			t.Fatalf("Entries = %v, %v", entries, err)
		}
		if n, err := m.FastRemoveValue(testCtx, "shared"); err != nil || n != 2 {
			t.Fatalf("FastRemoveValue = %d, %v", n, err)
		}
		if n, err := m.FastRemove(testCtx, "a", "missing"); err != nil || n != 1 {
			t.Fatalf("FastRemove = %d, %v", n, err)
		}
	})

	t.Run("ScoredSortedSetConditions", func(t *testing.T) {
		set := client.GetScoredSortedSet(uniqueKey(t, "scored"))
		defer set.Clear(testCtx) //nolint:errcheck
		if ok, err := set.AddIfAbsent(testCtx, "a", 2); err != nil || !ok {
			t.Fatalf("AddIfAbsent = %v, %v", ok, err)
		}
		if ok, err := set.TryAdd(testCtx, "a", 3); err != nil || ok {
			t.Fatalf("TryAdd existing = %v, %v", ok, err)
		}
		if ok, err := set.AddIfExists(testCtx, "a", 1); err != nil || !ok {
			t.Fatalf("AddIfExists = %v, %v", ok, err)
		}
		_, _ = set.AddIfAbsent(testCtx, "b", 4)
		if contains, err := set.Contains(testCtx, "a"); err != nil || !contains {
			t.Fatalf("Contains = %v, %v", contains, err)
		}
		if score, err := set.FirstScore(testCtx); err != nil || score != 1 {
			t.Fatalf("FirstScore = %v, %v", score, err)
		}
		if score, err := set.LastScore(testCtx); err != nil || score != 4 {
			t.Fatalf("LastScore = %v, %v", score, err)
		}
		if value, err := set.Random(testCtx); err != nil || value == nil {
			t.Fatalf("Random = %v, %v", value, err)
		}
		if values, err := set.RandomN(testCtx, 2); err != nil || len(values) != 2 {
			t.Fatalf("RandomN = %v, %v", values, err)
		}
	})

	t.Run("MapCacheDurationsAndBatchExpiry", func(t *testing.T) {
		cache := client.GetMapCache(uniqueKey(t, "mapcache"))
		defer cache.Clear(testCtx) //nolint:errcheck
		if ok, err := cache.FastPut(testCtx, "a", "one", time.Minute, 0); err != nil || !ok {
			t.Fatalf("FastPut = %v, %v", ok, err)
		}
		if ok, err := cache.FastPutIfAbsent(testCtx, "a", "two", time.Minute, 0); err != nil || ok {
			t.Fatalf("FastPutIfAbsent existing = %v, %v", ok, err)
		}
		if ok, err := cache.FastPut(testCtx, "b", "two"); err != nil || !ok {
			t.Fatalf("FastPut b = %v, %v", ok, err)
		}
		if n, err := cache.ExpireEntries(
			testCtx, []string{"a", "b", "missing"}, 2*time.Minute, time.Minute,
		); err != nil || n != 2 {
			t.Fatalf("ExpireEntries = %d, %v", n, err)
		}
		if n, err := cache.ExpireEntriesIfNotSet(
			testCtx, []string{"a", "b"}, time.Minute, 0,
		); err != nil || n != 0 {
			t.Fatalf("ExpireEntriesIfNotSet = %d, %v", n, err)
		}
	})

	t.Run("LocalCacheSnapshots", func(t *testing.T) {
		name := uniqueKey(t, "local")
		remote := client.GetMap(name)
		local := client.GetLocalCachedMap(name)
		defer local.Clear(testCtx) //nolint:errcheck
		if err := remote.PutAll(testCtx, map[string]any{"a": "one", "b": "two"}); err != nil {
			t.Fatal(err)
		}
		if err := local.PreloadCache(testCtx); err != nil {
			t.Fatal("PreloadCache:", err)
		}
		if values := local.CachedValues(); len(values) != 2 {
			t.Fatalf("CachedValues = %v", values)
		}
		if snapshot := local.GetCachedMap(); snapshot["a"] != "one" || snapshot["b"] != "two" {
			t.Fatalf("GetCachedMap = %v", snapshot)
		}
	})

	t.Run("BucketsConditions", func(t *testing.T) {
		buckets := client.GetBuckets()
		base := uniqueKey(t, "buckets")
		k1, k2 := base+":1", base+":2"
		defer client.GetKeys().Delete(testCtx, k1, k2) //nolint:errcheck
		if ok, err := buckets.SetIfAllKeysAbsent(
			testCtx, map[string]any{k1: "one", k2: "two"},
		); err != nil || !ok {
			t.Fatalf("SetIfAllKeysAbsent = %v, %v", ok, err)
		}
		if ok, err := buckets.SetIfAllKeysAbsent(
			testCtx, map[string]any{k1: "x", k2: "y"},
		); err != nil || ok {
			t.Fatalf("SetIfAllKeysAbsent existing = %v, %v", ok, err)
		}
		if ok, err := buckets.SetIfAllKeysExist(
			testCtx, map[string]any{k1: "x", k2: "y"},
		); err != nil || !ok {
			t.Fatalf("SetIfAllKeysExist = %v, %v", ok, err)
		}
	})
}

func TestSyncAPIGaps_Coordination(t *testing.T) {
	client := newTestClient(t)

	t.Run("PermitCounts", func(t *testing.T) {
		s := client.GetPermitExpirableSemaphore(uniqueKey(t, "permits"))
		defer s.Delete(testCtx) //nolint:errcheck
		_, _ = s.TrySetPermits(testCtx, 2)
		permit, err := s.TryAcquire(testCtx, time.Minute)
		if err != nil || permit == "" {
			t.Fatalf("TryAcquire = %q, %v", permit, err)
		}
		if n, err := s.GetPermits(testCtx); err != nil || n != 2 {
			t.Fatalf("GetPermits = %d, %v", n, err)
		}
		if err := s.SetPermits(testCtx, 3); err != nil {
			t.Fatal("SetPermits:", err)
		}
		if err := s.AddPermits(testCtx, 1); err != nil {
			t.Fatal("AddPermits:", err)
		}
		if n, err := s.GetPermits(testCtx); err != nil || n != 4 {
			t.Fatalf("GetPermits after changes = %d, %v", n, err)
		}
	})

	t.Run("AtomicConditions", func(t *testing.T) {
		long := client.GetAtomicLong(uniqueKey(t, "long"))
		defer long.Delete(testCtx) //nolint:errcheck
		_, _ = long.Set(testCtx, 5)
		if ok, err := long.SetIfLess(testCtx, 6, 10); err != nil || !ok {
			t.Fatalf("long SetIfLess = %v, %v", ok, err)
		}
		if ok, err := long.SetIfGreater(testCtx, 9, 4); err != nil || !ok {
			t.Fatalf("long SetIfGreater = %v, %v", ok, err)
		}

		double := client.GetAtomicDouble(uniqueKey(t, "double"))
		defer double.Delete(testCtx) //nolint:errcheck
		_, _ = double.Set(testCtx, 2.5)
		if previous, err := double.GetAndIncrement(testCtx); err != nil || previous != 2.5 {
			t.Fatalf("GetAndIncrement = %v, %v", previous, err)
		}
		if previous, err := double.GetAndDecrement(testCtx); err != nil || previous != 3.5 {
			t.Fatalf("GetAndDecrement = %v, %v", previous, err)
		}
		if ok, err := double.SetIfLess(testCtx, 3, 8); err != nil || !ok {
			t.Fatalf("double SetIfLess = %v, %v", ok, err)
		}
		if ok, err := double.SetIfGreater(testCtx, 7, 1); err != nil || !ok {
			t.Fatalf("double SetIfGreater = %v, %v", ok, err)
		}
	})

	t.Run("LockStatusAndWait", func(t *testing.T) {
		fair := client.GetFairLock(uniqueKey(t, "fair"))
		defer fair.ForceUnlock(testCtx) //nolint:errcheck
		if err := fair.Lock(testCtx, "owner", time.Minute); err != nil {
			t.Fatal(err)
		}
		if count, err := fair.HoldCount(testCtx, "owner"); err != nil || count != 1 {
			t.Fatalf("fair HoldCount = %d, %v", count, err)
		}
		if held, err := fair.IsHeldBy(testCtx, "owner"); err != nil || !held {
			t.Fatalf("fair IsHeldBy = %v, %v", held, err)
		}
		if ttl, err := fair.RemainTimeToLive(testCtx); err != nil || ttl <= 0 {
			t.Fatalf("fair TTL = %v, %v", ttl, err)
		}
		if ok, err := fair.TryLockWait(testCtx, "waiter", 25*time.Millisecond, time.Minute); err != nil || ok {
			t.Fatalf("fair TryLockWait = %v, %v", ok, err)
		}

		non := client.GetNonReentrantLock(uniqueKey(t, "non"))
		if ok, err := non.TryLock(testCtx, "owner", time.Minute); err != nil || !ok {
			t.Fatalf("non TryLock = %v, %v", ok, err)
		}
		if ttl, err := non.RemainTimeToLive(testCtx); err != nil || ttl <= 0 {
			t.Fatalf("non TTL = %v, %v", ttl, err)
		}
		if ok, err := non.ForceUnlock(testCtx); err != nil || !ok {
			t.Fatalf("non ForceUnlock = %v, %v", ok, err)
		}
	})

	t.Run("ReadWriteLockStatusAndWait", func(t *testing.T) {
		rw := client.GetReadWriteLock(uniqueKey(t, "rw"))
		read, write := rw.ReadLock(), rw.WriteLock()
		defer read.ForceUnlock(testCtx) //nolint:errcheck
		if err := read.Lock(testCtx, "reader", time.Minute); err != nil {
			t.Fatal(err)
		}
		if ok, err := read.TryLock(testCtx, "reader", time.Minute); err != nil || !ok {
			t.Fatalf("read reentry = %v, %v", ok, err)
		}
		if count, err := read.HoldCount(testCtx, "reader"); err != nil || count != 2 {
			t.Fatalf("read HoldCount = %d, %v", count, err)
		}
		if ttl, err := read.RemainTimeToLive(testCtx); err != nil || ttl <= 0 {
			t.Fatalf("read TTL = %v, %v", ttl, err)
		}
		if ok, err := write.TryLockWait(testCtx, "writer", 25*time.Millisecond, time.Minute); err != nil || ok {
			t.Fatalf("write TryLockWait = %v, %v", ok, err)
		}
	})

	t.Run("RateLimiterWait", func(t *testing.T) {
		limiter := client.GetRateLimiter(uniqueKey(t, "rate"))
		defer limiter.Delete(testCtx) //nolint:errcheck
		if err := limiter.SetRate(testCtx, 0, 1, 150*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		_, _ = limiter.TryAcquire(testCtx, 1)
		if ok, err := limiter.TryAcquireWait(testCtx, 1, 25*time.Millisecond); err != nil || ok {
			t.Fatalf("short TryAcquireWait = %v, %v", ok, err)
		}
		if ok, err := limiter.TryAcquireWait(testCtx, 1, time.Second); err != nil || !ok {
			t.Fatalf("long TryAcquireWait = %v, %v", ok, err)
		}
	})
}

func TestSyncAPIGaps_StreamAndTopics(t *testing.T) {
	client := newTestClient(t)

	t.Run("StreamFastClaims", func(t *testing.T) {
		stream := client.GetStream(uniqueKey(t, "stream"))
		defer stream.Delete(testCtx) //nolint:errcheck
		id1, _ := stream.Add(testCtx, map[string]any{"v": "one"})
		_, _ = stream.Add(testCtx, map[string]any{"v": "two"})
		if ok, err := stream.CreateGroup(testCtx, "g", "0"); err != nil || !ok {
			t.Fatalf("CreateGroup = %v, %v", ok, err)
		}
		if entries, err := stream.ReadGroup(testCtx, "g", "c1", 2, 0); err != nil || len(entries) != 2 {
			t.Fatalf("ReadGroup = %v, %v", entries, err)
		}
		if ids, err := stream.FastClaim(testCtx, "g", "c2", 0, id1); err != nil || len(ids) != 1 {
			t.Fatalf("FastClaim = %v, %v", ids, err)
		}
		if ids, _, err := stream.FastAutoClaim(testCtx, "g", "c3", 0, "0-0", 10); err != nil || len(ids) == 0 {
			t.Fatalf("FastAutoClaim = %v, %v", ids, err)
		}
	})

	t.Run("TopicListenerManagement", func(t *testing.T) {
		topic := client.GetTopic(uniqueKey(t, "topic"))
		if _, err := topic.Subscribe(func(any) {}); err != nil {
			t.Fatal(err)
		}
		if topic.CountListeners() != 1 || len(topic.ChannelNames()) != 1 {
			t.Fatalf("topic listeners/channels = %d/%v", topic.CountListeners(), topic.ChannelNames())
		}
		topic.RemoveAllListeners()
		if topic.CountListeners() != 0 {
			t.Fatal("topic listeners remain")
		}

		pattern := client.GetPatternTopic(uniqueKey(t, "pattern") + "*")
		if _, err := pattern.Subscribe(func(string, any) {}); err != nil {
			t.Fatal(err)
		}
		if pattern.CountListeners() != 1 {
			t.Fatal("pattern listener missing")
		}
		pattern.RemoveAllListeners()
		if pattern.CountListeners() != 0 {
			t.Fatal("pattern listeners remain")
		}

		reliable := client.GetReliableTopic(uniqueKey(t, "reliable"))
		defer reliable.Delete(context.Background()) //nolint:errcheck
		if _, err := reliable.Subscribe(func(any) {}); err != nil {
			t.Fatal(err)
		}
		if reliable.CountListeners() != 1 {
			t.Fatal("reliable listener missing")
		}
		reliable.RemoveAllListeners()
		if reliable.CountListeners() != 0 {
			t.Fatal("reliable listeners remain")
		}
	})
}
