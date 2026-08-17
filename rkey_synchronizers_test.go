package redi_test

import "testing"

func TestMapSetAndMultimap_KeySynchronizerNames(t *testing.T) {
	client := newTestClient(t)
	seedName := uniqueKey(t, "sync-name-seed")
	t.Cleanup(func() {
		interopCleanup(t, seedName)
		interopCleanupPattern(t, "{"+seedName+"}:*")
	})
	seed := client.GetSetMultimap(seedName)
	if _, err := seed.Put(testCtx, "key", "value"); err != nil {
		t.Fatal("seed Put:", err)
	}
	id, err := rawClient(t).HGet(testCtx, seedName, `"key"`).Result()
	if err != nil {
		t.Fatal("read verified key hash:", err)
	}

	assertNames := func(
		t *testing.T,
		baseName string,
		lockName, fairName, readWriteName, semaphoreName string,
	) {
		t.Helper()
		prefix := "{" + baseName + "}:" + id + ":"
		if lockName != prefix+"lock" {
			t.Errorf("GetLock name = %q; want %q", lockName, prefix+"lock")
		}
		if fairName != prefix+"fairlock" {
			t.Errorf("GetFairLock name = %q; want %q", fairName, prefix+"fairlock")
		}
		if readWriteName != prefix+"rw_lock" {
			t.Errorf("GetReadWriteLock name = %q; want %q", readWriteName, prefix+"rw_lock")
		}
		if semaphoreName != prefix+"semaphore" {
			t.Errorf("GetSemaphore name = %q; want %q", semaphoreName, prefix+"semaphore")
		}
	}

	mapName := uniqueKey(t, "sync-map")
	m := client.GetMap(mapName)
	assertNames(t, mapName,
		m.GetLock("key").Name(),
		m.GetFairLock("key").Name(),
		m.GetReadWriteLock("key").ReadLock().Name(),
		m.GetSemaphore("key").Name())

	setName := uniqueKey(t, "sync-set")
	s := client.GetSet(setName)
	assertNames(t, setName,
		s.GetLock("key").Name(),
		s.GetFairLock("key").Name(),
		s.GetReadWriteLock("key").ReadLock().Name(),
		s.GetSemaphore("key").Name())

	multimapName := uniqueKey(t, "sync-mm")
	multimap := client.GetSetMultimap(multimapName)
	assertNames(t, multimapName,
		multimap.GetLock("key").Name(),
		multimap.GetFairLock("key").Name(),
		multimap.GetReadWriteLock("key").ReadLock().Name(),
		multimap.GetSemaphore("key").Name())

	cacheName := uniqueKey(t, "sync-mm-cache")
	cache := client.GetListMultimapCache(cacheName)
	assertNames(t, cacheName,
		cache.GetLock("key").Name(),
		cache.GetFairLock("key").Name(),
		cache.GetReadWriteLock("key").ReadLock().Name(),
		cache.GetSemaphore("key").Name())
}
