package redi_test

import (
	"testing"
	"time"
)

func TestRSetMultimapCache_ExpireKeyEvicts(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "set-mm-cache")
	timeoutKey := "{" + name + "}:redisson_set_multimap_ttl"
	t.Cleanup(func() {
		interopCleanup(t, name, timeoutKey)
		interopCleanupPattern(t, "{"+name+"}:*")
	})

	cache := client.GetSetMultimapCache(name)
	if changed, err := cache.Put(testCtx, "key", "value"); err != nil || !changed {
		t.Fatalf("Put = %v, %v; want true", changed, err)
	}
	if ok, err := cache.ExpireKey(testCtx, "key", 80*time.Millisecond); err != nil || !ok {
		t.Fatalf("ExpireKey = %v, %v; want true", ok, err)
	}

	rc := rawClient(t)
	if _, err := rc.ZScore(testCtx, timeoutKey, `"key"`).Result(); err != nil {
		t.Fatalf("timeout member missing: %v", err)
	}
	id, err := rc.HGet(testCtx, name, `"key"`).Result()
	if err != nil {
		t.Fatal("multimap id:", err)
	}
	collectionKey := "{" + name + "}:" + id

	if !eventual(t, 2*time.Second, func() bool {
		values, getErr := cache.Get(testCtx, "key")
		if getErr != nil || len(values) != 0 {
			return false
		}
		hashExists, _ := rc.HExists(testCtx, name, `"key"`).Result()
		collectionExists, _ := rc.Exists(testCtx, collectionKey).Result()
		return !hashExists && collectionExists == 0
	}) {
		t.Fatal("expired key was not hidden and lazily evicted")
	}
}

func TestRListMultimapCache_PreservesOrderAndRefreshesExpiredKey(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "list-mm-cache")
	timeoutKey := "{" + name + "}:redisson_list_multimap_ttl"
	t.Cleanup(func() {
		interopCleanup(t, name, timeoutKey)
		interopCleanupPattern(t, "{"+name+"}:*")
	})

	cache := client.GetListMultimapCache(name)
	for _, value := range []string{"a", "b", "a"} {
		if _, err := cache.Put(testCtx, 42, value); err != nil {
			t.Fatal("Put:", err)
		}
	}
	values, err := cache.Get(testCtx, 42)
	if err != nil || !sameStrings(values, "a", "b", "a") {
		t.Fatalf("Get = %v, %v", values, err)
	}
	if ok, err := cache.ExpireKey(testCtx, 42, time.Nanosecond); err != nil || !ok {
		t.Fatalf("ExpireKey = %v, %v", ok, err)
	}
	if _, err := cache.Put(testCtx, 42, "fresh"); err != nil {
		t.Fatal("Put after expiry:", err)
	}
	values, err = cache.Get(testCtx, 42)
	if err != nil || !sameStrings(values, "fresh") {
		t.Fatalf("Get after expired-key refresh = %v, %v; want [fresh]", values, err)
	}
}
