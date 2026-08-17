package redi_test

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
	"github.com/redis/go-redis/v9"
)

func TestMapCacheSurface(t *testing.T) {
	client := newTestClient(t)
	rc := rawClient(t)

	t.Run("PutWithoutTTL", func(t *testing.T) {
		m := client.GetMapCache(uniqueKey(t, "put"))
		defer m.Clear(testCtx) //nolint:errcheck

		if err := m.Put(testCtx, "key", "value"); err != nil {
			t.Fatal("Put:", err)
		}
		got, err := m.Get(testCtx, "key")
		if err != nil || got != "value" {
			t.Fatalf("Get = %v, %v; want value, nil", got, err)
		}
	})

	t.Run("GetAllUnpacksValues", func(t *testing.T) {
		m := client.GetMapCache(uniqueKey(t, "getall"))
		defer m.Clear(testCtx) //nolint:errcheck

		if err := m.Put(testCtx, "a", "one", 0, 0); err != nil {
			t.Fatal("Put a:", err)
		}
		if err := m.Put(testCtx, "b", "two", 0, 0); err != nil {
			t.Fatal("Put b:", err)
		}
		got, err := m.GetAll(testCtx)
		if err != nil {
			t.Fatal("GetAll:", err)
		}
		if got["a"] != "one" || got["b"] != "two" {
			t.Fatalf("GetAll = %#v", got)
		}
	})

	t.Run("FastRemoveCleansCompanions", func(t *testing.T) {
		name := uniqueKey(t, "fastremove")
		m := client.GetMapCache(name)
		defer m.Clear(testCtx) //nolint:errcheck

		if err := m.Put(testCtx, "key", "value", time.Minute, time.Minute); err != nil {
			t.Fatal("Put:", err)
		}
		n, err := m.FastRemove(testCtx, "key")
		if err != nil || n != 1 {
			t.Fatalf("FastRemove = %d, %v; want 1, nil", n, err)
		}
		for _, companion := range []string{
			"redisson__timeout__set:{" + name + "}",
			"redisson__idle__set:{" + name + "}",
		} {
			_, err := rc.ZScore(testCtx, companion, `"key"`).Result()
			if !errors.Is(err, redis.Nil) {
				t.Fatalf("ZScore(%q) error = %v; want redis.Nil", companion, err)
			}
		}
	})

	t.Run("ClearRemovesCompanions", func(t *testing.T) {
		name := uniqueKey(t, "clear")
		m := client.GetMapCache(name)

		if err := m.SetMaxSize(testCtx, 2); err != nil {
			t.Fatal("SetMaxSize:", err)
		}
		if err := m.Put(testCtx, "key", "value", time.Minute, time.Minute); err != nil {
			t.Fatal("Put:", err)
		}
		if err := m.Clear(testCtx); err != nil {
			t.Fatal("Clear:", err)
		}
		keys := []string{
			name,
			"redisson__timeout__set:{" + name + "}",
			"redisson__idle__set:{" + name + "}",
			"redisson__map_cache__last_access__set:{" + name + "}",
			"{" + name + "}:redisson_options",
		}
		n, err := rc.Exists(testCtx, keys...).Result()
		if err != nil || n != 0 {
			t.Fatalf("Exists after Clear = %d, %v; want 0, nil", n, err)
		}
	})

	t.Run("TrySetMaxSizeUsesRedissonOptions", func(t *testing.T) {
		name := uniqueKey(t, "try-max-size")
		m := client.GetMapCache(name)
		defer m.Clear(testCtx) //nolint:errcheck

		ok, err := m.TrySetMaxSize(testCtx, 2)
		if err != nil || !ok {
			t.Fatalf("TrySetMaxSize first = %v, %v; want true, nil", ok, err)
		}
		ok, err = m.TrySetMaxSize(testCtx, 3)
		if err != nil || ok {
			t.Fatalf("TrySetMaxSize second = %v, %v; want false, nil", ok, err)
		}
		options := "{" + name + "}:redisson_options"
		got, err := rc.HGetAll(testCtx, options).Result()
		if err != nil {
			t.Fatal("HGetAll options:", err)
		}
		if got["max-size"] != "2" || got["mode"] != "LRU" {
			t.Fatalf("options = %#v; want max-size=2 mode=LRU", got)
		}
	})

	t.Run("LRUEvictsLeastRecentlyUsed", func(t *testing.T) {
		name := uniqueKey(t, "lru")
		m := client.GetMapCache(name)
		defer m.Clear(testCtx) //nolint:errcheck

		if err := m.SetMaxSize(testCtx, 2); err != nil {
			t.Fatal("SetMaxSize:", err)
		}
		if err := m.Put(testCtx, "a", 1); err != nil {
			t.Fatal("Put a:", err)
		}
		if err := m.Put(testCtx, "b", 2); err != nil {
			t.Fatal("Put b:", err)
		}
		accessKey := "redisson__map_cache__last_access__set:{" + name + "}"
		if err := rc.ZAdd(testCtx, accessKey,
			redis.Z{Score: 1, Member: `"a"`},
			redis.Z{Score: 2, Member: `"b"`},
		).Err(); err != nil {
			t.Fatal("seed access order:", err)
		}
		if _, err := m.Get(testCtx, "a"); err != nil {
			t.Fatal("Get a:", err)
		}
		if _, err := m.FastPut(testCtx, "c", 3); err != nil {
			t.Fatal("FastPut c:", err)
		}
		if got, err := m.Get(testCtx, "b"); err != nil || got != nil {
			t.Fatalf("Get evicted b = %v, %v; want nil, nil", got, err)
		}
		if size, err := m.Size(testCtx); err != nil || size != 2 {
			t.Fatalf("Size = %d, %v; want 2, nil", size, err)
		}
	})

	t.Run("LFUEvictsLeastFrequentlyUsed", func(t *testing.T) {
		m := client.GetMapCache(uniqueKey(t, "lfu"))
		defer m.Clear(testCtx) //nolint:errcheck

		if err := m.SetMaxSizeMode(testCtx, 2, redi.EvictionModeLFU); err != nil {
			t.Fatal("SetMaxSizeMode:", err)
		}
		if err := m.Put(testCtx, "a", 1); err != nil {
			t.Fatal("Put a:", err)
		}
		if err := m.Put(testCtx, "b", 2); err != nil {
			t.Fatal("Put b:", err)
		}
		for range 2 {
			if _, err := m.Get(testCtx, "a"); err != nil {
				t.Fatal("Get a:", err)
			}
		}
		if err := m.Put(testCtx, "c", 3); err != nil {
			t.Fatal("Put c:", err)
		}
		if got, err := m.Get(testCtx, "b"); err != nil || got != nil {
			t.Fatalf("Get evicted b = %v, %v; want nil, nil", got, err)
		}
		if size, err := m.Size(testCtx); err != nil || size != 2 {
			t.Fatalf("Size = %d, %v; want 2, nil", size, err)
		}
	})

	t.Run("EntryExpirationSurface", func(t *testing.T) {
		name := uniqueKey(t, "entry-expiry")
		m := client.GetMapCache(name)
		defer m.Clear(testCtx) //nolint:errcheck

		if err := m.Put(testCtx, "key", "value"); err != nil {
			t.Fatal("Put:", err)
		}
		ok, err := m.ExpireEntry(testCtx, "key", time.Minute, 2*time.Minute)
		if err != nil || !ok {
			t.Fatalf("ExpireEntry = %v, %v; want true, nil", ok, err)
		}
		raw, err := rc.HGet(testCtx, name, `"key"`).Result()
		if err != nil || len(raw) < 16 {
			t.Fatalf("packed value = %q, %v", raw, err)
		}
		maxIdle := math.Float64frombits(binary.LittleEndian.Uint64([]byte(raw[:8])))
		if maxIdle != float64((2 * time.Minute).Milliseconds()) {
			t.Fatalf("packed maxIdle = %v; want %v", maxIdle, (2 * time.Minute).Milliseconds())
		}
		for _, companion := range []string{
			"redisson__timeout__set:{" + name + "}",
			"redisson__idle__set:{" + name + "}",
		} {
			if _, err := rc.ZScore(testCtx, companion, `"key"`).Result(); err != nil {
				t.Fatalf("ZScore(%q): %v", companion, err)
			}
		}

		ok, err = m.UpdateEntryExpiration(testCtx, "key", 0, 0)
		if err != nil || !ok {
			t.Fatalf("UpdateEntryExpiration = %v, %v; want true, nil", ok, err)
		}
		raw, err = rc.HGet(testCtx, name, `"key"`).Result()
		if err != nil {
			t.Fatal("HGet after UpdateEntryExpiration:", err)
		}
		maxIdle = math.Float64frombits(binary.LittleEndian.Uint64([]byte(raw[:8])))
		if maxIdle != 0 {
			t.Fatalf("packed maxIdle after clear = %v; want 0", maxIdle)
		}

		ok, err = m.ExpireEntryIfNotSet(testCtx, "key", time.Minute, 0)
		if err != nil || !ok {
			t.Fatalf("ExpireEntryIfNotSet first = %v, %v; want true, nil", ok, err)
		}
		ok, err = m.ExpireEntryIfNotSet(testCtx, "key", 2*time.Minute, 0)
		if err != nil || ok {
			t.Fatalf("ExpireEntryIfNotSet second = %v, %v; want false, nil", ok, err)
		}
		ok, err = m.ExpireEntry(testCtx, "missing", time.Minute, 0)
		if err != nil || ok {
			t.Fatalf("ExpireEntry missing = %v, %v; want false, nil", ok, err)
		}
	})
}
