package redi_test

import (
	"testing"
	"time"
)

func TestRLocalCachedMap_Basics(t *testing.T) {
	client := newTestClient(t)
	m := client.GetLocalCachedMap(uniqueKey(t, "lcm"))
	defer m.Destroy()
	defer m.Clear(testCtx) //nolint:errcheck

	if err := m.Put(testCtx, "k1", "v1"); err != nil {
		t.Fatal("Put:", err)
	}
	v, err := m.Get(testCtx, "k1")
	if err != nil || v != "v1" {
		t.Fatalf("Get = %v, %v", v, err)
	}
	// Now cached locally.
	if keys := m.CachedKeys(); len(keys) != 1 {
		t.Fatalf("CachedKeys = %v", keys)
	}

	// Remove clears both layers.
	if err := m.Remove(testCtx, "k1"); err != nil {
		t.Fatal("Remove:", err)
	}
	if keys := m.CachedKeys(); len(keys) != 0 {
		t.Fatalf("CachedKeys after remove = %v", keys)
	}
	v, _ = m.Get(testCtx, "k1")
	if v != nil {
		t.Fatalf("Get after remove = %v", v)
	}
}

func TestRLocalCachedMap_CrossInstanceInvalidation(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "lcm-x")
	m1 := client.GetLocalCachedMap(name)
	defer m1.Destroy()
	m2 := client.GetLocalCachedMap(name)
	defer m2.Destroy()
	defer m1.Clear(testCtx) //nolint:errcheck

	// m1 writes; m2 reads (populating m2's local cache).
	if err := m1.Put(testCtx, "shared", "first"); err != nil {
		t.Fatal(err)
	}
	if v, err := m2.Get(testCtx, "shared"); err != nil || v != "first" {
		t.Fatalf("m2 Get = %v, %v", v, err)
	}
	if keys := m2.CachedKeys(); len(keys) != 1 {
		t.Fatalf("m2 cache before invalidation = %v", keys)
	}

	// m1 overwrites -> m2's local entry must be invalidated.
	if err := m1.Put(testCtx, "shared", "second"); err != nil {
		t.Fatal(err)
	}
	if !eventual(t, 3*time.Second, func() bool {
		return len(m2.CachedKeys()) == 0
	}) {
		t.Fatal("m2 local cache not invalidated after m1 overwrite")
	}
	// m2 now reads the fresh value from Redis.
	if v, err := m2.Get(testCtx, "shared"); err != nil || v != "second" {
		t.Fatalf("m2 Get after invalidation = %v, %v", v, err)
	}

	// Remove broadcast.
	if err := m1.Remove(testCtx, "shared"); err != nil {
		t.Fatal(err)
	}
	if !eventual(t, 3*time.Second, func() bool {
		_, cached := false, false
		_ = cached
		return len(m2.CachedKeys()) == 0
	}) {
		t.Fatal("m2 local cache not invalidated after m1 remove")
	}

	// Full clear broadcast.
	if err := m1.Put(testCtx, "a", 1); err != nil {
		t.Fatal(err)
	}
	if err := m1.Put(testCtx, "b", 2); err != nil {
		t.Fatal(err)
	}
	// Populate m2 cache with both.
	_, _ = m2.Get(testCtx, "a")
	_, _ = m2.Get(testCtx, "b")
	if len(m2.CachedKeys()) != 2 {
		t.Fatalf("m2 cache = %v", m2.CachedKeys())
	}
	if err := m1.Clear(testCtx); err != nil {
		t.Fatal(err)
	}
	if !eventual(t, 3*time.Second, func() bool {
		return len(m2.CachedKeys()) == 0
	}) {
		t.Fatal("m2 local cache not cleared by m1 Clear broadcast")
	}
}

func TestRLocalCachedMap_LocalCacheLimit(t *testing.T) {
	client := newTestClient(t)
	m := client.GetLocalCachedMap(uniqueKey(t, "lcm-limit"))
	defer m.Destroy()
	defer m.Clear(testCtx) //nolint:errcheck

	m.SetLocalCacheLimit(0, 2) // at most 2 cached entries
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := m.Put(testCtx, k, k); err != nil {
			t.Fatal(err)
		}
	}
	// Puts store locally too; the third over the limit is not cached.
	if keys := m.CachedKeys(); len(keys) > 2 {
		t.Fatalf("cache exceeded limit: %v", keys)
	}
	m.ClearLocalCache()
	if keys := m.CachedKeys(); len(keys) != 0 {
		t.Fatalf("ClearLocalCache left %v", keys)
	}
}

// TestJavaInterop_RLocalCachedMap: the underlying data uses the RMap wire
// format, so Java reads Go-written entries directly (the invalidation
// broadcast itself is Go-internal - documented limitation).
func TestJavaInterop_RLocalCachedMap(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-lcm")
	t.Cleanup(func() { interopCleanup(t, name, name+":inval") })
	m := client.GetLocalCachedMap(name)
	defer m.Destroy()

	// Go writes through; Java reads the same HASH.
	if err := m.Put(testCtx, "feature", "enabled"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("map_get " + name + ` "feature"`); err != nil || reply["value"] != "enabled" {
		t.Fatalf("java read of local-cached-map entry = %v, %v", reply, err)
	}

	// Java writes; Go's Get falls back to Redis and observes it once the
	// local copy is absent (never cached here).
	if _, err := javaSend("map_put " + name + ` "jkey" "jvalue"`); err != nil {
		t.Fatal(err)
	}
	v, err := m.Get(testCtx, "jkey")
	if err != nil || v != "jvalue" {
		t.Fatalf("Go Get of java-written entry = %v, %v", v, err)
	}
}
