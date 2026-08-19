package redi_test

import (
	"testing"
)

func TestRMap_PutGetDelete(t *testing.T) {
	client := newTestClient(t)
	m := client.GetRMap(uniqueKey(t, "map"))
	defer m.Delete(testCtx, "hello") //nolint:errcheck

	if err := m.Put(testCtx, "hello", "world"); err != nil {
		t.Fatal("Put:", err)
	}

	val, err := m.Get(testCtx, "hello")
	if err != nil {
		t.Fatal("Get:", err)
	}
	if val != "world" {
		t.Errorf("Get = %v, want %q", val, "world")
	}

	ok, err := m.ContainsKey(testCtx, "hello")
	if err != nil || !ok {
		t.Fatalf("ContainsKey = %v, %v; want true", ok, err)
	}

	if err := m.Delete(testCtx, "hello"); err != nil {
		t.Fatal("Delete:", err)
	}

	val, err = m.Get(testCtx, "hello")
	if err != nil {
		t.Fatal("Get after delete:", err)
	}
	if val != nil {
		t.Errorf("Get after delete = %v, want nil", val)
	}
}

func TestRMap_SizeAndAddAndGet(t *testing.T) {
	client := newTestClient(t)
	m := client.GetRMap(uniqueKey(t, "size"))
	defer m.Clear(testCtx) //nolint:errcheck

	_ = m.Put(testCtx, "a", 1)
	_ = m.Put(testCtx, "b", 2)

	sz, err := m.Size(testCtx)
	if err != nil || sz != 2 {
		t.Fatalf("Size = %d, %v; want 2", sz, err)
	}

	n, err := m.AddAndGet(testCtx, "a", 10)
	if err != nil || n != 11 {
		t.Fatalf("AddAndGet = %d, %v; want 11", n, err)
	}
}

func TestRMap_PutAll(t *testing.T) {
	client := newTestClient(t)
	m := client.GetRMap(uniqueKey(t, "putall"))
	defer m.Clear(testCtx) //nolint:errcheck

	if err := m.PutAll(testCtx, map[string]any{"x": 1, "y": "two"}); err != nil {
		t.Fatal(err)
	}
	sz, err := m.Size(testCtx)
	if err != nil || sz != 2 {
		t.Fatalf("Size = %d, %v; want 2", sz, err)
	}
	v, err := m.Get(testCtx, "y")
	if err != nil || v != "two" {
		t.Fatalf("Get y = %v, %v", v, err)
	}
	if err := m.PutAll(testCtx, nil); err != nil {
		t.Fatal("empty PutAll:", err)
	}
}

func TestRMap_FastPutReplaceKeys(t *testing.T) {
	client := newTestClient(t)
	m := client.GetRMap(uniqueKey(t, "surface"))
	defer m.Clear(testCtx) //nolint:errcheck

	neu, err := m.FastPut(testCtx, "k", "v1")
	if err != nil || !neu {
		t.Fatalf("FastPut new = %v, %v; want true", neu, err)
	}
	neu, err = m.FastPut(testCtx, "k", "v2")
	if err != nil || neu {
		t.Fatalf("FastPut update = %v, %v; want false", neu, err)
	}

	prev, err := m.Replace(testCtx, "missing", "x")
	if err != nil || prev != nil {
		t.Fatalf("Replace absent = %v, %v; want nil", prev, err)
	}
	prev, err = m.Replace(testCtx, "k", "v3")
	if err != nil || prev != "v2" {
		t.Fatalf("Replace = %v, %v; want v2", prev, err)
	}

	ok, err := m.ReplaceIf(testCtx, "k", "wrong", "nope")
	if err != nil || ok {
		t.Fatalf("ReplaceIf mismatch = %v, %v; want false", ok, err)
	}
	ok, err = m.ReplaceIf(testCtx, "k", "v3", "v4")
	if err != nil || !ok {
		t.Fatalf("ReplaceIf match = %v, %v; want true", ok, err)
	}
	v, _ := m.Get(testCtx, "k")
	if v != "v4" {
		t.Fatalf("Get after ReplaceIf = %v", v)
	}

	_ = m.Put(testCtx, "other", 1)
	keys, err := m.Keys(testCtx)
	if err != nil || len(keys) != 2 {
		t.Fatalf("Keys = %v, %v; want 2", keys, err)
	}
	seen := map[string]bool{}
	for _, k := range keys {
		seen[k] = true
	}
	if !seen["k"] || !seen["other"] {
		t.Fatalf("Keys missing entries: %v", keys)
	}
}

func TestRMap_FastRemovePutIfExistsValues(t *testing.T) {
	client := newTestClient(t)
	m := client.GetRMap(uniqueKey(t, "fast2"))
	defer m.Clear(testCtx) //nolint:errcheck

	ok, err := m.FastPutIfExists(testCtx, "a", "1")
	if err != nil || ok {
		t.Fatalf("FastPutIfExists absent = %v, %v; want false", ok, err)
	}
	_ = m.Put(testCtx, "a", "1")
	ok, err = m.FastPutIfExists(testCtx, "a", "2")
	if err != nil || !ok {
		t.Fatalf("FastPutIfExists present = %v, %v; want true", ok, err)
	}
	ok, err = m.FastReplace(testCtx, "a", "3")
	if err != nil || !ok {
		t.Fatalf("FastReplace = %v, %v; want true", ok, err)
	}
	v, _ := m.Get(testCtx, "a")
	if v != "3" {
		t.Fatalf("Get a = %#v, want \"3\"", v)
	}

	_ = m.Put(testCtx, "b", "x")
	vals, err := m.Values(testCtx)
	if err != nil || len(vals) != 2 {
		t.Fatalf("Values = %v, %v; want 2", vals, err)
	}

	n, err := m.FastRemove(testCtx, "a", "missing")
	if err != nil || n != 1 {
		t.Fatalf("FastRemove = %d, %v; want 1", n, err)
	}
	sz, _ := m.Size(testCtx)
	if sz != 1 {
		t.Fatalf("Size after FastRemove = %d", sz)
	}
}

// Regression: Clear used to call field Delete() with zero args (no-op).
func TestRMap_ClearDeletesRedisKey(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "clear")
	m := client.GetRMap(name)
	_ = m.Put(testCtx, "a", 1)
	_ = m.Put(testCtx, "b", 2)

	if err := m.Clear(testCtx); err != nil {
		t.Fatal("Clear:", err)
	}
	sz, err := m.Size(testCtx)
	if err != nil || sz != 0 {
		t.Fatalf("Size after Clear = %d, %v; want 0", sz, err)
	}
	ok, err := m.Exists(testCtx)
	if err != nil || ok {
		t.Fatalf("Exists after Clear = %v, %v; want false", ok, err)
	}
}

func TestRMap_RemoveAndRemoveIf(t *testing.T) {
	client := newTestClient(t)
	m := client.GetRMap(uniqueKey(t, "map-rm"))
	defer m.Clear(testCtx) //nolint:errcheck

	if err := m.Put(testCtx, "k", "v1"); err != nil {
		t.Fatal(err)
	}
	ok, err := m.RemoveIf(testCtx, "k", "nope")
	if err != nil || ok {
		t.Fatalf("RemoveIf mismatch = %v, %v; want false", ok, err)
	}
	v, err := m.Get(testCtx, "k")
	if err != nil || v != "v1" {
		t.Fatalf("Get after mismatch = %v, %v", v, err)
	}
	ok, err = m.RemoveIf(testCtx, "k", "v1")
	if err != nil || !ok {
		t.Fatalf("RemoveIf match = %v, %v; want true", ok, err)
	}
	v, err = m.Get(testCtx, "k")
	if err != nil || v != nil {
		t.Fatalf("Get after RemoveIf = %v, %v; want nil", v, err)
	}

	if err := m.Put(testCtx, "k", "v2"); err != nil {
		t.Fatal(err)
	}
	prev, err := m.Remove(testCtx, "k")
	if err != nil || prev != "v2" {
		t.Fatalf("Remove = %v, %v; want v2", prev, err)
	}
	prev, err = m.Remove(testCtx, "k")
	if err != nil || prev != nil {
		t.Fatalf("Remove absent = %v, %v; want nil", prev, err)
	}
}
