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
