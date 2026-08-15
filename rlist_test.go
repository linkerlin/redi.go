package redi_test

import (
	"testing"
)

func TestRList_AddGetSize(t *testing.T) {
	client := newTestClient(t)
	l := client.GetRList(uniqueKey(t, "list"))
	defer l.Clear(testCtx) //nolint:errcheck

	if err := l.Add(testCtx, "a", "b", "c"); err != nil {
		t.Fatal("Add:", err)
	}

	sz, err := l.Size(testCtx)
	if err != nil || sz != 3 {
		t.Fatalf("Size = %d, %v; want 3", sz, err)
	}

	val, err := l.Get(testCtx, 1)
	if err != nil {
		t.Fatal("Get:", err)
	}
	if val != "b" {
		t.Errorf("Get(1) = %v, want %q", val, "b")
	}

	all, err := l.Range(testCtx, 0, -1)
	if err != nil {
		t.Fatal("Range:", err)
	}
	if len(all) != 3 {
		t.Errorf("Range len = %d, want 3", len(all))
	}
}

func TestRList_Remove(t *testing.T) {
	client := newTestClient(t)
	l := client.GetRList(uniqueKey(t, "rm"))
	defer l.Clear(testCtx) //nolint:errcheck

	_ = l.Add(testCtx, "x", "x", "y")
	_ = l.Remove(testCtx, 2, "x")

	sz, _ := l.Size(testCtx)
	if sz != 1 {
		t.Errorf("after remove size = %d, want 1", sz)
	}
}
