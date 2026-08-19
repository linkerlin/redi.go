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
	n, err := l.RemoveCounted(testCtx, 2, "x")
	if err != nil || n != 2 {
		t.Fatalf("RemoveCounted = %d, %v; want 2", n, err)
	}

	sz, _ := l.Size(testCtx)
	if sz != 1 {
		t.Errorf("after remove size = %d, want 1", sz)
	}
}

func TestRList_Surface(t *testing.T) {
	client := newTestClient(t)
	l := client.GetRList(uniqueKey(t, "surf"))
	defer l.Clear(testCtx) //nolint:errcheck

	lenAfter, err := l.AddCounted(testCtx, "a", "b", "c", "b")
	if err != nil || lenAfter != 4 {
		t.Fatalf("AddCounted = %d, %v; want 4", lenAfter, err)
	}

	all, err := l.ReadAll(testCtx)
	if err != nil || len(all) != 4 {
		t.Fatalf("ReadAll = %v, %v", all, err)
	}

	idx, err := l.IndexOf(testCtx, "b")
	if err != nil || idx != 1 {
		t.Fatalf("IndexOf = %d, %v; want 1", idx, err)
	}
	idx, err = l.LastIndexOf(testCtx, "b")
	if err != nil || idx != 3 {
		t.Fatalf("LastIndexOf = %d, %v; want 3", idx, err)
	}
	ok, err := l.Contains(testCtx, "c")
	if err != nil || !ok {
		t.Fatalf("Contains = %v, %v", ok, err)
	}

	n, err := l.AddBefore(testCtx, "b", "x")
	if err != nil || n != 5 {
		t.Fatalf("AddBefore = %d, %v; want 5", n, err)
	}
	// list: a, x, b, c, b
	n, err = l.AddAfter(testCtx, "c", "y")
	if err != nil || n != 6 {
		t.Fatalf("AddAfter = %d, %v; want 6", n, err)
	}

	v, err := l.RemoveByIndex(testCtx, 1) // remove x
	if err != nil || v != "x" {
		t.Fatalf("RemoveByIndex = %v, %v; want x", v, err)
	}
	if err := l.FastRemoveByIndex(testCtx, 0); err != nil { // remove a
		t.Fatal(err)
	}
	// remaining starts with b
	head, _ := l.Get(testCtx, 0)
	if head != "b" {
		t.Fatalf("head after removes = %v, want b", head)
	}

	if err := l.Trim(testCtx, 0, 1); err != nil {
		t.Fatal(err)
	}
	sz, _ := l.Size(testCtx)
	if sz != 2 {
		t.Fatalf("Size after Trim = %d, want 2", sz)
	}
	if err := l.FastSet(testCtx, 0, "z"); err != nil {
		t.Fatal(err)
	}
	z, _ := l.Get(testCtx, 0)
	if z != "z" {
		t.Fatalf("FastSet Get = %v", z)
	}
}

func TestRList_GetMany(t *testing.T) {
	client := newTestClient(t)
	l := client.GetRList(uniqueKey(t, "getmany"))
	defer l.Clear(testCtx) //nolint:errcheck

	if err := l.Add(testCtx, "a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	got, err := l.GetMany(testCtx, 2, 0, 9)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != "c" || got[1] != "a" || got[2] != nil {
		t.Fatalf("GetMany = %#v; want [c a <nil>]", got)
	}
	empty, err := l.GetMany(testCtx)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty GetMany = %#v, %v", empty, err)
	}
}
