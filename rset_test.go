package redi_test

import (
	"testing"
)

func TestRSet_AddContainsRemove(t *testing.T) {
	client := newTestClient(t)
	s := client.GetRSet(uniqueKey(t, "set"))
	defer s.Clear(testCtx) //nolint:errcheck

	if err := s.Add(testCtx, "a", "b", "c"); err != nil {
		t.Fatal("Add:", err)
	}

	ok, err := s.Contains(testCtx, "b")
	if err != nil || !ok {
		t.Fatalf("Contains = %v, %v; want true", ok, err)
	}

	sz, err := s.Size(testCtx)
	if err != nil || sz != 3 {
		t.Fatalf("Size = %d, %v; want 3", sz, err)
	}

	if err := s.Remove(testCtx, "b"); err != nil {
		t.Fatal("Remove:", err)
	}

	ok, _ = s.Contains(testCtx, "b")
	if ok {
		t.Error("Contains(b) after remove = true, want false")
	}
}

func TestRSet_CountedRandomMoveReadAll(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "set2")
	dest := uniqueKey(t, "set2-dst")
	s := client.GetRSet(name)
	d := client.GetRSet(dest)
	defer s.Clear(testCtx) //nolint:errcheck
	defer d.Clear(testCtx) //nolint:errcheck

	n, err := s.AddCounted(testCtx, "a", "b", "a")
	if err != nil || n != 2 {
		t.Fatalf("AddCounted = %d, %v; want 2", n, err)
	}
	n, err = s.AddCounted(testCtx, "a")
	if err != nil || n != 0 {
		t.Fatalf("AddCounted dup = %d, %v; want 0", n, err)
	}

	all, err := s.ReadAll(testCtx)
	if err != nil || len(all) != 2 {
		t.Fatalf("ReadAll = %v, %v", all, err)
	}

	one, err := s.Random(testCtx)
	if err != nil || one == nil {
		t.Fatalf("Random = %v, %v", one, err)
	}
	many, err := s.RandomN(testCtx, 5)
	if err != nil || len(many) == 0 {
		t.Fatalf("RandomN = %v, %v", many, err)
	}

	moved, err := s.Move(testCtx, dest, "a")
	if err != nil || !moved {
		t.Fatalf("Move = %v, %v; want true", moved, err)
	}
	ok, _ := d.Contains(testCtx, "a")
	if !ok {
		t.Fatal("destination missing moved member")
	}
	ok, _ = s.Contains(testCtx, "a")
	if ok {
		t.Fatal("source still has moved member")
	}

	popped, err := s.RemoveRandom(testCtx)
	if err != nil || popped != "b" {
		t.Fatalf("RemoveRandom = %v, %v; want b", popped, err)
	}
	n, err = s.RemoveCounted(testCtx, "b")
	if err != nil || n != 0 {
		t.Fatalf("RemoveCounted missing = %d, %v", n, err)
	}

	_ = s.Add(testCtx, "x", "y", "z")
	batch, err := s.RemoveRandomN(testCtx, 2)
	if err != nil || len(batch) != 2 {
		t.Fatalf("RemoveRandomN = %v, %v; want 2", batch, err)
	}
}

func TestRSet_SetOperations(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "set-ops")
	otherName := name + ":other"
	unionDest := name + ":union"
	diffDest := name + ":diff"
	interDest := name + ":inter"
	defer client.GetKeys().Delete(testCtx, name, otherName, unionDest, diffDest, interDest) //nolint:errcheck

	s := client.GetRSet(name)
	other := client.GetRSet(otherName)
	if err := s.Add(testCtx, "a", "b"); err != nil {
		t.Fatal(err)
	}
	if err := other.Add(testCtx, "b", "c"); err != nil {
		t.Fatal(err)
	}
	union, err := s.Union(testCtx, otherName)
	if err != nil || len(union) != 3 {
		t.Fatalf("Union = %v, %v", union, err)
	}
	diff, err := s.Diff(testCtx, otherName)
	if err != nil || len(diff) != 1 || diff[0] != "a" {
		t.Fatalf("Diff = %v, %v", diff, err)
	}
	inter, err := s.Intersection(testCtx, otherName)
	if err != nil || len(inter) != 1 || inter[0] != "b" {
		t.Fatalf("Intersection = %v, %v", inter, err)
	}
	if n, err := s.UnionStore(testCtx, unionDest, otherName); err != nil || n != 3 {
		t.Fatalf("UnionStore = %d, %v", n, err)
	}
	if n, err := s.DiffStore(testCtx, diffDest, otherName); err != nil || n != 1 {
		t.Fatalf("DiffStore = %d, %v", n, err)
	}
	if n, err := s.IntersectionStore(testCtx, interDest, otherName); err != nil || n != 1 {
		t.Fatalf("IntersectionStore = %d, %v", n, err)
	}
}
