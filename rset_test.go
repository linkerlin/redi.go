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
