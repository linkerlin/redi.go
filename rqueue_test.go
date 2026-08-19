package redi_test

import (
	"testing"
)

func TestRQueue_OfferPollPeek(t *testing.T) {
	client := newTestClient(t)
	q := client.GetRQueue(uniqueKey(t, "queue"))
	defer q.Clear(testCtx) //nolint:errcheck

	if err := q.Offer(testCtx, "first", "second"); err != nil {
		t.Fatal("Offer:", err)
	}

	head, err := q.Peek(testCtx)
	if err != nil {
		t.Fatal("Peek:", err)
	}
	if head != "first" {
		t.Errorf("Peek = %v, want %q", head, "first")
	}

	val, err := q.Poll(testCtx)
	if err != nil {
		t.Fatal("Poll:", err)
	}
	if val != "first" {
		t.Errorf("Poll = %v, want %q", val, "first")
	}

	sz, _ := q.Size(testCtx)
	if sz != 1 {
		t.Errorf("Size after poll = %d, want 1", sz)
	}

	_, _ = q.Poll(testCtx)
	val, err = q.Poll(testCtx)
	if err != nil {
		t.Fatal("Poll on empty:", err)
	}
	if val != nil {
		t.Errorf("Poll on empty = %v, want nil", val)
	}
}

func TestRQueue_IndexOfPollNAndMove(t *testing.T) {
	client := newTestClient(t)
	src := uniqueKey(t, "queue-n")
	dst := uniqueKey(t, "queue-dst")
	q := client.GetQueue(src)
	other := client.GetQueue(dst)
	t.Cleanup(func() { _ = q.Clear(testCtx); _ = other.Clear(testCtx) })

	if err := q.Offer(testCtx, "a", "b", "c", "d"); err != nil {
		t.Fatal(err)
	}
	idx, err := q.IndexOf(testCtx, "c")
	if err != nil || idx != 2 {
		t.Fatalf("IndexOf = %d, %v; want 2", idx, err)
	}
	missing, err := q.IndexOf(testCtx, "z")
	if err != nil || missing != -1 {
		t.Fatalf("IndexOf missing = %d, %v; want -1", missing, err)
	}

	head, err := q.PollN(testCtx, 2)
	if err != nil || len(head) != 2 || head[0] != "a" || head[1] != "b" {
		t.Fatalf("PollN = %v, %v; want [a b]", head, err)
	}
	empty, err := q.PollN(testCtx, 0)
	if err != nil || len(empty) != 0 {
		t.Fatalf("PollN(0) = %v, %v", empty, err)
	}

	moved, err := q.PollLastAndOfferFirstTo(testCtx, dst)
	if err != nil || moved != "d" {
		t.Fatalf("PollLastAndOfferFirstTo = %v, %v; want d", moved, err)
	}
	got, err := other.Peek(testCtx)
	if err != nil || got != "d" {
		t.Fatalf("dest Peek = %v, %v; want d", got, err)
	}
	left, err := q.ReadAll(testCtx)
	if err != nil || len(left) != 1 || left[0] != "c" {
		t.Fatalf("source after move = %v, %v; want [c]", left, err)
	}
}

func TestRQueue_EmptyPeek(t *testing.T) {
	client := newTestClient(t)
	q := client.GetRQueue(uniqueKey(t, "empty"))
	defer q.Clear(testCtx) //nolint:errcheck

	val, err := q.Peek(testCtx)
	if err != nil {
		t.Fatal("Peek on empty:", err)
	}
	if val != nil {
		t.Errorf("Peek on empty = %v, want nil", val)
	}

	sz, _ := q.Size(testCtx)
	if sz != 0 {
		t.Errorf("Size of empty queue = %d, want 0", sz)
	}
}
