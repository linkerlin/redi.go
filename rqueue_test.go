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
