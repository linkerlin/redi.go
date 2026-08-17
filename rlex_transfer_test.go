package redi_test

import (
	"testing"
	"time"
)

func TestRLexSortedSet(t *testing.T) {
	client := newTestClient(t)
	s := client.GetLexSortedSet(uniqueKey(t, "lex"))
	defer s.Clear(testCtx) //nolint:errcheck

	for _, e := range []string{"banana", "apple", "cherry"} {
		if _, err := s.Add(testCtx, e); err != nil {
			t.Fatal(err)
		}
	}

	first, _ := s.First(testCtx)
	last, _ := s.Last(testCtx)
	if first != "apple" || last != "cherry" {
		t.Fatalf("first/last = %q/%q", first, last)
	}

	ranged, _ := s.RangeByLex(testCtx, "apple", "banana", 0, -1)
	if len(ranged) != 2 || ranged[0] != "apple" || ranged[1] != "banana" {
		t.Fatalf("RangeByLex = %v", ranged)
	}

	head, _ := s.RangeHead(testCtx, "cherry", 0, -1)
	if len(head) != 2 {
		t.Fatalf("RangeHead = %v", head)
	}
	tail, _ := s.RangeTail(testCtx, "banana", 0, -1)
	if len(tail) != 1 || tail[0] != "cherry" {
		t.Fatalf("RangeTail = %v", tail)
	}

	n, _ := s.CountRange(testCtx, "apple", "banana")
	if n != 2 {
		t.Fatalf("CountRange = %d", n)
	}
	n, _ = s.CountTail(testCtx, "banana")
	if n != 1 {
		t.Fatalf("CountTail = %d", n)
	}

	// RangeByLex with offset/count.
	ranged, _ = s.RangeByLex(testCtx, "apple", "cherry", 1, 1)
	if len(ranged) != 1 || ranged[0] != "banana" {
		t.Fatalf("RangeByLex offset/count = %v", ranged)
	}

	rank, _ := s.Rank(testCtx, "banana")
	revRank, _ := s.RevRank(testCtx, "banana")
	if rank != 1 || revRank != 1 {
		t.Fatalf("rank/revRank = %d/%d; want 1/1", rank, revRank)
	}
	if missing, _ := s.Rank(testCtx, "missing"); missing != -1 {
		t.Fatalf("Rank(missing) = %d; want -1", missing)
	}
	reversed, _ := s.RangeReversed(testCtx, 0, -1)
	if len(reversed) != 3 || reversed[0] != "cherry" || reversed[2] != "apple" {
		t.Fatalf("RangeReversed = %v", reversed)
	}
	reversed, _ = s.RangeByLexReversed(testCtx, "apple", "cherry", 1, 1)
	if len(reversed) != 1 || reversed[0] != "banana" {
		t.Fatalf("RangeByLexReversed offset/count = %v", reversed)
	}
	headReversed, _ := s.RangeHeadReversed(testCtx, "cherry", 0, -1)
	if len(headReversed) != 2 || headReversed[0] != "banana" {
		t.Fatalf("RangeHeadReversed = %v", headReversed)
	}
	tailReversed, _ := s.RangeTailReversed(testCtx, "apple", 0, -1)
	if len(tailReversed) != 2 || tailReversed[0] != "cherry" {
		t.Fatalf("RangeTailReversed = %v", tailReversed)
	}
	if random, err := s.Random(testCtx); err != nil || random == "" {
		t.Fatalf("Random = %q, %v", random, err)
	}
	if random, err := s.RandomN(testCtx, 2); err != nil || len(random) != 2 {
		t.Fatalf("RandomN = %v, %v", random, err)
	}

	// Removal.
	n, _ = s.RemoveRangeHead(testCtx, "cherry")
	if n != 2 {
		t.Fatalf("RemoveRangeHead = %d", n)
	}
	sz, _ := s.Size(testCtx)
	if sz != 1 {
		t.Fatalf("Size after removal = %d", sz)
	}

	// Poll.
	polled, _ := s.PollFirst(testCtx)
	if polled != "cherry" {
		t.Fatalf("PollFirst = %q", polled)
	}
	polled, _ = s.PollLast(testCtx)
	if polled != "" {
		t.Fatalf("PollLast on empty = %q", polled)
	}
}

func TestRTransferQueue(t *testing.T) {
	client := newTestClient(t)
	src := client.GetTransferQueue(uniqueKey(t, "tq-src"))
	dst := client.GetQueue(uniqueKey(t, "tq-dst"))
	defer src.Clear(testCtx) //nolint:errcheck
	defer dst.Clear(testCtx) //nolint:errcheck

	// Empty transfer returns nil.
	v, err := src.Transfer(testCtx, dst.Name())
	if err != nil || v != nil {
		t.Fatalf("empty Transfer = %v, %v", v, err)
	}

	if err := src.Offer(testCtx, "job-1", "job-2"); err != nil {
		t.Fatal(err)
	}
	v, err = src.Transfer(testCtx, dst.Name())
	if err != nil || v != "job-1" {
		t.Fatalf("Transfer = %v, %v", v, err)
	}
	head, _ := dst.Peek(testCtx)
	if head != "job-1" {
		t.Fatalf("destination head = %v", head)
	}
	srcSz, _ := src.Size(testCtx)
	if srcSz != 1 {
		t.Fatalf("source size = %d", srcSz)
	}

	// Transfer the remaining element so the source drains.
	v, err = src.Transfer(testCtx, dst.Name())
	if err != nil || v != "job-2" {
		t.Fatalf("second Transfer = %v, %v", v, err)
	}

	// Blocking wait: TryTransfer stays empty until a concurrent offer.
	arrived := make(chan any, 1)
	go func() {
		v, _ := src.TryTransfer(testCtx, dst.Name(), 3*time.Second)
		arrived <- v
	}()
	time.Sleep(150 * time.Millisecond)
	if err := src.Offer(testCtx, "job-3"); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-arrived:
		if v != "job-3" {
			t.Fatalf("TryTransfer = %v", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TryTransfer did not pick up the offered element")
	}
}
