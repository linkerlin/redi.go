package redi_test

import (
	"context"
	"testing"
	"time"
)

func TestRBoundedBlockingQueue_FullOfferAndPollWake(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "bounded")
	capacityKey := "redisson_bqs:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, capacityKey) })

	q := client.GetBoundedBlockingQueue(name)
	set, err := q.TrySetCapacity(testCtx, 1)
	if err != nil || !set {
		t.Fatalf("TrySetCapacity = %v, %v; want true", set, err)
	}
	if ok, err := q.Offer(testCtx, "first"); err != nil || !ok {
		t.Fatalf("first Offer = %v, %v; want true", ok, err)
	}
	if ok, err := q.Offer(testCtx, "full"); err != nil || ok {
		t.Fatalf("full Offer = %v, %v; want false", ok, err)
	}

	result := make(chan error, 1)
	go func() {
		ok, err := q.OfferWait(testCtx, "second", 3*time.Second)
		if err == nil && !ok {
			err = context.DeadlineExceeded
		}
		result <- err
	}()
	if !eventual(t, time.Second, func() bool {
		select {
		case err := <-result:
			t.Fatalf("OfferWait returned before capacity was released: %v", err)
		default:
			return true
		}
		return false
	}) {
		t.Fatal("OfferWait did not block")
	}

	v, err := q.Poll(testCtx)
	if err != nil || v != "first" {
		t.Fatalf("Poll = %v, %v; want first", v, err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal("OfferWait:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Poll did not wake blocked OfferWait")
	}
	v, err = q.Poll(testCtx)
	if err != nil || v != "second" {
		t.Fatalf("second Poll = %v, %v; want second", v, err)
	}
}

func TestRBoundedBlockingQueue_TakeUnblocks(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "bounded-take")
	capacityKey := "redisson_bqs:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, capacityKey) })

	q := client.GetBoundedBlockingQueue(name)
	if ok, err := q.TrySetCapacity(testCtx, 1); err != nil || !ok {
		t.Fatalf("TrySetCapacity = %v, %v", ok, err)
	}
	got := make(chan any, 1)
	go func() {
		v, _ := q.Take(testCtx, 3*time.Second)
		got <- v
	}()
	if err := q.Put(testCtx, "job"); err != nil {
		t.Fatal("Put:", err)
	}
	select {
	case v := <-got:
		if v != "job" {
			t.Fatalf("Take = %v; want job", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Put did not unblock Take")
	}
	if remaining, err := q.RemainingCapacity(testCtx); err != nil || remaining != 1 {
		t.Fatalf("RemainingCapacity = %d, %v; want 1", remaining, err)
	}
}
