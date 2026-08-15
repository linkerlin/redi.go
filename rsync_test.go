package redi_test

import (
	"context"
	"testing"
	"time"
)

func TestRSemaphore(t *testing.T) {
	client := newTestClient(t)
	s := client.GetSemaphore(uniqueKey(t, "sem"))
	defer s.Delete(testCtx) //nolint:errcheck

	ok, err := s.TrySetPermits(testCtx, 2)
	if err != nil || !ok {
		t.Fatalf("TrySetPermits = %v, %v", ok, err)
	}

	ok, _ = s.TryAcquire(testCtx, 1)
	if !ok {
		t.Fatal("first TryAcquire failed")
	}
	ok, _ = s.TryAcquire(testCtx, 1)
	if !ok {
		t.Fatal("second TryAcquire failed")
	}
	ok, _ = s.TryAcquire(testCtx, 1)
	if ok {
		t.Fatal("third TryAcquire should fail (0 permits left)")
	}

	if err := s.Release(testCtx, 1); err != nil {
		t.Fatal("Release:", err)
	}
	ok, _ = s.TryAcquire(testCtx, 1)
	if !ok {
		t.Fatal("TryAcquire after Release failed")
	}
}

func TestRSemaphore_BlockingAcquireWakes(t *testing.T) {
	client := newTestClient(t)
	s := client.GetSemaphore(uniqueKey(t, "semwake"))
	defer s.Delete(testCtx) //nolint:errcheck

	if _, err := s.TrySetPermits(testCtx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TryAcquire(testCtx, 1); err != nil {
		t.Fatal(err)
	}

	acquired := make(chan error, 1)
	go func() {
		acquired <- s.Acquire(context.Background(), 1)
	}()

	time.Sleep(150 * time.Millisecond)
	if err := s.Release(testCtx, 1); err != nil {
		t.Fatal("Release:", err)
	}

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal("Acquire:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire did not wake within 2s of Release")
	}
}

func TestRCountDownLatch(t *testing.T) {
	client := newTestClient(t)
	l := client.GetCountDownLatch(uniqueKey(t, "cdl"))
	defer l.Delete(testCtx) //nolint:errcheck

	ok, err := l.TrySetCount(testCtx, 2)
	if err != nil || !ok {
		t.Fatalf("TrySetCount = %v, %v", ok, err)
	}

	done := make(chan bool, 1)
	go func() {
		open, _ := l.Await(context.Background(), 0)
		done <- open
	}()

	if err := l.CountDown(testCtx); err != nil {
		t.Fatal("CountDown 1:", err)
	}
	n, _ := l.GetCount(testCtx)
	if n != 1 {
		t.Fatalf("GetCount = %d, want 1", n)
	}

	select {
	case <-done:
		t.Fatal("Await returned before count reached zero")
	case <-time.After(150 * time.Millisecond):
	}

	if err := l.CountDown(testCtx); err != nil {
		t.Fatal("CountDown 2:", err)
	}

	select {
	case open := <-done:
		if !open {
			t.Fatal("Await returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Await did not return within 2s of zero count")
	}
}
