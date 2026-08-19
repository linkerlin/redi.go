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

func TestRSemaphore_TryAcquireWait(t *testing.T) {
	client := newTestClient(t)
	s := client.GetSemaphore(uniqueKey(t, "semwait"))
	defer s.Delete(testCtx) //nolint:errcheck

	if _, err := s.TrySetPermits(testCtx, 0); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.TryAcquireWait(testCtx, 1, 100*time.Millisecond); err != nil || ok {
		t.Fatalf("TryAcquireWait timeout = %v, %v", ok, err)
	}

	acquired := make(chan bool, 1)
	go func() {
		ok, _ := s.TryAcquireWait(context.Background(), 1, 2*time.Second)
		acquired <- ok
	}()
	if err := s.Release(testCtx, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case ok := <-acquired:
		if !ok {
			t.Fatal("TryAcquireWait returned false after Release")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TryAcquireWait did not return after Release")
	}
}

func TestRSemaphore_TrySetPermitsPublishes(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "sem-setpub")
	s := client.GetSemaphore(name)
	defer s.Delete(testCtx) //nolint:errcheck

	channel := "redisson_sc:{" + name + "}"
	sub := client.Redis().Subscribe(testCtx, channel)
	defer sub.Close() //nolint:errcheck
	if _, err := sub.Receive(testCtx); err != nil {
		t.Fatal("subscribe ack:", err)
	}
	msgs := sub.Channel()

	ok, err := s.TrySetPermits(testCtx, 3)
	if err != nil || !ok {
		t.Fatalf("TrySetPermits = %v, %v", ok, err)
	}
	select {
	case msg := <-msgs:
		if msg.Payload != "3" {
			t.Fatalf("publish payload = %q, want 3", msg.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("TrySetPermits did not PUBLISH to redisson_sc")
	}

	ok, err = s.TrySetPermits(testCtx, 9)
	if err != nil || ok {
		t.Fatalf("second TrySetPermits = %v, %v; want false", ok, err)
	}
	select {
	case msg := <-msgs:
		t.Fatalf("second TrySetPermits published %q", msg.Payload)
	case <-time.After(150 * time.Millisecond):
	}

	nameTTL := uniqueKey(t, "sem-setttl")
	st := client.GetSemaphore(nameTTL)
	defer st.Delete(testCtx) //nolint:errcheck
	ok, err = st.TrySetPermitsWithTTL(testCtx, 2, time.Minute)
	if err != nil || !ok {
		t.Fatalf("TrySetPermitsWithTTL = %v, %v", ok, err)
	}
	ttl, err := st.RemainTTL(testCtx)
	if err != nil || ttl <= 0 {
		t.Fatalf("TTL after TrySetPermitsWithTTL = %v, %v", ttl, err)
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
