package redi_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

func TestRLock_LockUnlock(t *testing.T) {
	client := newTestClient(t)
	lockName := uniqueKey(t, "lock")
	l := client.GetRLock(lockName)

	const clientID = "test-client-1"
	if err := l.Lock(testCtx, clientID, 5*time.Second); err != nil {
		t.Fatal("Lock:", err)
	}

	// Second lock attempt with a different client should fail (TryLock).
	l2 := client.GetRLock(lockName)
	acquired, err := l2.TryLock(testCtx, "other-client", 5*time.Second)
	if err != nil {
		t.Fatal("TryLock:", err)
	}
	if acquired {
		t.Error("TryLock by different client while lock held: expected false")
	}

	if err := l.Unlock(testCtx, clientID); err != nil {
		t.Fatal("Unlock:", err)
	}

	// After unlock the other client should be able to acquire.
	acquired, err = l2.TryLock(testCtx, "other-client", 5*time.Second)
	if err != nil {
		t.Fatal("TryLock after unlock:", err)
	}
	if !acquired {
		t.Error("TryLock after unlock: expected true")
	}
	_ = l2.Unlock(testCtx, "other-client")
}

func TestRLock_Reentrant(t *testing.T) {
	client := newTestClient(t)
	l := client.GetRLock(uniqueKey(t, "reentrant"))
	const cid = "reentrant-client"

	if err := l.Lock(testCtx, cid, 5*time.Second); err != nil {
		t.Fatal("first Lock:", err)
	}
	if err := l.Lock(testCtx, cid, 5*time.Second); err != nil {
		t.Fatal("reentrant Lock:", err)
	}
	n, err := l.HoldCount(testCtx, cid)
	if err != nil || n != 2 {
		t.Fatalf("HoldCount = %d, %v; want 2", n, err)
	}
	if err := l.Unlock(testCtx, cid); err != nil {
		t.Fatal("first Unlock:", err)
	}
	if err := l.Unlock(testCtx, cid); err != nil {
		t.Fatal("second Unlock:", err)
	}
}

func TestClient_HolderID(t *testing.T) {
	client := newTestClient(t)
	h := client.HolderID("7")
	if h != client.ID()+":7" {
		t.Fatalf("HolderID = %q, want %s:7", h, client.ID())
	}
	if client.HolderID("") != client.ID()+":0" {
		t.Fatalf("empty threadID = %q", client.HolderID(""))
	}

	l := client.GetLock(uniqueKey(t, "holder"))
	if err := l.Lock(testCtx, h, time.Minute); err != nil {
		t.Fatal(err)
	}
	defer l.Unlock(testCtx, h) //nolint:errcheck
	ok, err := l.IsHeldBy(testCtx, h)
	if err != nil || !ok {
		t.Fatalf("IsHeldBy = %v, %v", ok, err)
	}
	ttl, err := l.RemainTimeToLive(testCtx)
	if err != nil || ttl < time.Second {
		t.Fatalf("RemainTimeToLive = %v, %v", ttl, err)
	}
}

// TestRLock_Watchdog verifies P0 fix C1: with a short watchdog lease the
// lock is renewed indefinitely, so nobody else can acquire it.
func TestRLock_Watchdog(t *testing.T) {
	cfg := redi.DefaultConfig()
	cfg.LockWatchdogTimeout = 1500 * time.Millisecond
	client, err := redi.NewClient(cfg)
	if err != nil {
		t.Skip("Redis not available:", err)
	}
	defer client.Close() //nolint:errcheck // test cleanup

	l := client.GetRLock(uniqueKey(t, "watchdog"))
	if err := l.Lock(testCtx, "holder", 0); err != nil { // 0 → watchdog
		t.Fatal("Lock:", err)
	}
	defer l.Unlock(testCtx, "holder") //nolint:errcheck

	// 3 renewal intervals pass; without the watchdog the 1.5s lease expires.
	time.Sleep(4 * time.Second)

	got, err := l.TryLock(testCtx, "intruder", time.Second)
	if err != nil {
		t.Fatal("TryLock:", err)
	}
	if got {
		t.Fatal("watchdog failed: intruder acquired a lock that should be renewed")
	}
}

// TestRLock_WrongOwnerUnlockKeepsRenewal verifies P0 fix C3: an Unlock by a
// non-owner returns ErrLockNotHeld and must not kill the holder's watchdog.
func TestRLock_WrongOwnerUnlockKeepsRenewal(t *testing.T) {
	cfg := redi.DefaultConfig()
	cfg.LockWatchdogTimeout = 1500 * time.Millisecond
	client, err := redi.NewClient(cfg)
	if err != nil {
		t.Skip("Redis not available:", err)
	}
	defer client.Close() //nolint:errcheck // test cleanup

	l := client.GetRLock(uniqueKey(t, "c3"))
	if err := l.Lock(testCtx, "holder", 0); err != nil {
		t.Fatal("Lock:", err)
	}
	defer l.Unlock(testCtx, "holder") //nolint:errcheck

	if err := l.Unlock(testCtx, "not-the-owner"); err == nil {
		t.Fatal("expected error from non-owner Unlock")
	}

	time.Sleep(3 * time.Second)
	held, err := l.IsHeldBy(testCtx, "holder")
	if err != nil {
		t.Fatal("IsHeldBy:", err)
	}
	if !held {
		t.Fatal("holder lost the lock after a foreign Unlock")
	}
}

// TestRLock_PubSubWakeup verifies waiting Lock returns quickly after unlock
// (pub/sub wake-up instead of 1s fallback).
func TestRLock_PubSubWakeup(t *testing.T) {
	client := newTestClient(t)
	lockName := uniqueKey(t, "wake")
	l := client.GetRLock(lockName)
	if err := l.Lock(testCtx, "first", time.Minute); err != nil {
		t.Fatal("Lock:", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- l.Lock(testCtx, "second", time.Minute)
	}()

	// Wait until the waiter is subscribed, then unlock.
	time.Sleep(100 * time.Millisecond)
	if err := l.Unlock(testCtx, "first"); err != nil {
		t.Fatal("Unlock:", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal("second Lock:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Lock did not wake up within 2s of unlock")
	}
	_ = l.Unlock(testCtx, "second")
}

func TestRLock_Concurrent(t *testing.T) {
	client := newTestClient(t)
	lockName := uniqueKey(t, "concurrent")
	var counter atomic.Int64
	const goroutines = 5

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cid := "worker-" + string(rune('0'+id))
			l := client.GetRLock(lockName)
			lCtx, cancel := context.WithTimeout(testCtx, 10*time.Second)
			defer cancel()
			if err := l.Lock(lCtx, cid, 5*time.Second); err != nil {
				t.Errorf("worker %d Lock: %v", id, err)
				return
			}
			counter.Add(1)
			_ = l.Unlock(testCtx, cid)
		}(i)
	}
	wg.Wait()

	if counter.Load() != goroutines {
		t.Errorf("counter = %d, want %d", counter.Load(), goroutines)
	}
}
