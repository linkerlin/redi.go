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
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, err := redi.NewClient(cfg)
	if err != nil {
		t.Fatal("NewClient:", err)
	}
	defer client.Close()

	ctx := context.Background()
	lockName := uniqueKey(t, "lock")
	l := client.GetRLock(lockName)

	const clientID = "test-client-1"
	if err := l.Lock(ctx, clientID, 5*time.Second); err != nil {
		t.Fatal("Lock:", err)
	}

	// Second lock attempt with a different client should fail (TryLock).
	l2 := client.GetRLock(lockName)
	acquired, err := l2.TryLock(ctx, "other-client", 5*time.Second)
	if err != nil {
		t.Fatal("TryLock:", err)
	}
	if acquired {
		t.Error("TryLock by different client while lock held: expected false")
	}

	if err := l.Unlock(ctx, clientID); err != nil {
		t.Fatal("Unlock:", err)
	}

	// After unlock the other client should be able to acquire.
	acquired, err = l2.TryLock(ctx, "other-client", 5*time.Second)
	if err != nil {
		t.Fatal("TryLock after unlock:", err)
	}
	if !acquired {
		t.Error("TryLock after unlock: expected true")
	}
	_ = l2.Unlock(ctx, "other-client")
}

func TestRLock_Reentrant(t *testing.T) {
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, _ := redi.NewClient(cfg)
	defer client.Close()

	ctx := context.Background()
	l := client.GetRLock(uniqueKey(t, "reentrant"))
	const cid = "reentrant-client"

	if err := l.Lock(ctx, cid, 5*time.Second); err != nil {
		t.Fatal("first Lock:", err)
	}
	// Re-entrant lock by same client should succeed.
	if err := l.Lock(ctx, cid, 5*time.Second); err != nil {
		t.Fatal("reentrant Lock:", err)
	}
	// Need two Unlocks.
	if err := l.Unlock(ctx, cid); err != nil {
		t.Fatal("first Unlock:", err)
	}
	if err := l.Unlock(ctx, cid); err != nil {
		t.Fatal("second Unlock:", err)
	}
}

func TestRLock_Concurrent(t *testing.T) {
	if !redisAvailable(t) {
		return
	}
	cfg := redi.DefaultConfig()
	client, _ := redi.NewClient(cfg)
	defer client.Close()

	ctx := context.Background()
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
			lCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := l.Lock(lCtx, cid, 5*time.Second); err != nil {
				t.Errorf("worker %d Lock: %v", id, err)
				return
			}
			counter.Add(1)
			_ = l.Unlock(ctx, cid)
		}(i)
	}
	wg.Wait()

	if counter.Load() != goroutines {
		t.Errorf("counter = %d, want %d", counter.Load(), goroutines)
	}
}
