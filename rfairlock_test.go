package redi_test

import (
	"context"
	"testing"
	"time"
)

func TestRFairLock_FIFO(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "fair-fifo")
	queueKey := "redisson_lock_queue:{" + name + "}"
	timeoutKey := "redisson_lock_timeout:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, queueKey, timeoutKey) })

	lock := client.GetFairLock(name)
	if err := lock.Lock(testCtx, "owner", time.Minute); err != nil {
		t.Fatal("owner Lock:", err)
	}

	ctx, cancel := context.WithTimeout(testCtx, 5*time.Second)
	defer cancel()
	firstAcquired := make(chan error, 1)
	releaseFirst := make(chan struct{})
	go func() {
		err := lock.Lock(ctx, "first", time.Minute)
		firstAcquired <- err
		if err == nil {
			<-releaseFirst
			_ = lock.Unlock(testCtx, "first")
		}
	}()

	rc := rawClient(t)
	if !eventual(t, 2*time.Second, func() bool {
		v, _ := rc.LIndex(testCtx, queueKey, 0).Result()
		return v == "first"
	}) {
		t.Fatal("first waiter was not queued")
	}

	secondAcquired := make(chan error, 1)
	go func() {
		err := lock.Lock(ctx, "second", time.Minute)
		secondAcquired <- err
		if err == nil {
			_ = lock.Unlock(testCtx, "second")
		}
	}()
	if !eventual(t, 2*time.Second, func() bool {
		v, _ := rc.LRange(testCtx, queueKey, 0, -1).Result()
		return len(v) == 2 && v[0] == "first" && v[1] == "second"
	}) {
		t.Fatal("waiters were not queued in FIFO order")
	}

	if err := lock.Unlock(testCtx, "owner"); err != nil {
		t.Fatal("owner Unlock:", err)
	}
	select {
	case err := <-firstAcquired:
		if err != nil {
			t.Fatal("first waiter Lock:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first waiter did not acquire")
	}
	select {
	case err := <-secondAcquired:
		t.Fatalf("second waiter acquired before first released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirst)
	select {
	case err := <-secondAcquired:
		if err != nil {
			t.Fatal("second waiter Lock:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second waiter did not acquire after first")
	}
}

func TestWire_FairLockLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-fair")
	queueKey := "redisson_lock_queue:{" + name + "}"
	timeoutKey := "redisson_lock_timeout:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, queueKey, timeoutKey) })

	lock := client.GetFairLock(name)
	if err := lock.Lock(testCtx, "owner", time.Minute); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(testCtx)
	done := make(chan error, 1)
	go func() { done <- lock.Lock(ctx, "waiter", time.Minute) }()

	rc := rawClient(t)
	if !eventual(t, 2*time.Second, func() bool {
		queueType, _ := rc.Type(testCtx, queueKey).Result()
		timeoutType, _ := rc.Type(testCtx, timeoutKey).Result()
		return queueType == "list" && timeoutType == "zset"
	}) {
		t.Fatalf("companions must be LIST %q and ZSET %q", queueKey, timeoutKey)
	}
	members, err := rc.LRange(testCtx, queueKey, 0, -1).Result()
	if err != nil || len(members) != 1 || members[0] != "waiter" {
		t.Fatalf("queue = %v, %v; want [waiter]", members, err)
	}
	if _, err := rc.ZScore(testCtx, timeoutKey, "waiter").Result(); err != nil {
		t.Fatalf("timeout member missing: %v", err)
	}

	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter did not return")
	}
	_ = lock.Unlock(testCtx, "owner")
}
