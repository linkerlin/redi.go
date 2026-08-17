package redi_test

import (
	"errors"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

func TestRNonReentrantFairLock_RejectsReentryAndHandsOff(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "non-reentrant-fair")
	queueKey := "redisson_lock_queue:{" + name + "}"
	timeoutKey := "redisson_lock_timeout:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, queueKey, timeoutKey) })

	lock := client.GetNonReentrantFairLock(name)
	ok, err := lock.TryLock(testCtx, "owner", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first TryLock = %v, %v; want true", ok, err)
	}
	ok, err = lock.TryLock(testCtx, "owner", time.Minute)
	if ok || !errors.Is(err, redi.ErrLockReentrant) {
		t.Fatalf("reentrant TryLock = %v, %v; want ErrLockReentrant", ok, err)
	}

	acquired := make(chan error, 1)
	go func() {
		err := lock.Lock(testCtx, "next", time.Minute)
		acquired <- err
		if err == nil {
			_ = lock.Unlock(testCtx, "next")
		}
	}()
	rc := rawClient(t)
	if !eventual(t, 2*time.Second, func() bool {
		head, _ := rc.LIndex(testCtx, queueKey, 0).Result()
		return head == "next"
	}) {
		t.Fatal("next holder did not join the fair queue")
	}
	if err := lock.Unlock(testCtx, "owner"); err != nil {
		t.Fatal("owner Unlock:", err)
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal("next Lock:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("next holder did not acquire after unlock")
	}
}
