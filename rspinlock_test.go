package redi_test

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRSpinLock_ContentionAndCancellation(t *testing.T) {
	client := newTestClient(t)
	lock := client.GetSpinLock(uniqueKey(t, "spin-lock"))
	defer lock.ForceUnlock(testCtx) //nolint:errcheck

	acquired, err := lock.TryLock(testCtx, "first", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("first TryLock = %v, %v", acquired, err)
	}
	acquired, err = lock.TryLock(testCtx, "second", time.Minute)
	if err != nil || acquired {
		t.Fatalf("contended TryLock = %v, %v; want false", acquired, err)
	}

	ctx, cancel := context.WithTimeout(testCtx, 40*time.Millisecond)
	defer cancel()
	if err := lock.Lock(ctx, "blocked", time.Minute); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lock cancellation error = %v, want context deadline", err)
	}

	if err := lock.Unlock(testCtx, "first"); err != nil {
		t.Fatal("Unlock first:", err)
	}
	acquired, err = lock.TryLock(testCtx, "second", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("TryLock after unlock = %v, %v", acquired, err)
	}
	if err := lock.Unlock(testCtx, "second"); err != nil {
		t.Fatal("Unlock second:", err)
	}
}
