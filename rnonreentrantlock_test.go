package redi_test

import (
	"testing"
	"time"
)

func TestRNonReentrantLock_RejectsReentry(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "non-reentrant-lock")
	lock := client.GetNonReentrantLock(name)
	t.Cleanup(func() { interopCleanup(t, name) })

	acquired, err := lock.TryLock(testCtx, "same-holder", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("first TryLock = %v, %v", acquired, err)
	}
	acquired, err = lock.TryLock(testCtx, "same-holder", time.Minute)
	if err != nil || acquired {
		t.Fatalf("reentrant TryLock = %v, %v; want false", acquired, err)
	}

	if err := lock.Unlock(testCtx, "same-holder"); err != nil {
		t.Fatal("Unlock:", err)
	}
	acquired, err = lock.TryLock(testCtx, "different-holder", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("TryLock after unlock = %v, %v", acquired, err)
	}
	if err := lock.Unlock(testCtx, "different-holder"); err != nil {
		t.Fatal("final Unlock:", err)
	}
}
