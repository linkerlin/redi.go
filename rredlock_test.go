package redi_test

import (
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

func TestRRedLock_MajorityAndRollback(t *testing.T) {
	client := newTestClient(t)
	names := []string{
		uniqueKey(t, "redlock-1"),
		uniqueKey(t, "redlock-2"),
		uniqueKey(t, "redlock-3"),
	}
	t.Cleanup(func() { interopCleanup(t, names...) })
	locks := []*redi.RLock{
		client.GetLock(names[0]),
		client.GetLock(names[1]),
		client.GetLock(names[2]),
	}
	redLock := client.NewRedLock(locks...)

	if err := locks[2].Lock(testCtx, "other", time.Minute); err != nil {
		t.Fatal("block third lock:", err)
	}
	ok, err := redLock.TryLock(testCtx, "majority", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock with 2/3 available = %v, %v; want true", ok, err)
	}
	for i := 0; i < 2; i++ {
		held, _ := locks[i].IsHeldBy(testCtx, "majority")
		if !held {
			t.Fatalf("majority member %d not held", i)
		}
	}
	if err := redLock.Unlock(testCtx, "majority"); err != nil {
		t.Fatal("Unlock majority:", err)
	}
	otherHeld, _ := locks[2].IsHeldBy(testCtx, "other")
	if !otherHeld {
		t.Fatal("RedLock Unlock released another holder's member")
	}

	if err := locks[1].Lock(testCtx, "other", time.Minute); err != nil {
		t.Fatal("block second lock:", err)
	}
	ok, err = redLock.TryLock(testCtx, "minority", time.Minute)
	if err != nil || ok {
		t.Fatalf("TryLock with only 1/3 available = %v, %v; want false", ok, err)
	}
	held, _ := locks[0].IsHeldBy(testCtx, "minority")
	if held {
		t.Fatal("minority acquisition did not roll back")
	}
	_ = locks[1].Unlock(testCtx, "other")
	_ = locks[2].Unlock(testCtx, "other")
}
