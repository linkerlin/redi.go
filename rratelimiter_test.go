package redi_test

import (
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

func TestRRateLimiter(t *testing.T) {
	client := newTestClient(t)
	r := client.GetRateLimiter(uniqueKey(t, "rate"))
	defer r.Delete(testCtx) //nolint:errcheck

	if _, err := r.TrySetRate(testCtx, redi.RateTypeOverall, 2, time.Second); err != nil {
		t.Fatal("TrySetRate:", err)
	}

	results := make([]bool, 3)
	for i := range results {
		ok, err := r.TryAcquire(testCtx, 1)
		if err != nil {
			t.Fatal("TryAcquire:", err)
		}
		results[i] = ok
	}
	if !results[0] || !results[1] || results[2] {
		t.Fatalf("acquires = %v, want [true true false]", results)
	}

	// After the window passes, permits return.
	time.Sleep(1100 * time.Millisecond)
	ok, err := r.TryAcquire(testCtx, 1)
	if err != nil {
		t.Fatal("TryAcquire after window:", err)
	}
	if !ok {
		t.Fatal("acquire should succeed after the window elapsed")
	}
}

func TestRRateLimiter_ConfigPersisted(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "ratecfg")
	r1 := client.GetRateLimiter(name)
	defer r1.Delete(testCtx) //nolint:errcheck

	if _, err := r1.TrySetRate(testCtx, redi.RateTypeOverall, 1, time.Second); err != nil {
		t.Fatal(err)
	}

	// A second instance (e.g. another process) must see the same config —
	// this is redi.py's C3 lesson: config that lives only in memory fails
	// cross-process limiting.
	r2 := client.GetRateLimiter(name)
	ok, err := r2.TryAcquire(testCtx, 1)
	if err != nil {
		t.Fatal("TryAcquire:", err)
	}
	if !ok {
		t.Fatal("fresh instance should acquire the first permit")
	}
	ok, _ = r2.TryAcquire(testCtx, 1)
	if ok {
		t.Fatal("fresh instance must respect the persisted rate=1 config")
	}
}

func TestRDelayedQueue(t *testing.T) {
	client := newTestClient(t)
	q := client.GetDelayedQueue(uniqueKey(t, "delayed"))
	defer q.Delete(testCtx) //nolint:errcheck

	if err := q.Offer(testCtx, "later", 300*time.Millisecond); err != nil {
		t.Fatal("Offer:", err)
	}

	v, err := q.Peek(testCtx)
	if err != nil {
		t.Fatal("Peek:", err)
	}
	if v != nil {
		t.Fatalf("element visible before delay: %v", v)
	}

	if !eventual(t, 3*time.Second, func() bool {
		v, err := q.Poll(testCtx)
		return err == nil && v == "later"
	}) {
		t.Fatal("delayed element did not become ready within 3s")
	}
}
