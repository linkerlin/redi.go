package redi_test

import (
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

// TestRRateLimiter_ExpiredPermitsReturnToPool locks the purge arithmetic:
// the byte-math reader ported from redi.py read the permits tail
// big-endian while writers packed little-endian, releasing 16777216 per
// expired permit instead of 1 (value ballooned to 16M+ in the wild).
func TestRRateLimiter_ExpiredPermitsReturnToPool(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "rate-expiry")
	r := client.GetRateLimiter(name)
	defer r.Delete(testCtx) //nolint:errcheck

	if _, err := r.TrySetRate(testCtx, redi.RateTypeOverall, 3, 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Consume the whole window.
	for i := 0; i < 3; i++ {
		if ok, err := r.TryAcquire(testCtx, 1); err != nil || !ok {
			t.Fatalf("acquire %d = %v, %v", i, ok, err)
		}
	}
	if avail, _ := r.AvailablePermits(testCtx); avail != 0 {
		t.Fatalf("available after drain = %d", avail)
	}

	// Wait past the window: the expired permits must return EXACTLY rate.
	if !eventual(t, 3*time.Second, func() bool {
		avail, _ := r.AvailablePermits(testCtx)
		return avail == 3
	}) {
		avail, _ := r.AvailablePermits(testCtx)
		t.Fatalf("available after expiry = %d, want exactly 3 (purge math broken)", avail)
	}

	// And a fresh window cycle works.
	if ok, _ := r.TryAcquire(testCtx, 3); !ok {
		t.Fatal("full-window acquire after expiry failed")
	}
}

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
