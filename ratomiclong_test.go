package redi_test

import (
	"testing"
)

func TestRAtomicLong(t *testing.T) {
	client := newTestClient(t)
	a := client.GetRAtomicLong(uniqueKey(t, "atomic"))
	defer a.Delete(testCtx) //nolint:errcheck

	if _, err := a.Set(testCtx, 0); err != nil {
		t.Fatal("Set:", err)
	}

	val, err := a.IncrementAndGet(testCtx)
	if err != nil || val != 1 {
		t.Fatalf("IncrementAndGet = %d, %v; want 1", val, err)
	}

	val, err = a.AddAndGet(testCtx, 9)
	if err != nil || val != 10 {
		t.Fatalf("AddAndGet(9) = %d, %v; want 10", val, err)
	}

	val, err = a.DecrementAndGet(testCtx)
	if err != nil || val != 9 {
		t.Fatalf("DecrementAndGet = %d, %v; want 9", val, err)
	}

	swapped, err := a.CompareAndSet(testCtx, 9, 42)
	if err != nil || !swapped {
		t.Fatalf("CompareAndSet(9, 42) = %v, %v; want true", swapped, err)
	}

	swapped, err = a.CompareAndSet(testCtx, 9, 99)
	if err != nil || swapped {
		t.Fatalf("CompareAndSet(9, 99) = %v, %v; want false", swapped, err)
	}

	val, err = a.Get(testCtx)
	if err != nil || val != 42 {
		t.Fatalf("Get = %d, %v; want 42", val, err)
	}
}

// TestRAtomicLong_MissingKey verifies P0-6/7: missing key reads as 0 and
// CAS(expect=0) succeeds on a fresh key.
func TestRAtomicLong_MissingKey(t *testing.T) {
	client := newTestClient(t)
	a := client.GetRAtomicLong(uniqueKey(t, "missing"))
	defer a.Delete(testCtx) //nolint:errcheck

	val, err := a.Get(testCtx)
	if err != nil || val != 0 {
		t.Fatalf("Get on missing = %d, %v; want 0", val, err)
	}

	swapped, err := a.CompareAndSet(testCtx, 0, 7)
	if err != nil || !swapped {
		t.Fatalf("CAS(0,7) on missing = %v, %v; want true", swapped, err)
	}
}
