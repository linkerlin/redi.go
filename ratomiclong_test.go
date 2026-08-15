package redi_test

import (
	"context"
	"testing"

	redi "github.com/linkerlin/redi.go"
)

func TestRAtomicLong(t *testing.T) {
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
	a := client.GetRAtomicLong(uniqueKey(t, "atomic"))
	// clean up
	_ = a.Set(ctx, 0)

	// IncrementAndGet
	val, err := a.IncrementAndGet(ctx)
	if err != nil {
		t.Fatal("IncrementAndGet:", err)
	}
	if val != 1 {
		t.Errorf("IncrementAndGet = %d, want 1", val)
	}

	// AddAndGet
	val, err = a.AddAndGet(ctx, 9)
	if err != nil {
		t.Fatal("AddAndGet:", err)
	}
	if val != 10 {
		t.Errorf("AddAndGet(9) = %d, want 10", val)
	}

	// DecrementAndGet
	val, err = a.DecrementAndGet(ctx)
	if err != nil {
		t.Fatal("DecrementAndGet:", err)
	}
	if val != 9 {
		t.Errorf("DecrementAndGet = %d, want 9", val)
	}

	// CompareAndSet – should succeed
	swapped, err := a.CompareAndSet(ctx, 9, 42)
	if err != nil {
		t.Fatal("CompareAndSet:", err)
	}
	if !swapped {
		t.Error("CompareAndSet(9, 42): expected swap to succeed")
	}

	// CompareAndSet – should fail
	swapped, err = a.CompareAndSet(ctx, 9, 99)
	if err != nil {
		t.Fatal("CompareAndSet:", err)
	}
	if swapped {
		t.Error("CompareAndSet(9, 99): expected swap to fail (current is 42)")
	}

	// Get
	val, err = a.Get(ctx)
	if err != nil {
		t.Fatal("Get:", err)
	}
	if val != 42 {
		t.Errorf("Get = %d, want 42", val)
	}
}
