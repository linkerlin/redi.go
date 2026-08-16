package redi_test

import (
	"testing"
	"time"
)

func TestRRingBuffer(t *testing.T) {
	client := newTestClient(t)
	r := client.GetRingBuffer(uniqueKey(t, "rb"))
	defer r.Delete(testCtx) //nolint:errcheck

	if _, err := r.TrySetCapacity(testCtx, 3); err != nil {
		t.Fatal(err)
	}
	// Re-init is a no-op.
	if ok, _ := r.TrySetCapacity(testCtx, 10); ok {
		t.Fatal("re-init capacity should return false")
	}

	for i := 0; i < 5; i++ {
		if err := r.Add(testCtx, i); err != nil {
			t.Fatal(err)
		}
	}
	sz, _ := r.Size(testCtx)
	if sz != 3 {
		t.Fatalf("Size after overflow = %d, want 3 (evicted)", sz)
	}
	newest, _ := r.ReadNewest(testCtx, 3)
	if len(newest) != 3 || !numEq(newest[0], 2) || !numEq(newest[2], 4) {
		t.Fatalf("newest = %v, want [2 3 4]", newest)
	}
	oldest, _ := r.ReadOldest(testCtx, 2)
	if len(oldest) != 2 || !numEq(oldest[0], 2) {
		t.Fatalf("oldest = %v", oldest)
	}
	rem, _ := r.RemainingCapacity(testCtx)
	if rem != 0 {
		t.Fatalf("remaining = %d, want 0", rem)
	}

	// AddAll trims in one pass.
	r2 := client.GetRingBuffer(uniqueKey(t, "rb2"))
	defer r2.Delete(testCtx) //nolint:errcheck
	if _, err := r2.TrySetCapacity(testCtx, 2); err != nil {
		t.Fatal(err)
	}
	if err := r2.AddAll(testCtx, "a", "b", "c", "d"); err != nil {
		t.Fatal(err)
	}
	sz, _ = r2.Size(testCtx)
	if sz != 2 {
		t.Fatalf("r2 Size = %d, want 2", sz)
	}
	vals, _ := r2.ReadNewest(testCtx, 2)
	if vals[0] != "c" || vals[1] != "d" {
		t.Fatalf("r2 = %v, want [c d]", vals)
	}

	// SetCapacity shrinks live buffers.
	if err := r2.SetCapacity(testCtx, 1); err != nil {
		t.Fatal(err)
	}
	sz, _ = r2.Size(testCtx)
	if sz != 1 {
		t.Fatalf("after SetCapacity(1) size = %d", sz)
	}
}

func TestRShardedTopic(t *testing.T) {
	client := newTestClient(t)
	topic := client.GetShardedTopic(uniqueKey(t, "stopic"))

	got := make(chan any, 2)
	id, err := topic.Subscribe(func(msg any) { got <- msg })
	if err != nil {
		t.Fatal("Subscribe:", err)
	}

	n, err := topic.Publish(testCtx, "sharded-hello")
	if err != nil {
		t.Fatal("Publish:", err)
	}
	if n < 1 {
		t.Fatalf("SSPUBLISH receivers = %d, want >= 1", n)
	}
	select {
	case v := <-got:
		if v != "sharded-hello" {
			t.Fatalf("got %v", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sharded message not received")
	}

	if n, _ := topic.CountSubscribers(testCtx); n < 1 {
		t.Fatal("SHARDNUMSUB = 0")
	}
	topic.Unsubscribe(id)
}

func TestRFunction(t *testing.T) {
	client := newTestClient(t)
	f := client.GetFunction()

	const lib = `#!lua name=redigo_test
redis.register_function{function_name='redigo_double', callback=function(keys, args)
    return tonumber(args[1]) * 2
end, flags={'no-writes'}}`

	if err := f.Load(testCtx, lib); err != nil {
		t.Skipf("FUNCTION LOAD unavailable (%v) - needs Redis 7+", err)
	}
	defer f.Delete(testCtx, "redigo_test") //nolint:errcheck

	v, err := f.Call(testCtx, "redigo_double", nil, 21)
	if err != nil {
		t.Fatal("Call:", err)
	}
	if !numEq(v, 42) {
		t.Fatalf("FCall = %#v, want 42", v)
	}

	libs, err := f.List(testCtx, false)
	if err != nil || len(libs) != 1 || libs[0].Name != "redigo_test" {
		t.Fatalf("List = %+v, %v", libs, err)
	}

	v, err = f.CallReadOnly(testCtx, "redigo_double", nil, 5)
	if err != nil || !numEq(v, 10) {
		t.Fatalf("CallReadOnly = %#v, %v", v, err)
	}
}
