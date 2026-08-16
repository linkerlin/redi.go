package redi_test

import (
	"testing"
	"time"
)

func TestRLongAdder(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "adder")
	a1 := client.GetLongAdder(name)
	defer a1.Destroy()
	a2 := client.GetLongAdder(name) // second instance in another "process" role
	defer a2.Destroy()

	// Buffered writes: no network, both accumulate locally.
	a1.Add(5)
	a1.Increment()
	a2.Add(3)
	a2.Decrement()

	// Sum coordinates a flush across every live instance (both Go adders).
	sum, err := a1.Sum(testCtx)
	if err != nil {
		t.Fatal("Sum:", err)
	}
	if sum != 8 { // 5+1+3-1
		t.Fatalf("Sum = %d, want 8", sum)
	}

	// Sum is non-destructive: buffers keep accumulating.
	a1.Add(2)
	sum, err = a1.Sum(testCtx)
	if err != nil || sum != 10 {
		t.Fatalf("second Sum = %d, %v; want 10 (non-destructive)", sum, err)
	}

	// Reset clears every live buffer.
	if err := a1.Reset(testCtx); err != nil {
		t.Fatal("Reset:", err)
	}
	sum, err = a1.Sum(testCtx)
	if err != nil || sum != 0 {
		t.Fatalf("Sum after Reset = %d, %v; want 0", sum, err)
	}

	// Cleanup transient keys.
	interopCleanupPattern(t, "{"+name+"}:*")
}

func TestRDoubleAdder(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "dadder")
	a := client.GetDoubleAdder(name)
	defer a.Destroy()

	a.Add(0.5)
	a.Increment()
	sum, err := a.Sum(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	if sum < 1.4 || sum > 1.6 {
		t.Fatalf("Sum = %v, want ~1.5", sum)
	}
	interopCleanupPattern(t, "{"+name+"}:*")
}

// TestJavaInterop_RLongAdder: Go and Java adder instances share one name —
// sums from either language include BOTH sides' buffered writes (the Java
// protocol: publish on {name}:adder-topic, flush to {name}:{id}:counter,
// barrier on {name}:{id}:semaphore).
func TestJavaInterop_RLongAdder(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-adder")
	t.Cleanup(func() { interopCleanupPattern(t, "{"+name+"}:*") })

	// Java creates its adder instance (subscribes to the topic).
	mustJava(t, "adder_create", name)
	time.Sleep(500 * time.Millisecond) // let the Java subscription settle

	a := client.GetLongAdder(name)
	defer a.Destroy()

	// Both sides buffer writes.
	a.Add(100)
	mustJava(t, "adder_add", name, "23")

	// Go's sum must flush BOTH buffers (Java instance responds to our topic
	// message and flushes its local total into our counter cell).
	if !eventual(t, 15*time.Second, func() bool {
		sum, err := a.Sum(testCtx)
		return err == nil && sum == 123
	}) {
		sum, _ := a.Sum(testCtx)
		t.Fatalf("Go Sum = %d, want 123 (100 Go + 23 Java)", sum)
	}

	// And Java's sum sees both sides too (Go listener flushes on request).
	mustJava(t, "adder_add", name, "7")
	if reply, err := javaSend("adder_sum " + name); err != nil {
		t.Fatal(err)
	} else if !numEq(reply["value"], 130) {
		t.Fatalf("java sum = %v; want 130 (123 + 7)", reply["value"])
	}
	// Non-destructive on both sides.
	if sum, err := a.Sum(testCtx); err != nil || sum != 130 {
		t.Fatalf("Go Sum after java sum = %d, %v; want 130 (non-destructive)", sum, err)
	}
}
