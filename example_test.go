package redi_test

import (
	"context"
	"fmt"
	"log"
	"time"

	redi "github.com/linkerlin/redi.go"
)

// The examples double as integration tests (skipped without Redis) and as
// godoc front-page material.

//nolint:gocritic // example code reads top-to-bottom
func ExampleNewClient() {
	client, err := redi.NewClient(redi.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close() //nolint:errcheck // example

	ctx := context.Background()

	// A distributed lock: ttl<=0 enables the watchdog auto-renewal.
	lock := client.GetLock("demo:lock")
	if err := lock.Lock(ctx, "worker-1:1", 0); err != nil {
		log.Fatal(err)
	}
	defer lock.Unlock(ctx, "worker-1:1") //nolint:errcheck // example

	// A distributed map (Redisson wire-compatible values).
	_, _ = client.GetKeys().Delete(ctx, "demo:map")
	m := client.GetMap("demo:map")
	_ = m.Put(ctx, "greeting", "hello")
	v, _ := m.Get(ctx, "greeting")
	fmt.Println(v)
	// Output: hello
}

func ExampleRLock() {
	client, err := redi.NewClient(redi.DefaultConfig())
	if err != nil {
		fmt.Println("skip:", err)
		return
	}
	defer client.Close() //nolint:errcheck // example

	ctx := context.Background()
	_, _ = client.GetKeys().Delete(ctx, "demo:try-lock")
	lock := client.GetRLock("demo:try-lock")

	// One-shot acquisition without blocking.
	ok, err := lock.TryLock(ctx, "app:42", time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("first acquire:", ok)

	// Re-entry by the same holder increments the counter.
	if err := lock.Lock(ctx, "app:42", time.Minute); err != nil {
		log.Fatal(err)
	}
	n, _ := lock.HoldCount(ctx, "app:42")
	fmt.Println("hold count:", n)

	_ = lock.Unlock(ctx, "app:42")
	_ = lock.Unlock(ctx, "app:42")
	// Output:
	// first acquire: true
	// hold count: 2
}

func ExampleRAtomicLong() {
	client, err := redi.NewClient(redi.DefaultConfig())
	if err != nil {
		fmt.Println("skip:", err)
		return
	}
	defer client.Close() //nolint:errcheck // example
	ctx := context.Background()
	_, _ = client.GetKeys().Delete(ctx, "demo:hits")
	counter := client.GetAtomicLong("demo:hits")
	counter.Set(ctx, 0) //nolint:errcheck // example

	n, _ := counter.IncrementAndGet(ctx)
	fmt.Println("after inc:", n)

	swapped, _ := counter.CompareAndSet(ctx, 1, 100)
	fmt.Println("cas 1->100:", swapped)
	// Output:
	// after inc: 1
	// cas 1->100: true
}

func ExampleRRateLimiter() {
	client, err := redi.NewClient(redi.DefaultConfig())
	if err != nil {
		fmt.Println("skip:", err)
		return
	}
	defer client.Close() //nolint:errcheck // example
	ctx := context.Background()

	// Config is persisted in Redis: every process sharing the name
	// shares the same sliding window. (Self-cleanup INCLUDING the
	// companion keys keeps the example re-runnable.)
	_, _ = client.GetKeys().Delete(ctx, "demo:api", "{demo:api}:value", "{demo:api}:permits")
	rl := client.GetRateLimiter("demo:api")
	_, _ = rl.TrySetRate(ctx, redi.RateTypeOverall, 2, time.Minute)

	first, _ := rl.TryAcquire(ctx, 1)
	second, _ := rl.TryAcquire(ctx, 1)
	third, _ := rl.TryAcquire(ctx, 1)
	fmt.Println(first, second, third)
	// Output: true true false
}

func ExampleRBatch() {
	client, err := redi.NewClient(redi.DefaultConfig())
	if err != nil {
		fmt.Println("skip:", err)
		return
	}
	defer client.Close() //nolint:errcheck // example
	ctx := context.Background()

	// Queue writes into one pipeline, flush in a single round-trip.
	_, _ = client.GetKeys().Delete(ctx, "demo:batch")
	batch := client.NewBatch()
	m := batch.GetMap("demo:batch")
	for i := 0; i < 100; i++ {
		_ = m.Put(ctx, fmt.Sprint(i), i)
	}
	if err := batch.Execute(ctx); err != nil {
		log.Fatal(err)
	}

	sz, _ := client.GetMap("demo:batch").Size(ctx)
	fmt.Println("batched size:", sz)
	_ = client.GetMap("demo:batch").Clear(ctx)
	// Output: batched size: 100
}
