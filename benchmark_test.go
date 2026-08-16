package redi_test

import (
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

// Benchmarks for the hot paths (run: go test -bench . -benchtime 1s).
// They quantify the codec-layer overhead called out in 演进方案.md P5-4.

func BenchmarkRAtomicLong_Increment(b *testing.B) {
	client, err := newBenchClient()
	if err != nil {
		b.Skip(err)
	}
	defer client.Close() //nolint:errcheck // test cleanup
	a := client.GetAtomicLong(uniqueKeyB(b, "along"))
	defer a.Delete(testCtx) //nolint:errcheck
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := a.IncrementAndGet(testCtx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRMap_PutGet(b *testing.B) {
	client, err := newBenchClient()
	if err != nil {
		b.Skip(err)
	}
	defer client.Close() //nolint:errcheck // test cleanup
	m := client.GetMap(uniqueKeyB(b, "map"))
	defer m.Clear(testCtx) //nolint:errcheck
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := m.Put(testCtx, "k", "v"); err != nil {
			b.Fatal(err)
		}
		if _, err := m.Get(testCtx, "k"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRLock_TryUnlock(b *testing.B) {
	client, err := newBenchClient()
	if err != nil {
		b.Skip(err)
	}
	defer client.Close() //nolint:errcheck // test cleanup
	l := client.GetLock(uniqueKeyB(b, "lock"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := l.Lock(testCtx, "bench", time.Minute); err != nil {
			b.Fatal(err)
		}
		if err := l.Unlock(testCtx, "bench"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRBucket_SetGet(b *testing.B) {
	client, err := newBenchClient()
	if err != nil {
		b.Skip(err)
	}
	defer client.Close() //nolint:errcheck // test cleanup
	bk := client.GetBucket(uniqueKeyB(b, "bucket"))
	defer bk.Delete(testCtx) //nolint:errcheck
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bk.Set(testCtx, "v"); err != nil {
			b.Fatal(err)
		}
		if _, err := bk.Get(testCtx); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchClient() (*redi.Client, error) {
	cfg := redi.DefaultConfig()
	return redi.NewClient(cfg)
}

func uniqueKeyB(b *testing.B, suffix string) string {
	return "redi:bench:" + b.Name() + ":" + suffix
}
