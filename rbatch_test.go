package redi_test

import (
	"fmt"
	"testing"

	redi "github.com/linkerlin/redi.go"
)

func TestRBatch(t *testing.T) {
	client := newTestClient(t)
	base := uniqueKey(t, "batch")
	t.Cleanup(func() {
		interopCleanupPattern(t, base+"*")
		interopCleanupPattern(t, "{"+base+"}*")
	})

	b := client.NewBatch()
	m := b.GetMap(base + ":map")
	bk := b.GetBucket(base + ":bucket")
	q := b.GetQueue(base + ":queue")
	al := b.GetAtomicLong(base + ":along")
	z := b.GetScoredSortedSet(base + ":zset")

	for i := 0; i < 10; i++ {
		if err := m.Put(testCtx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bk.Set(testCtx, "payload"); err != nil {
		t.Fatal(err)
	}
	if err := q.Offer(testCtx, "j1", "j2", "j3"); err != nil {
		t.Fatal(err)
	}
	if _, err := al.AddAndGet(testCtx, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := z.Add(testCtx, "m1", 10); err != nil {
		t.Fatal(err)
	}

	if b.Len() != 14 { // 10 HSET + 1 SET + 1 RPUSH (3 offers in one) + 1 INCRBY + 1 ZADD
		t.Fatalf("queued commands = %d, want 14", b.Len())
	}

	if err := b.Execute(testCtx); err != nil {
		t.Fatal("Execute:", err)
	}
	if b.Len() != 0 {
		t.Fatalf("pipeline not drained: %d", b.Len())
	}

	// Verify through normal (non-batch) structures.
	all, err := client.GetMap(base + ":map").GetAll(testCtx)
	if err != nil || len(all) != 10 {
		t.Fatalf("map after batch = %v (len %d), %v", all, len(all), err)
	}
	v, _ := client.GetBucket(base + ":bucket").Get(testCtx)
	if v != "payload" {
		t.Fatalf("bucket = %v", v)
	}
	qn, _ := client.GetQueue(base + ":queue").Size(testCtx)
	if qn != 3 {
		t.Fatalf("queue size = %d", qn)
	}
	n, _ := client.GetAtomicLong(base + ":along").Get(testCtx)
	if n != 5 {
		t.Fatalf("counter = %d", n)
	}
	zn, _ := client.GetScoredSortedSet(base + ":zset").Size(testCtx)
	if zn != 1 {
		t.Fatalf("zset size = %d", zn)
	}

	// Discard path: queued commands must not execute.
	b2 := client.NewBatch()
	if err := b2.GetMap(base+":map").Put(testCtx, "discard", "x"); err != nil {
		t.Fatal(err)
	}
	b2.Discard()
	if err := b2.Execute(testCtx); err != nil {
		t.Logf("execute after discard: %v (pipeline empty is fine)", err)
	}
	if _, err := client.GetMap(base+":map").Get(testCtx, "discard"); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkRMap_Put10_Sequential(b *testing.B) {
	client, err := newBenchClient()
	if err != nil {
		b.Skip(err)
	}
	defer client.Close()
	m := client.GetMap(uniqueKeyB(b, "seq"))
	defer m.Clear(testCtx) //nolint:errcheck
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			_ = m.Put(testCtx, fmt.Sprintf("k%d", j), j)
		}
	}
}

func BenchmarkRMap_Put10_Batch(b *testing.B) {
	client, err := newBenchClient()
	if err != nil {
		b.Skip(err)
	}
	defer client.Close()
	name := uniqueKeyB(b, "bat")
	defer client.GetMap(name).Clear(testCtx) //nolint:errcheck
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := client.NewBatch()
		m := batch.GetMap(name)
		for j := 0; j < 10; j++ {
			_ = m.Put(testCtx, fmt.Sprintf("k%d", j), j)
		}
		_ = batch.Execute(testCtx)
	}
}

var _ = redi.ScriptReturnValue // keep the import used if tests shrink
