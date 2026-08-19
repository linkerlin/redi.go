package redi_test

import (
	"context"
	"testing"
	"time"
)

func TestRBucket(t *testing.T) {
	client := newTestClient(t)
	b := client.GetBucket(uniqueKey(t, "bucket"))
	defer b.Delete(testCtx) //nolint:errcheck

	v, err := b.Get(testCtx)
	if err != nil || v != nil {
		t.Fatalf("Get on empty = %v, %v; want nil", v, err)
	}

	if err := b.Set(testCtx, "hello"); err != nil {
		t.Fatal("Set:", err)
	}
	v, _ = b.Get(testCtx)
	if v != "hello" {
		t.Fatalf("Get = %v, want hello", v)
	}

	ok, err := b.TrySet(testCtx, "other")
	if err != nil || ok {
		t.Fatalf("TrySet on existing = %v, %v; want false", ok, err)
	}

	ok, err = b.CompareAndSet(testCtx, "hello", "world")
	if err != nil || !ok {
		t.Fatalf("CAS = %v, %v; want true", ok, err)
	}

	prev, err := b.GetAndSet(testCtx, "third")
	if err != nil || prev != "world" {
		t.Fatalf("GetAndSet = %v, %v; want world", prev, err)
	}

	prev, err = b.GetAndDelete(testCtx)
	if err != nil || prev != "third" {
		t.Fatalf("GetAndDelete = %v, %v; want third", prev, err)
	}
	v, _ = b.Get(testCtx)
	if v != nil {
		t.Fatal("bucket not deleted")
	}
}

func TestRBucket_TTL(t *testing.T) {
	client := newTestClient(t)
	b := client.GetBucket(uniqueKey(t, "bucketttl"))
	defer b.Delete(testCtx) //nolint:errcheck

	if err := b.SetWithTTL(testCtx, "x", 250*time.Millisecond); err != nil {
		t.Fatal("SetWithTTL:", err)
	}
	if !eventual(t, 2*time.Second, func() bool {
		v, _ := b.Get(testCtx)
		return v == nil
	}) {
		t.Fatal("bucket value did not expire (ms TTL broken)")
	}
}

func TestRBucket_SyncAPIs(t *testing.T) {
	client := newTestClient(t)
	b := client.GetBucket(uniqueKey(t, "bucket-sync"))
	defer b.Delete(testCtx) //nolint:errcheck

	if ok, err := b.TrySetWithTTL(testCtx, "first", 3*time.Second); err != nil || !ok {
		t.Fatalf("TrySetWithTTL = %v, %v", ok, err)
	}
	if err := b.SetAndKeepTTL(testCtx, "kept"); err != nil {
		t.Fatal("SetAndKeepTTL:", err)
	}
	if ttl, err := b.RemainTTL(testCtx); err != nil || ttl <= 0 {
		t.Fatalf("RemainTTL after SetAndKeepTTL = %v, %v", ttl, err)
	}
	if ok, err := b.SetIfExistsWithTTL(testCtx, "second", 3*time.Second); err != nil || !ok {
		t.Fatalf("SetIfExistsWithTTL = %v, %v", ok, err)
	}
	prev, err := b.GetAndSetWithTTL(testCtx, "third", 3*time.Second)
	if err != nil || prev != "second" {
		t.Fatalf("GetAndSetWithTTL = %v, %v", prev, err)
	}
	if size, err := b.Size(testCtx); err != nil || size == 0 {
		t.Fatalf("Size = %d, %v", size, err)
	}
	if ok, err := b.CompareAndDelete(testCtx, "wrong"); err != nil || ok {
		t.Fatalf("CompareAndDelete mismatch = %v, %v", ok, err)
	}
	if ok, err := b.CompareAndDelete(testCtx, "third"); err != nil || !ok {
		t.Fatalf("CompareAndDelete match = %v, %v", ok, err)
	}
}

func TestRBucket_GetAndExpire(t *testing.T) {
	client := newTestClient(t)
	b := client.GetBucket(uniqueKey(t, "bucket-getex"))
	defer b.Delete(testCtx) //nolint:errcheck

	if err := b.Set(testCtx, "live"); err != nil {
		t.Fatal(err)
	}
	got, err := b.GetAndExpire(testCtx, time.Minute)
	if err != nil || got != "live" {
		t.Fatalf("GetAndExpire = %v, %v", got, err)
	}
	ttl, err := b.RemainTTL(testCtx)
	if err != nil || ttl <= 0 {
		t.Fatalf("RemainTTL after GetAndExpire = %v, %v", ttl, err)
	}
	got, err = b.GetAndClearExpire(testCtx)
	if err != nil || got != "live" {
		t.Fatalf("GetAndClearExpire = %v, %v", got, err)
	}
	ttl, err = b.RemainTTL(testCtx)
	if err != nil || ttl >= 0 {
		t.Fatalf("RemainTTL after persist = %v, %v; want < 0", ttl, err)
	}
	got, err = b.GetAndExpireAt(testCtx, time.Now().Add(time.Minute))
	if err != nil || got != "live" {
		t.Fatalf("GetAndExpireAt = %v, %v", got, err)
	}

	dump, err := b.Dump(testCtx)
	if err != nil || len(dump) == 0 {
		t.Fatalf("Dump = %d, %v", len(dump), err)
	}
	copyName := uniqueKey(t, "bucket-copy")
	other := client.GetBucket(copyName)
	defer other.Delete(testCtx) //nolint:errcheck
	if ok, err := b.Copy(testCtx, copyName); err != nil || !ok {
		t.Fatalf("Copy = %v, %v", ok, err)
	}
	copied, err := other.Get(testCtx)
	if err != nil || copied != "live" {
		t.Fatalf("copied Get = %v, %v", copied, err)
	}
	if err := other.Set(testCtx, "overwrite"); err != nil {
		t.Fatal(err)
	}
	if ok, err := b.CopyAndReplace(testCtx, copyName); err != nil || !ok {
		t.Fatalf("CopyAndReplace = %v, %v", ok, err)
	}
	if n, err := b.SizeInMemory(testCtx); err != nil || n <= 0 {
		t.Fatalf("SizeInMemory = %d, %v", n, err)
	}
	if _, err := b.IdleTime(testCtx); err != nil {
		t.Fatalf("IdleTime: %v", err)
	}
	fresh := client.GetBucket(uniqueKey(t, "bucket-restore"))
	defer fresh.Delete(testCtx) //nolint:errcheck
	if err := fresh.Restore(testCtx, dump); err != nil {
		t.Fatal("Restore:", err)
	}
	got, err = fresh.Get(testCtx)
	if err != nil || got != "live" {
		t.Fatalf("Restore Get = %v, %v", got, err)
	}
	if err := fresh.RestoreAndReplace(testCtx, dump); err != nil {
		t.Fatal("RestoreAndReplace:", err)
	}
}

func TestRDeque(t *testing.T) {
	client := newTestClient(t)
	d := client.GetDeque(uniqueKey(t, "deque"))
	defer d.Clear(testCtx) //nolint:errcheck

	if err := d.AddFirst(testCtx, "b"); err != nil {
		t.Fatal("AddFirst:", err)
	}
	if err := d.AddLast(testCtx, "c"); err != nil {
		t.Fatal("AddLast:", err)
	}
	if err := d.AddFirst(testCtx, "a"); err != nil {
		t.Fatal("AddFirst 2:", err)
	}

	first, _ := d.PeekFirst(testCtx)
	last, _ := d.PeekLast(testCtx)
	if first != "a" || last != "c" {
		t.Fatalf("peeks = %v/%v, want a/c", first, last)
	}

	v, _ := d.RemoveFirst(testCtx)
	if v != "a" {
		t.Fatalf("RemoveFirst = %v", v)
	}
	v, _ = d.RemoveLast(testCtx)
	if v != "c" {
		t.Fatalf("RemoveLast = %v", v)
	}
	sz, _ := d.Size(testCtx)
	if sz != 1 {
		t.Fatalf("Size = %d, want 1", sz)
	}
}

func TestRDeque_OccurrenceAndMove(t *testing.T) {
	client := newTestClient(t)
	src := uniqueKey(t, "deque-occ")
	dst := uniqueKey(t, "deque-move")
	d := client.GetDeque(src)
	other := client.GetDeque(dst)
	t.Cleanup(func() { _ = d.Clear(testCtx); _ = other.Clear(testCtx) })

	_ = d.AddLast(testCtx, "a", "x", "b", "x")
	ok, err := d.RemoveFirstOccurrence(testCtx, "x")
	if err != nil || !ok {
		t.Fatalf("RemoveFirstOccurrence = %v, %v", ok, err)
	}
	all, err := d.ReadAll(testCtx)
	if err != nil || len(all) != 3 || all[0] != "a" || all[1] != "b" || all[2] != "x" {
		t.Fatalf("after first occurrence = %v, %v", all, err)
	}
	ok, err = d.RemoveLastOccurrence(testCtx, "x")
	if err != nil || !ok {
		t.Fatalf("RemoveLastOccurrence = %v, %v", ok, err)
	}

	_ = d.AddLast(testCtx, "tail")
	moved, err := d.Move(testCtx, dst, "RIGHT", "LEFT")
	if err != nil || moved != "tail" {
		t.Fatalf("Move = %v, %v", moved, err)
	}
	head, err := other.PeekFirst(testCtx)
	if err != nil || head != "tail" {
		t.Fatalf("dest head = %v, %v", head, err)
	}

	bd := client.GetBlockingDeque(src)
	_ = bd.AddFirst(testCtx, "blk")
	moved, err = bd.MoveWithTimeout(testCtx, dst, "LEFT", "RIGHT", time.Second)
	if err != nil || moved != "blk" {
		t.Fatalf("MoveWithTimeout = %v, %v", moved, err)
	}
}

func TestRDeque_PollN(t *testing.T) {
	client := newTestClient(t)
	d := client.GetDeque(uniqueKey(t, "deque-n"))
	defer d.Clear(testCtx) //nolint:errcheck

	_ = d.AddLast(testCtx, "a", "b", "c", "d")
	head, err := d.PollFirstN(testCtx, 2)
	if err != nil || len(head) != 2 || head[0] != "a" || head[1] != "b" {
		t.Fatalf("PollFirstN = %v, %v", head, err)
	}
	tail, err := d.PollLastN(testCtx, 2)
	if err != nil || len(tail) != 2 || tail[0] != "d" || tail[1] != "c" {
		t.Fatalf("PollLastN = %v, %v; want [d c]", tail, err)
	}
	empty, err := d.PollFirstN(testCtx, 3)
	if err != nil || len(empty) != 0 {
		t.Fatalf("PollFirstN empty = %v, %v", empty, err)
	}
}

func TestGetRedLockAlias(t *testing.T) {
	client := newTestClient(t)
	n1, n2, n3 := uniqueKey(t, "rl-a"), uniqueKey(t, "rl-b"), uniqueKey(t, "rl-c")
	t.Cleanup(func() { interopCleanup(t, n1, n2, n3) })
	rl := client.GetRedLock(client.GetLock(n1), client.GetLock(n2), client.GetLock(n3))
	ok, err := rl.TryLock(testCtx, client.HolderID("1"), time.Minute)
	if err != nil || !ok {
		t.Fatalf("GetRedLock TryLock = %v, %v", ok, err)
	}
	_ = rl.Unlock(testCtx, client.HolderID("1"))
}

func TestRBlockingQueue_Take(t *testing.T) {
	client := newTestClient(t)
	q := client.GetBlockingQueue(uniqueKey(t, "bq"))
	defer q.Clear(testCtx) //nolint:errcheck

	got := make(chan any, 1)
	go func() {
		v, _ := q.Take(context.Background())
		got <- v
	}()

	time.Sleep(150 * time.Millisecond)
	if err := q.Offer(testCtx, "job"); err != nil {
		t.Fatal("Offer:", err)
	}

	select {
	case v := <-got:
		if v != "job" {
			t.Fatalf("Take = %v", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Take did not return within 2s of Offer")
	}
}

func TestRScoredSortedSet(t *testing.T) {
	client := newTestClient(t)
	s := client.GetScoredSortedSet(uniqueKey(t, "zset"))
	defer s.Clear(testCtx) //nolint:errcheck

	if _, err := s.Add(testCtx, "alice", 90); err != nil {
		t.Fatal("Add:", err)
	}
	if _, err := s.Add(testCtx, "bob", 85); err != nil {
		t.Fatal("Add:", err)
	}

	score, _ := s.Score(testCtx, "alice")
	if score != 90 {
		t.Fatalf("Score = %v", score)
	}

	rank, _ := s.Rank(testCtx, "alice")
	if rank != 1 { // bob=85 ranks first
		t.Fatalf("Rank(alice) = %d, want 1", rank)
	}

	all, _ := s.Range(testCtx, 0, -1)
	if len(all) != 2 || all[0] != "bob" || all[1] != "alice" {
		t.Fatalf("Range = %v", all)
	}

	n, _ := s.Count(testCtx, 86, 100)
	if n != 1 {
		t.Fatalf("Count = %d, want 1", n)
	}

	newScore, err := s.AddScore(testCtx, "bob", 10)
	if err != nil || newScore != 95 {
		t.Fatalf("AddScore = %v, %v; want 95", newScore, err)
	}
}

func TestRAtomicDouble(t *testing.T) {
	client := newTestClient(t)
	a := client.GetAtomicDouble(uniqueKey(t, "adbl"))
	defer a.Delete(testCtx) //nolint:errcheck

	v, err := a.AddAndGet(testCtx, 0.5)
	if err != nil || v != 0.5 {
		t.Fatalf("AddAndGet = %v, %v; want 0.5", v, err)
	}
	v, _ = a.IncrementAndGet(testCtx)
	if v != 1.5 {
		t.Fatalf("IncrementAndGet = %v", v)
	}
	ok, _ := a.CompareAndSet(testCtx, 1.5, 2.5)
	if !ok {
		t.Fatal("CAS failed")
	}
	got, _ := a.Get(testCtx)
	if got != 2.5 {
		t.Fatalf("Get = %v", got)
	}
}
