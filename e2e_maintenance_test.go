package redi_test

import (
	"context"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

// Maintenance coverage: hit previously 0% surfaces that don't need optional modules.

func TestE2E_DelayedQueue_Sizes(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-dq-size")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	dq := client.GetDelayedQueue(name)

	if err := dq.Offer(testCtx, "a", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if n, err := dq.DelayedSize(testCtx); err != nil || n < 1 {
		t.Fatalf("DelayedSize = %d, %v", n, err)
	}
	if n, err := dq.ReadySize(testCtx); err != nil || n != 0 {
		t.Fatalf("ReadySize = %d, %v", n, err)
	}
	if n, err := dq.Size(testCtx); err != nil || n < 1 {
		t.Fatalf("Size = %d, %v", n, err)
	}
}

func TestE2E_LexSortedSet_Gaps(t *testing.T) {
	client := newTestClient(t)
	s := client.GetLexSortedSet(uniqueKey(t, "e2e-lex"))
	t.Cleanup(func() { _ = s.Clear(testCtx) })

	for _, m := range []string{"a", "b", "c", "d"} {
		if ok, err := s.Add(testCtx, m); err != nil || !ok {
			t.Fatalf("Add(%s) = %v, %v", m, ok, err)
		}
	}
	if ok, err := s.Contains(testCtx, "b"); err != nil || !ok {
		t.Fatalf("Contains = %v, %v", ok, err)
	}
	if n, err := s.CountHead(testCtx, "c"); err != nil || n != 2 {
		t.Fatalf("CountHead = %d, %v", n, err)
	}
	rng, err := s.Range(testCtx, 0, 1)
	if err != nil || len(rng) != 2 || rng[0] != "a" {
		t.Fatalf("Range = %v, %v", rng, err)
	}
	if n, err := s.RemoveRangeTail(testCtx, "c"); err != nil || n < 1 {
		t.Fatalf("RemoveRangeTail = %d, %v", n, err)
	}
	if ok, err := s.Remove(testCtx, "a"); err != nil || !ok {
		t.Fatalf("Remove = %v, %v", ok, err)
	}
	if _, err := s.Add(testCtx, "m"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(testCtx, "n"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.RemoveRangeByLex(testCtx, "m", "n"); err != nil || n < 1 {
		t.Fatalf("RemoveRangeByLex = %d, %v", n, err)
	}
}

func TestE2E_RateLimiter_AcquireRelease(t *testing.T) {
	client := newTestClient(t)
	rl := client.GetRateLimiter(uniqueKey(t, "e2e-rl"))
	t.Cleanup(func() { _ = rl.Delete(testCtx) })

	if err := rl.SetRate(testCtx, redi.RateTypeOverall, 2, time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(testCtx, 2*time.Second)
	defer cancel()
	if err := rl.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := rl.Release(testCtx, 1); err != nil {
		t.Fatal(err)
	}
	if n, err := rl.AvailablePermits(testCtx); err != nil || n < 1 {
		t.Fatalf("AvailablePermits = %d, %v", n, err)
	}
}

func TestE2E_Stream_ClaimTrimConsumer(t *testing.T) {
	client := newTestClient(t)
	st := client.GetStream(uniqueKey(t, "e2e-stream-claim"))
	t.Cleanup(func() { _ = st.Delete(testCtx) })

	id1, err := st.Add(testCtx, map[string]any{"v": "1"})
	if err != nil {
		t.Fatal(err)
	}
	id2, err := st.Add(testCtx, map[string]any{"v": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := st.CreateGroup(testCtx, "g", "0"); err != nil || !ok {
		t.Fatalf("CreateGroup = %v, %v", ok, err)
	}
	if _, err := st.CreateConsumer(testCtx, "g", "c1"); err != nil {
		t.Fatal(err)
	}
	entries, err := st.ReadGroup(testCtx, "g", "c1", 10, 0)
	if err != nil || len(entries) < 2 {
		t.Fatalf("ReadGroup = %v, %v", entries, err)
	}
	claimed, err := st.Claim(testCtx, "g", "c2", 0, id1, id2)
	if err != nil || len(claimed) < 1 {
		t.Fatalf("Claim = %v, %v", claimed, err)
	}
	if _, err := st.Ack(testCtx, "g", id1, id2); err != nil {
		t.Fatal(err)
	}
	if n, err := st.RemoveConsumer(testCtx, "g", "c1"); err != nil || n < 0 {
		t.Fatalf("RemoveConsumer = %d, %v", n, err)
	}
	if _, err := st.TrimMinID(testCtx, id2, false); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_LocalCachedMap_WriteSurface(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-lcm")
	m := client.GetLocalCachedMap(name)
	t.Cleanup(func() {
		_ = m.Clear(testCtx)
		m.Destroy()
	})

	if err := m.Put(testCtx, "a", "1"); err != nil {
		t.Fatal(err)
	}
	var got string
	ok, err := m.GetInto(testCtx, "a", &got)
	if err != nil || !ok || got != "1" {
		t.Fatalf("GetInto = %q, %v, %v", got, ok, err)
	}
	if err := m.PutAll(testCtx, map[string]any{"b": "2", "c": "3"}); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.PutIfAbsent(testCtx, "b", "x"); err != nil || ok {
		t.Fatalf("PutIfAbsent existing = %v, %v", ok, err)
	}
	if ok, err := m.FastPut(testCtx, "d", "4"); err != nil || !ok {
		t.Fatalf("FastPut = %v, %v", ok, err)
	}
	prev, err := m.Replace(testCtx, "a", "9")
	if err != nil || prev != "1" {
		t.Fatalf("Replace = %v, %v", prev, err)
	}
	if ok, err := m.ReplaceIf(testCtx, "a", "9", "10"); err != nil || !ok {
		t.Fatalf("ReplaceIf = %v, %v", ok, err)
	}
	if ok, err := m.FastPutIfExists(testCtx, "a", "11"); err != nil || !ok {
		t.Fatalf("FastPutIfExists = %v, %v", ok, err)
	}
	if ok, err := m.FastReplace(testCtx, "b", "22"); err != nil || !ok {
		t.Fatalf("FastReplace = %v, %v", ok, err)
	}
	if n, err := m.FastRemove(testCtx, "c"); err != nil || n != 1 {
		t.Fatalf("FastRemove = %d, %v", n, err)
	}
}

func TestE2E_BoundedBQ_PollIntoClear(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-bbq")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	q := client.GetBoundedBlockingQueue(name)
	if ok, err := q.TrySetCapacity(testCtx, 4); err != nil || !ok {
		t.Fatalf("TrySetCapacity = %v, %v", ok, err)
	}
	if ok, err := q.Offer(testCtx, "x"); err != nil || !ok {
		t.Fatalf("Offer = %v, %v", ok, err)
	}
	var v string
	ok, err := q.PollInto(testCtx, &v)
	if err != nil || !ok || v != "x" {
		t.Fatalf("PollInto = %q, %v, %v", v, ok, err)
	}
	if _, err := q.Offer(testCtx, "y"); err != nil {
		t.Fatal(err)
	}
	if err := q.Clear(testCtx); err != nil {
		t.Fatal(err)
	}
	if err := q.Delete(testCtx); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_LocksStatusAndWait(t *testing.T) {
	client := newTestClient(t)
	holder := client.HolderID("1")

	lock := client.GetLock(uniqueKey(t, "e2e-lock-status"))
	t.Cleanup(func() { _, _ = lock.ForceUnlock(testCtx) })
	if err := lock.Lock(testCtx, holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	if locked, err := lock.IsLocked(testCtx); err != nil || !locked {
		t.Fatalf("IsLocked = %v, %v", locked, err)
	}
	ok, err := lock.TryLockWait(testCtx, client.HolderID("2"), 30*time.Millisecond, time.Minute)
	if err != nil || ok {
		t.Fatalf("TryLockWait = %v, %v", ok, err)
	}
	_ = lock.Unlock(testCtx, holder)

	fair := client.GetFairLock(uniqueKey(t, "e2e-fair-status"))
	t.Cleanup(func() { _, _ = fair.ForceUnlock(testCtx) })
	if err := fair.Lock(testCtx, holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	if locked, err := fair.IsLocked(testCtx); err != nil || !locked {
		t.Fatalf("fair IsLocked = %v, %v", locked, err)
	}
	_ = fair.Unlock(testCtx, holder)

	spin := client.GetSpinLock(uniqueKey(t, "e2e-spin-wait"))
	t.Cleanup(func() { _, _ = spin.ForceUnlock(testCtx) })
	if ok, err := spin.TryLock(testCtx, holder, time.Minute); err != nil || !ok {
		t.Fatalf("spin TryLock = %v, %v", ok, err)
	}
	ok, err = spin.TryLockWait(testCtx, client.HolderID("2"), 20*time.Millisecond, time.Minute)
	if err != nil || ok {
		t.Fatalf("spin TryLockWait = %v, %v", ok, err)
	}
	_ = spin.Unlock(testCtx, holder)
}

func TestE2E_MiscLowCoverage(t *testing.T) {
	client := newTestClient(t)
	holder := client.HolderID("1")

	nodes := client.GetRedisNodes()
	if nodes.Mode() != client.Config().Mode {
		t.Fatalf("Mode = %v", nodes.Mode())
	}
	if _, err := nodes.Info(testCtx); err != nil {
		t.Fatal(err)
	}
	if nodes.String() == "" {
		t.Fatal("String empty")
	}

	b := client.GetBucket(uniqueKey(t, "e2e-bucket-exists"))
	t.Cleanup(func() { _ = b.Delete(testCtx) })
	if err := b.Set(testCtx, "v"); err != nil {
		t.Fatal(err)
	}
	if ok, err := b.SetIfExists(testCtx, "w"); err != nil || !ok {
		t.Fatalf("SetIfExists = %v, %v", ok, err)
	}

	al := client.GetAtomicLong(uniqueKey(t, "e2e-along"))
	t.Cleanup(func() { _ = al.Delete(testCtx) })
	_, _ = al.Set(testCtx, 10)
	if prev, err := al.GetAndAdd(testCtx, 2); err != nil || prev != 10 {
		t.Fatalf("GetAndAdd = %d, %v", prev, err)
	}
	if prev, err := al.GetAndIncrement(testCtx); err != nil || prev != 12 {
		t.Fatalf("GetAndIncrement = %d, %v", prev, err)
	}
	if prev, err := al.GetAndDecrement(testCtx); err != nil || prev != 13 {
		t.Fatalf("GetAndDecrement = %d, %v", prev, err)
	}
	if prev, err := al.GetAndSet(testCtx, 1); err != nil || prev != 12 {
		t.Fatalf("GetAndSet = %d, %v", prev, err)
	}
	if prev, err := al.GetAndDelete(testCtx); err != nil || prev != 1 {
		t.Fatalf("GetAndDelete = %d, %v", prev, err)
	}

	ad := client.GetAtomicDouble(uniqueKey(t, "e2e-adouble"))
	t.Cleanup(func() { _ = ad.Delete(testCtx) })
	_, _ = ad.Set(testCtx, 5)
	if v, err := ad.DecrementAndGet(testCtx); err != nil || v != 4 {
		t.Fatalf("DecrementAndGet = %v, %v", v, err)
	}
	if prev, err := ad.GetAndSet(testCtx, 9); err != nil || prev != 4 {
		t.Fatalf("GetAndSet = %v, %v", prev, err)
	}
	if prev, err := ad.GetAndDelete(testCtx); err != nil || prev != 9 {
		t.Fatalf("GetAndDelete = %v, %v", prev, err)
	}

	adder := client.GetLongAdder(uniqueKey(t, "e2e-adder"))
	t.Cleanup(func() { adder.Destroy() })
	adder.Add(3)
	adder.Decrement()
	if sum, err := adder.Sum(testCtx); err != nil || sum != 2 {
		t.Fatalf("Sum = %d, %v", sum, err)
	}
	if err := adder.Reset(testCtx); err != nil {
		t.Fatal(err)
	}

	sem := client.GetSemaphore(uniqueKey(t, "e2e-sem"))
	t.Cleanup(func() { _ = sem.Delete(testCtx) })
	_, _ = sem.TrySetPermits(testCtx, 3)
	if n, err := sem.DrainPermits(testCtx); err != nil || n != 3 {
		t.Fatalf("DrainPermits = %d, %v", n, err)
	}
	if err := sem.AddPermits(testCtx, 2); err != nil {
		t.Fatal(err)
	}

	sc := client.GetSetCache(uniqueKey(t, "e2e-sc-rm"))
	t.Cleanup(func() { _ = sc.Clear(testCtx) })
	_, _ = sc.Add(testCtx, "z", time.Minute, 0)
	if ok, err := sc.Remove(testCtx, "z"); err != nil || !ok {
		t.Fatalf("SetCache Remove = %v, %v", ok, err)
	}

	pq := client.GetPriorityQueue(uniqueKey(t, "e2e-pq"))
	t.Cleanup(func() { _ = pq.Clear(testCtx) })
	_ = pq.Offer(testCtx, "a", 1)
	_ = pq.Offer(testCtx, "b", 2)
	if n, err := pq.Size(testCtx); err != nil || n != 2 {
		t.Fatalf("Size = %d, %v", n, err)
	}
	if err := pq.Remove(testCtx, "a"); err != nil {
		t.Fatal(err)
	}

	fenced := client.GetFencedLock(uniqueKey(t, "e2e-fenced"))
	t.Cleanup(func() { _, _ = fenced.ForceUnlock(testCtx) })
	tok, err := fenced.LockAndGetToken(testCtx, holder, time.Minute)
	if err != nil || tok == 0 {
		t.Fatalf("LockAndGetToken = %d, %v", tok, err)
	}
	_ = fenced.Unlock(testCtx, holder)

	bloom := client.GetBloomFilter(uniqueKey(t, "e2e-bloom-meta"))
	t.Cleanup(func() { _ = bloom.Delete(testCtx) })
	_, _ = bloom.TryInit(testCtx, 1000, 0.01)
	if n, err := bloom.ExpectedInsertions(testCtx); err != nil || n != 1000 {
		t.Fatalf("ExpectedInsertions = %d, %v", n, err)
	}
	if p, err := bloom.FalseProbability(testCtx); err != nil || p != 0.01 {
		t.Fatalf("FalseProbability = %v, %v", p, err)
	}

	fn := client.GetFunction()
	if err := fn.Flush(testCtx); err != nil {
		t.Log("Function Flush:", err) // may be empty / restricted
	}

	bdq := client.GetBlockingDeque(uniqueKey(t, "e2e-deque-to"))
	t.Cleanup(func() { _ = bdq.Clear(testCtx) })
	_ = bdq.AddFirst(testCtx, "head")
	if v, err := bdq.PollFirstWithTimeout(testCtx, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	} else if v != "head" {
		t.Fatalf("PollFirstWithTimeout = %v", v)
	}

	list := client.GetList(uniqueKey(t, "e2e-list-rm"))
	t.Cleanup(func() { _ = list.Clear(testCtx) })
	_ = list.Add(testCtx, "x", "y")
	if err := list.Remove(testCtx, 1, "x"); err != nil {
		t.Fatal(err)
	}

	hll := client.GetHyperLogLog(uniqueKey(t, "e2e-hll"))
	t.Cleanup(func() { _ = hll.Delete(testCtx) })
	if _, err := hll.AddAll(testCtx, "a", "b"); err != nil {
		t.Fatal(err)
	}
	other := client.GetHyperLogLog(uniqueKey(t, "e2e-hll2"))
	t.Cleanup(func() { _ = other.Delete(testCtx) })
	_, _ = other.Add(testCtx, "c")
	if n, err := hll.CountWith(testCtx, other.Name()); err != nil || n < 2 {
		t.Fatalf("CountWith = %d, %v", n, err)
	}
	if err := hll.MergeWith(testCtx, other.Name()); err != nil {
		t.Fatal(err)
	}
	dest := uniqueKey(t, "e2e-hll-dest")
	t.Cleanup(func() { interopCleanup(t, dest) })
	if err := hll.MergeInto(testCtx, dest, other.Name()); err != nil {
		t.Fatal(err)
	}

	geo := client.GetGeo(uniqueKey(t, "e2e-geo"))
	t.Cleanup(func() { _ = geo.Delete(testCtx) })
	_, _ = geo.Add(testCtx, 13.0, 52.0, "berlin")
	if h, err := geo.Hash(testCtx, "berlin"); err != nil || h == "" {
		t.Fatalf("Hash = %q, %v", h, err)
	}
	if n, err := geo.Size(testCtx); err != nil || n != 1 {
		t.Fatalf("Size = %d, %v", n, err)
	}
	if _, err := geo.SearchByMember(testCtx, "berlin", 100, "km", true, true); err != nil {
		t.Fatal(err)
	}
	if err := geo.Remove(testCtx, "berlin"); err != nil {
		t.Fatal(err)
	}

	rt := client.GetReliableTopic(uniqueKey(t, "e2e-rt-size"))
	t.Cleanup(func() { _ = rt.Delete(testCtx) })
	if _, err := rt.Size(testCtx); err != nil {
		t.Fatal(err)
	}

	rw := client.GetReadWriteLock(uniqueKey(t, "e2e-rw-status"))
	wl := rw.WriteLock()
	t.Cleanup(func() { _, _ = wl.ForceUnlock(testCtx) })
	if err := wl.Lock(testCtx, holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	if locked, err := wl.IsLocked(testCtx); err != nil || !locked {
		t.Fatalf("rw IsLocked = %v, %v", locked, err)
	}
	if held, err := wl.IsHeldBy(testCtx, holder); err != nil || !held {
		t.Fatalf("rw IsHeldBy = %v, %v", held, err)
	}
	_ = wl.Unlock(testCtx, holder)

	keys := client.GetKeys()
	k := uniqueKey(t, "e2e-keys-unlink")
	_ = client.GetBucket(k).Set(testCtx, "1")
	if n, err := keys.Unlink(testCtx, k); err != nil || n < 1 {
		t.Fatalf("Unlink = %d, %v", n, err)
	}
	pat := uniqueKey(t, "e2e-keys-pat")
	_ = client.GetBucket(pat+"-a").Set(testCtx, "1")
	if n, err := keys.UnlinkByPattern(testCtx, pat+"*"); err != nil || n < 1 {
		t.Fatalf("UnlinkByPattern = %d, %v", n, err)
	}

	nrl := client.GetNonReentrantLock(uniqueKey(t, "e2e-nrl-status"))
	t.Cleanup(func() { _, _ = nrl.ForceUnlock(testCtx) })
	if err := nrl.Lock(testCtx, holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	if locked, err := nrl.IsLocked(testCtx); err != nil || !locked {
		t.Fatalf("nrl IsLocked = %v, %v", locked, err)
	}
	if held, err := nrl.IsHeldBy(testCtx, holder); err != nil || !held {
		t.Fatalf("nrl IsHeldBy = %v, %v", held, err)
	}
	ok, err := nrl.TryLockWait(testCtx, client.HolderID("2"), 20*time.Millisecond, time.Minute)
	if err != nil || ok {
		t.Fatalf("nrl TryLockWait = %v, %v", ok, err)
	}
	_ = nrl.Unlock(testCtx, holder)

	obj := client.GetBucket(uniqueKey(t, "e2e-obj-tools2"))
	t.Cleanup(func() { interopCleanup(t, obj.Name()) })
	_ = obj.Set(testCtx, "v")
	if ok, err := obj.Touch(testCtx); err != nil || !ok {
		t.Fatalf("Touch = %v, %v", ok, err)
	}
	if ok, err := obj.ExpireAt(testCtx, time.Now().Add(time.Minute)); err != nil || !ok {
		t.Fatalf("ExpireAt = %v, %v", ok, err)
	}
	if ok, err := obj.ClearExpire(testCtx); err != nil || !ok {
		t.Fatalf("ClearExpire = %v, %v", ok, err)
	}
	if err := obj.Unlink(testCtx); err != nil {
		t.Fatal(err)
	}

	q := client.GetQueue(uniqueKey(t, "e2e-q-take"))
	t.Cleanup(func() { _ = q.Clear(testCtx) })
	_ = q.Offer(testCtx, "t")
	if v, err := q.Take(testCtx, time.Second); err != nil || v != "t" {
		t.Fatalf("Take = %v, %v", v, err)
	}
}
