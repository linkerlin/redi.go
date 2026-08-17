package redi_test

import (
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

// Second maintenance pass: hit remaining near-0% surfaces that do not need
// Redis modules or destructive Flush/Migrate.

func TestE2E_KeySynchronizers_MapSet(t *testing.T) {
	client := newTestClient(t)
	m := client.GetMap(uniqueKey(t, "e2e-sync-map"))
	t.Cleanup(func() { _ = m.Clear(testCtx) })
	pes := m.GetPermitExpirableSemaphore("k")
	t.Cleanup(func() { _ = pes.Delete(testCtx) })
	if ok, err := pes.TrySetPermits(testCtx, 1); err != nil || !ok {
		t.Fatalf("TrySetPermits = %v, %v", ok, err)
	}
	id, err := pes.Acquire(testCtx, time.Second)
	if err != nil || id == "" {
		t.Fatalf("Acquire = %q, %v", id, err)
	}
	if ok, err := pes.Release(testCtx, id); err != nil || !ok {
		t.Fatalf("Release = %v, %v", ok, err)
	}

	s := client.GetSet(uniqueKey(t, "e2e-sync-set"))
	t.Cleanup(func() { _ = s.Clear(testCtx) })
	latch := s.GetCountDownLatch("v")
	t.Cleanup(func() { _ = latch.Delete(testCtx) })
	if ok, err := latch.TrySetCount(testCtx, 1); err != nil || !ok {
		t.Fatalf("TrySetCount = %v, %v", ok, err)
	}
	if n, err := latch.GetCount(testCtx); err != nil || n != 1 {
		t.Fatalf("GetCount = %d, %v", n, err)
	}
	_ = latch.CountDown(testCtx)
}

func TestE2E_DoubleAdder_DecReset(t *testing.T) {
	client := newTestClient(t)
	a := client.GetDoubleAdder(uniqueKey(t, "e2e-dadder"))
	t.Cleanup(a.Destroy)
	a.Increment()
	a.Decrement()
	if sum, err := a.Sum(testCtx); err != nil || sum != 0 {
		t.Fatalf("Sum after Inc/Dec = %v, %v", sum, err)
	}
	a.Add(3.5)
	if err := a.Reset(testCtx); err != nil {
		t.Fatal(err)
	}
	if sum, err := a.Sum(testCtx); err != nil || sum != 0 {
		t.Fatalf("Sum after Reset = %v, %v", sum, err)
	}
}

func TestE2E_MapCache_GetIntoEvictListener(t *testing.T) {
	client := newTestClient(t)
	m := client.GetMapCache(uniqueKey(t, "e2e-mc-into"))
	t.Cleanup(func() { _ = m.Clear(testCtx) })

	if err := m.Put(testCtx, "f", "hello", time.Minute, time.Minute); err != nil {
		t.Fatal(err)
	}
	var got string
	ok, err := m.GetInto(testCtx, "f", &got)
	if err != nil || !ok || got != "hello" {
		t.Fatalf("GetInto = %q, %v, %v", got, ok, err)
	}

	id, err := m.AddListener(redi.EventCreated, func(_, _, _, _ any) {})
	if err != nil {
		t.Fatal(err)
	}
	if !m.RemoveListener(redi.EventCreated, id) {
		t.Fatal("RemoveListener = false")
	}
	m.StartAutoEviction(50 * time.Millisecond)
}

func TestE2E_RedLock_WaitAndLock(t *testing.T) {
	client := newTestClient(t)
	names := []string{
		uniqueKey(t, "e2e-rl1"),
		uniqueKey(t, "e2e-rl2"),
		uniqueKey(t, "e2e-rl3"),
	}
	t.Cleanup(func() { interopCleanup(t, names...) })
	locks := []*redi.RLock{
		client.GetLock(names[0]),
		client.GetLock(names[1]),
		client.GetLock(names[2]),
	}
	red := client.NewRedLock(locks...)
	holder := client.HolderID("1")

	ok, err := red.TryLockWait(testCtx, holder, 200*time.Millisecond, time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLockWait = %v, %v", ok, err)
	}
	_ = red.Unlock(testCtx, holder)

	if err := red.Lock(testCtx, holder, time.Minute, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	_ = red.Unlock(testCtx, holder)
}

func TestE2E_BlockingQueue_PollWithTimeout(t *testing.T) {
	client := newTestClient(t)
	q := client.GetBlockingQueue(uniqueKey(t, "e2e-bq-poll"))
	t.Cleanup(func() { _ = q.Clear(testCtx) })
	_ = q.Offer(testCtx, "x")
	v, err := q.PollWithTimeout(testCtx, time.Second)
	if err != nil || v != "x" {
		t.Fatalf("PollWithTimeout = %v, %v", v, err)
	}
	v, err = q.PollWithTimeout(testCtx, time.Millisecond)
	if err != nil || v != nil {
		t.Fatalf("empty PollWithTimeout = %v, %v", v, err)
	}
}

func TestE2E_PriorityBlockingDeque_PollLast(t *testing.T) {
	client := newTestClient(t)
	q := client.GetPriorityBlockingDeque(uniqueKey(t, "e2e-pbd"))
	t.Cleanup(func() { _ = q.Clear(testCtx) })
	_ = q.Offer(testCtx, "low", 1)
	_ = q.Offer(testCtx, "high", 10)
	v, err := q.PollLastWithTimeout(testCtx, time.Second)
	if err != nil || v != "high" {
		t.Fatalf("PollLastWithTimeout = %v, %v", v, err)
	}
}

func TestE2E_DelayedQueue_PeekReady(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-dq-peek")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	dq := client.GetDelayedQueue(name)
	if err := dq.Offer(testCtx, "soon", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	eventual(t, 2*time.Second, func() bool {
		v, err := dq.Peek(testCtx)
		return err == nil && v == "soon"
	})
}

func TestE2E_List_RemoveByIndex(t *testing.T) {
	client := newTestClient(t)
	list := client.GetList(uniqueKey(t, "e2e-list-idx"))
	t.Cleanup(func() { _ = list.Clear(testCtx) })
	_ = list.Add(testCtx, "a", "b", "c")
	v, err := list.RemoveByIndex(testCtx, 1)
	if err != nil || v != "b" {
		t.Fatalf("RemoveByIndex = %v, %v", v, err)
	}
}

func TestE2E_RateLimiter_KeepAliveTrySet(t *testing.T) {
	client := newTestClient(t)
	rl := client.GetRateLimiter(uniqueKey(t, "e2e-rl-ka"))
	t.Cleanup(func() { _ = rl.Delete(testCtx) })
	ok, err := rl.TrySetRateWithKeepAlive(testCtx, redi.RateTypeOverall, 5, time.Second, 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("TrySetRateWithKeepAlive = %v, %v", ok, err)
	}
	ok, err = rl.TrySetRateWithKeepAlive(testCtx, redi.RateTypeOverall, 1, time.Second, 2*time.Second)
	if err != nil || ok {
		t.Fatalf("second TrySet = %v, %v; want false", ok, err)
	}
}

func TestE2E_LocalCachedMap_PutIfAbsent(t *testing.T) {
	client := newTestClient(t)
	m := client.GetLocalCachedMap(uniqueKey(t, "e2e-lcm-pia"))
	t.Cleanup(func() { _ = m.Clear(testCtx); m.Destroy() })
	ok, err := m.PutIfAbsent(testCtx, "k", "v1")
	if err != nil || !ok {
		t.Fatalf("PutIfAbsent = %v, %v", ok, err)
	}
	ok, err = m.PutIfAbsent(testCtx, "k", "v2")
	if err != nil || ok {
		t.Fatalf("second PutIfAbsent = %v, %v", ok, err)
	}
}

func TestE2E_BoundedBQ_Put(t *testing.T) {
	client := newTestClient(t)
	q := client.GetBoundedBlockingQueue(uniqueKey(t, "e2e-bbq-put"))
	t.Cleanup(func() { _ = q.Delete(testCtx) })
	_ = q.Delete(testCtx)
	if ok, err := q.TrySetCapacity(testCtx, 2); err != nil || !ok {
		t.Fatalf("TrySetCapacity = %v, %v", ok, err)
	}
	if err := q.Put(testCtx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := q.Put(testCtx, "b"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- q.Put(testCtx, "c")
	}()
	time.Sleep(30 * time.Millisecond)
	if _, err := q.Poll(testCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Put did not complete")
	}
}

func TestE2E_BinaryStream_Write(t *testing.T) {
	client := newTestClient(t)
	s := client.GetBinaryStream(uniqueKey(t, "e2e-bin-w"))
	t.Cleanup(func() { _ = s.Delete(testCtx) })
	if err := s.Set(testCtx, []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	n, err := s.Write(testCtx, 2, []byte("XY"))
	if err != nil || n != 2 {
		t.Fatalf("Write = %d, %v", n, err)
	}
	got, err := s.Get(testCtx)
	if err != nil || string(got) != "abXY" {
		t.Fatalf("Get = %q, %v", got, err)
	}
}

func TestE2E_FencedLock_LockAndGetToken(t *testing.T) {
	client := newTestClient(t)
	holder := client.HolderID("1")
	lock := client.GetFencedLock(uniqueKey(t, "e2e-fenced-lock"))
	t.Cleanup(func() { _, _ = lock.ForceUnlock(testCtx) })
	tok, err := lock.LockAndGetToken(testCtx, holder, time.Minute)
	if err != nil || tok == 0 {
		t.Fatalf("LockAndGetToken = %d, %v", tok, err)
	}
	_ = lock.Unlock(testCtx, holder)
}

func TestE2E_RWLock_WriteLock(t *testing.T) {
	client := newTestClient(t)
	holder := client.HolderID("1")
	rw := client.GetReadWriteLock(uniqueKey(t, "e2e-rw-lock"))
	wl := rw.WriteLock()
	t.Cleanup(func() { _, _ = wl.ForceUnlock(testCtx) })
	if err := wl.Lock(testCtx, holder, time.Minute); err != nil {
		t.Fatal(err)
	}
	_ = wl.Unlock(testCtx, holder)
}
