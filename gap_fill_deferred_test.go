package redi_test

import (
	"strings"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

func skipArray(t *testing.T, err error) bool {
	t.Helper()
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "unknown command") || strings.Contains(s, "arset") ||
		strings.Contains(s, "array") {
		t.Skip("Redis ARRAY commands unavailable:", err)
		return true
	}
	return false
}

func TestRArray_Core(t *testing.T) {
	client := newTestClient(t)
	arr := client.GetArray(uniqueKey(t, "array"))
	t.Cleanup(func() { _ = arr.Clear(testCtx) })

	n, err := arr.Set(testCtx, 0, "hello")
	if skipArray(t, err) {
		return
	}
	if err != nil || n != 1 {
		t.Fatalf("Set = %d, %v", n, err)
	}
	v, err := arr.Get(testCtx, 0)
	if err != nil || v != "hello" {
		t.Fatalf("Get = %v, %v", v, err)
	}
	if _, err := arr.SetAll(testCtx, 2, "a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	multi, err := arr.GetMulti(testCtx, 0, 2, 4, 99)
	if err != nil || len(multi) != 4 || multi[0] != "hello" || multi[1] != "a" || multi[2] != "c" || multi[3] != nil {
		t.Fatalf("GetMulti = %#v, %v", multi, err)
	}
	lenN, err := arr.Length(testCtx)
	if err != nil || lenN < 5 {
		t.Fatalf("Length = %d, %v", lenN, err)
	}
	cnt, err := arr.Count(testCtx)
	if err != nil || cnt < 4 {
		t.Fatalf("Count = %d, %v", cnt, err)
	}
	if _, err := arr.DeleteIndexes(testCtx, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := arr.DeleteRange(testCtx, 3, 4); err != nil {
		t.Fatal(err)
	}
}

func TestRArray_InsertSeek(t *testing.T) {
	client := newTestClient(t)
	arr := client.GetArray(uniqueKey(t, "array-ins"))
	t.Cleanup(func() { _ = arr.Clear(testCtx) })

	idx, err := arr.Insert(testCtx, "alpha")
	if skipArray(t, err) {
		return
	}
	if err != nil || idx != 0 {
		t.Fatalf("Insert = %d, %v", idx, err)
	}
	if _, err := arr.Insert(testCtx, "beta", "gamma"); err != nil {
		t.Fatal(err)
	}
	next, err := arr.Next(testCtx)
	if err != nil || next != 3 {
		t.Fatalf("Next = %d, %v", next, err)
	}
	ok, err := arr.Seek(testCtx, 10)
	if err != nil || !ok {
		t.Fatalf("Seek = %v, %v", ok, err)
	}
	idx, err = arr.Insert(testCtx, "tail")
	if err != nil || idx != 10 {
		t.Fatalf("Insert after Seek = %d, %v", idx, err)
	}
}

func TestRPriorityBlockingQueue_Take(t *testing.T) {
	client := newTestClient(t)
	q := client.GetPriorityBlockingQueue(uniqueKey(t, "pbq"))
	t.Cleanup(func() { _ = q.Clear(testCtx) })

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = q.Offer(testCtx, "urgent", 1)
	}()
	v, err := q.Take(testCtx)
	if err != nil || v != "urgent" {
		t.Fatalf("Take = %v, %v", v, err)
	}
	v, err = q.PollWithTimeout(testCtx, 100*time.Millisecond)
	if err != nil || v != nil {
		t.Fatalf("PollWithTimeout empty = %v, %v", v, err)
	}
}

func TestRPriorityBlockingDeque_Ends(t *testing.T) {
	client := newTestClient(t)
	q := client.GetPriorityBlockingDeque(uniqueKey(t, "pbd"))
	t.Cleanup(func() { _ = q.Clear(testCtx) })

	_ = q.Offer(testCtx, "low", 100)
	_ = q.Offer(testCtx, "high", 1)
	v, err := q.Take(testCtx)
	if err != nil || v != "high" {
		t.Fatalf("Take = %v, %v", v, err)
	}
	v, err = q.TakeLast(testCtx)
	if err != nil || v != "low" {
		t.Fatalf("TakeLast = %v, %v", v, err)
	}
}

func TestRPriorityDeque_Ends(t *testing.T) {
	client := newTestClient(t)
	q := client.GetPriorityDeque(uniqueKey(t, "pdq"))
	t.Cleanup(func() { _ = q.Clear(testCtx) })

	_ = q.Offer(testCtx, "a", 10)
	_ = q.Offer(testCtx, "b", 20)
	_ = q.Offer(testCtx, "c", 30)
	first, _ := q.PeekFirst(testCtx)
	last, _ := q.PeekLast(testCtx)
	if first != "a" || last != "c" {
		t.Fatalf("PeekFirst/Last = %v, %v", first, last)
	}
	v, _ := q.PollLast(testCtx)
	if v != "c" {
		t.Fatalf("PollLast = %v", v)
	}
	v, _ = q.PollFirst(testCtx)
	if v != "a" {
		t.Fatalf("PollFirst = %v", v)
	}
}

func TestRClientSideCaching_Facade(t *testing.T) {
	client := newTestClient(t)
	csc := client.GetClientSideCaching()
	if csc.Enabled() {
		t.Fatal("default client should not enable CSC")
	}
	if csc.Owned() {
		t.Fatal("shared facade must not be owned")
	}
	if err := csc.Destroy(); err != nil {
		t.Fatal(err)
	}
	b := csc.GetBucket(uniqueKey(t, "csc-bucket"))
	t.Cleanup(func() { _ = b.Delete(testCtx) })
	if err := b.Set(testCtx, "x"); err != nil {
		t.Fatal(err)
	}
	v, err := b.Get(testCtx)
	if err != nil || v != "x" {
		t.Fatalf("Get = %v, %v", v, err)
	}
}

func TestRClientSideCaching_WithOptionsInvalidate(t *testing.T) {
	parent := newTestClient(t)
	csc, err := parent.GetClientSideCachingWithOptions(&redi.ClientSideCachingOptions{
		MaxEntries:    128,
		DrainInterval: time.Millisecond,
	})
	if err != nil {
		t.Skip("CSC unavailable:", err)
	}
	t.Cleanup(func() { _ = csc.Destroy() })
	if !csc.Enabled() || !csc.Owned() {
		t.Fatalf("Enabled=%v Owned=%v", csc.Enabled(), csc.Owned())
	}

	name := uniqueKey(t, "csc-inv")
	t.Cleanup(func() { interopCleanup(t, name) })

	b := csc.GetBucket(name)
	if v, err := b.Get(testCtx); err != nil || v != nil {
		t.Fatalf("empty Get = %v, %v", v, err)
	}
	if v, err := b.Get(testCtx); err != nil || v != nil {
		t.Fatalf("cached miss Get = %v, %v", v, err)
	}

	mutator := parent.GetBucket(name)
	if err := mutator.Set(testCtx, "123"); err != nil {
		t.Fatal(err)
	}
	if !eventual(t, 3*time.Second, func() bool {
		v, err := b.Get(testCtx)
		return err == nil && v == "123"
	}) {
		t.Fatal("CSC did not observe mutator write after invalidation")
	}
}
