package redi_test

import (
	"context"
	"testing"
	"time"
)

func TestRSetCache(t *testing.T) {
	client := newTestClient(t)
	s := client.GetSetCache(uniqueKey(t, "setcache"))
	defer s.Clear(testCtx) //nolint:errcheck

	ok, err := s.Add(testCtx, "ephemeral", 300*time.Millisecond, 0)
	if err != nil || !ok {
		t.Fatalf("Add = %v, %v", ok, err)
	}
	ok, _ = s.Add(testCtx, "stable", 0, 0)
	if !ok {
		t.Fatal("second Add should be new")
	}
	ok, _ = s.Add(testCtx, "stable", 0, 0)
	if ok {
		t.Fatal("re-Add of existing element should report false")
	}

	sz, _ := s.Size(testCtx)
	if sz != 2 {
		t.Fatalf("Size = %d, want 2", sz)
	}

	contains, _ := s.Contains(testCtx, "ephemeral")
	if !contains {
		t.Fatal("Contains before expiry = false")
	}

	if !eventual(t, 2*time.Second, func() bool {
		contains, _ := s.Contains(testCtx, "ephemeral")
		sz, _ = s.Size(testCtx)
		return !contains && sz == 1
	}) {
		t.Fatalf("element did not expire (contains=%v size=%d)", contains, sz)
	}

	members, _ := s.Members(testCtx)
	if len(members) != 1 || members[0] != "stable" {
		t.Fatalf("Members = %v", members)
	}
}

func TestRSetCache_MaxIdleRefresh(t *testing.T) {
	client := newTestClient(t)
	s := client.GetSetCache(uniqueKey(t, "setcache-idle"))
	defer s.Clear(testCtx) //nolint:errcheck

	if _, err := s.Add(testCtx, "hot", 0, 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// Keep touching the element: each Contains refreshes the idle deadline.
	for i := 0; i < 6; i++ {
		time.Sleep(200 * time.Millisecond)
		contains, err := s.Contains(testCtx, "hot")
		if err != nil {
			t.Fatal(err)
		}
		if !contains {
			t.Fatalf("maxIdle element expired despite access at iteration %d", i)
		}
	}
	// Stop touching: it must expire within the idle window. (Check via
	// Size, which evicts without refreshing the idle deadline.)
	if !eventual(t, 3*time.Second, func() bool {
		sz, _ := s.Size(testCtx)
		return sz == 0
	}) {
		t.Fatal("idle element did not expire after access stopped")
	}
}

func TestRBlockingDeque(t *testing.T) {
	client := newTestClient(t)
	d := client.GetBlockingDeque(uniqueKey(t, "bdeque"))
	defer d.Clear(testCtx) //nolint:errcheck

	// Phase 1: exactly one blocked client (TakeFirst); the next push is
	// served to it regardless of push side.
	first := make(chan any, 1)
	go func() {
		v, _ := d.TakeFirst(context.Background())
		first <- v
	}()
	time.Sleep(150 * time.Millisecond)
	if err := d.AddLast(testCtx, "e1"); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-first:
		if v != "e1" {
			t.Fatalf("TakeFirst = %v", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TakeFirst not woken")
	}

	// Phase 2: exactly one blocked client (TakeLast).
	last := make(chan any, 1)
	go func() {
		v, _ := d.TakeLast(context.Background())
		last <- v
	}()
	time.Sleep(150 * time.Millisecond)
	if err := d.AddFirst(testCtx, "e2"); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-last:
		if v != "e2" {
			t.Fatalf("TakeLast = %v", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TakeLast not woken")
	}

	// Timeout path.
	if err := d.AddFirst(testCtx, "x"); err != nil {
		t.Fatal(err)
	}
	v, err := d.PollLastWithTimeout(testCtx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if v != "x" {
		t.Fatalf("PollLastWithTimeout = %v", v)
	}
	v, err = d.PollFirstWithTimeout(testCtx, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Fatalf("PollFirstWithTimeout on empty = %v", v)
	}
}

func TestRMultimap_Set(t *testing.T) {
	client := newTestClient(t)
	m := client.GetSetMultimap(uniqueKey(t, "setmm"))
	defer m.Clear(testCtx) //nolint:errcheck

	ok, err := m.Put(testCtx, "k1", "v1")
	if err != nil || !ok {
		t.Fatalf("Put new = %v, %v", ok, err)
	}
	ok, _ = m.Put(testCtx, "k1", "v1") // duplicate in a set
	if ok {
		t.Fatal("duplicate Put should report false for set multimap")
	}
	if _, err := m.Put(testCtx, "k1", "v2"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Put(testCtx, "k2", "w1"); err != nil {
		t.Fatal(err)
	}

	n, _ := m.Size(testCtx)
	if n != 3 {
		t.Fatalf("Size = %d, want 3", n)
	}

	contains, _ := m.ContainsKey(testCtx, "k1")
	if !contains {
		t.Fatal("ContainsKey(k1) = false")
	}
	contains, _ = m.ContainsEntry(testCtx, "k1", "v2")
	if !contains {
		t.Fatal("ContainsEntry(k1,v2) = false")
	}
	contains, _ = m.ContainsEntry(testCtx, "k1", "nope")
	if contains {
		t.Fatal("ContainsEntry(k1,nope) = true")
	}

	got, _ := m.Get(testCtx, "k1")
	if len(got) != 2 {
		t.Fatalf("Get(k1) = %v", got)
	}

	removed, err := m.RemoveEntry(testCtx, "k1", "v1")
	if err != nil || !removed {
		t.Fatalf("RemoveEntry = %v, %v", removed, err)
	}
	n, _ = m.Size(testCtx)
	if n != 2 {
		t.Fatalf("Size after RemoveEntry = %d, want 2", n)
	}

	if ok, _ := m.RemoveAll(testCtx, "k2"); !ok {
		t.Fatal("RemoveAll(k2) = false")
	}
	contains, _ = m.ContainsKey(testCtx, "k2")
	if contains {
		t.Fatal("k2 still present after RemoveAll")
	}
}

func TestRMultimap_List(t *testing.T) {
	client := newTestClient(t)
	m := client.GetListMultimap(uniqueKey(t, "listmm"))
	defer m.Clear(testCtx) //nolint:errcheck

	for _, v := range []string{"a", "b", "a"} {
		if _, err := m.Put(testCtx, "k", v); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := m.Get(testCtx, "k")
	if len(got) != 3 || got[0] != "a" || got[2] != "a" {
		t.Fatalf("list multimap keeps duplicates+order, got %v", got)
	}
}
