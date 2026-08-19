package redi_test

import (
	"context"
	"testing"
	"time"
)

func TestRPermitExpirableSemaphore(t *testing.T) {
	client := newTestClient(t)
	s := client.GetPermitExpirableSemaphore(uniqueKey(t, "pes"))
	defer s.Delete(testCtx) //nolint:errcheck

	ok, err := s.TrySetPermits(testCtx, 2)
	if err != nil || !ok {
		t.Fatalf("TrySetPermits = %v, %v", ok, err)
	}
	ok, _ = s.TrySetPermits(testCtx, 5) // already initialized
	if ok {
		t.Fatal("re-init should return false")
	}

	pid1, err := s.TryAcquire(testCtx, time.Minute)
	if err != nil || pid1 == "" {
		t.Fatalf("TryAcquire 1 = %q, %v", pid1, err)
	}
	pid2, _ := s.TryAcquire(testCtx, time.Minute)
	if pid2 == "" || pid1 == pid2 {
		t.Fatalf("TryAcquire 2 = %q (pid1 %q)", pid2, pid1)
	}
	pid3, _ := s.TryAcquire(testCtx, time.Minute)
	if pid3 != "" {
		t.Fatal("third acquire should fail (0 free)")
	}

	n, _ := s.AvailablePermits(testCtx)
	if n != 0 {
		t.Fatalf("Available = %d", n)
	}
	n, _ = s.AcquiredPermits(testCtx)
	if n != 2 {
		t.Fatalf("Acquired = %d", n)
	}

	// Unknown permit cannot release.
	ok, _ = s.Release(testCtx, "bogus-permit")
	if ok {
		t.Fatal("bogus permit released")
	}

	ok, err = s.Release(testCtx, pid1)
	if err != nil || !ok {
		t.Fatalf("Release(pid1) = %v, %v", ok, err)
	}
	n, _ = s.AvailablePermits(testCtx)
	if n != 1 {
		t.Fatalf("Available after release = %d", n)
	}

	// Lease extension + expiry.
	rem, _ := s.LeaseTime(testCtx, pid2)
	if rem <= 0 || rem > time.Minute {
		t.Fatalf("LeaseTime = %v", rem)
	}
	if ok, _ := s.UpdateLeaseTime(testCtx, pid2, 250*time.Millisecond); !ok {
		t.Fatal("UpdateLeaseTime = false")
	}
	if !eventual(t, 3*time.Second, func() bool {
		n, _ := s.AcquiredPermits(testCtx)
		return n == 0
	}) {
		t.Fatal("expired permit was not reclaimed")
	}
	// After expiry purge, a new acquire succeeds (reclaim-on-acquire).
	pid4, _ := s.TryAcquire(testCtx, time.Minute)
	if pid4 == "" {
		t.Fatal("acquire after expiry purge failed")
	}
	_ = pid4
}

func TestRPermitExpirableSemaphore_TrySetPermitsPublishes(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "pes-setpub")
	s := client.GetPermitExpirableSemaphore(name)
	defer s.Delete(testCtx) //nolint:errcheck

	channel := "redisson_sc:{" + name + "}"
	sub := client.Redis().Subscribe(testCtx, channel)
	defer sub.Close() //nolint:errcheck
	if _, err := sub.Receive(testCtx); err != nil {
		t.Fatal("subscribe ack:", err)
	}
	msgs := sub.Channel()

	ok, err := s.TrySetPermits(testCtx, 3)
	if err != nil || !ok {
		t.Fatalf("TrySetPermits = %v, %v", ok, err)
	}
	select {
	case msg := <-msgs:
		if msg.Payload != "3" {
			t.Fatalf("publish payload = %q, want 3", msg.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("TrySetPermits did not PUBLISH to redisson_sc")
	}

	ok, err = s.TrySetPermits(testCtx, 9)
	if err != nil || ok {
		t.Fatalf("second TrySetPermits = %v, %v; want false", ok, err)
	}
	select {
	case msg := <-msgs:
		t.Fatalf("second TrySetPermits published %q", msg.Payload)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestRPermitExpirableSemaphore_Blocking(t *testing.T) {
	client := newTestClient(t)
	s := client.GetPermitExpirableSemaphore(uniqueKey(t, "pes-block"))
	defer s.Delete(testCtx) //nolint:errcheck

	if _, err := s.TrySetPermits(testCtx, 1); err != nil {
		t.Fatal(err)
	}
	pid, err := s.TryAcquire(testCtx, time.Minute)
	if err != nil || pid == "" {
		t.Fatal(err)
	}

	acquired := make(chan string, 1)
	go func() {
		p, _ := s.Acquire(context.Background(), time.Minute)
		acquired <- p
	}()

	time.Sleep(150 * time.Millisecond)
	select {
	case <-acquired:
		t.Fatal("Acquire returned with 0 permits")
	default:
	}
	if _, err := s.Release(testCtx, pid); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-acquired:
		if p == "" {
			t.Fatal("Acquire returned empty permit")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire not woken by Release")
	}
}

func TestRReliableTopic(t *testing.T) {
	client := newTestClient(t)
	topic := client.GetReliableTopic(uniqueKey(t, "rtopic"))
	defer topic.Delete(testCtx) //nolint:errcheck

	got1 := make(chan any, 4)
	got2 := make(chan any, 4)
	id1, err := topic.Subscribe(func(msg any) { got1 <- msg })
	if err != nil {
		t.Fatal("Subscribe 1:", err)
	}
	id2, err := topic.Subscribe(func(msg any) { got2 <- msg })
	if err != nil {
		t.Fatal("Subscribe 2:", err)
	}

	// Broadcast: EVERY subscriber receives every message (independent groups).
	if _, err := topic.Publish(testCtx, "first"); err != nil {
		t.Fatal("Publish:", err)
	}
	if _, err := topic.Publish(testCtx, "second"); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []chan any{got1, got2} {
		for i := 0; i < 2; i++ {
			select {
			case <-ch:
			case <-time.After(3 * time.Second):
				t.Fatal("listener did not receive both messages (broadcast broken)")
			}
		}
	}

	n, _ := topic.CountSubscribers(testCtx)
	if n != 2 {
		t.Fatalf("CountSubscribers = %d, want 2", n)
	}

	// Messages published BEFORE subscribing are still delivered (Java's
	// StreamMessageId.ALL start).
	topic.Unsubscribe(id2)
	got3 := make(chan any, 1)
	id3, err := topic.Subscribe(func(msg any) { got3 <- msg })
	if err != nil {
		t.Fatal("Subscribe 3:", err)
	}
	select {
	case v := <-got3:
		if v != "first" {
			t.Fatalf("late subscriber first message = %v, want \"first\" (history replay)", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("late subscriber received nothing (history replay broken)")
	}
	// Drain the replayed second message too.
	select {
	case <-got3:
	case <-time.After(3 * time.Second):
	}
	topic.Unsubscribe(id1)
	topic.Unsubscribe(id3)

	n, _ = topic.CountSubscribers(testCtx)
	if n != 0 {
		t.Fatalf("CountSubscribers after unsubscribe = %d", n)
	}
}

// TestJavaInterop_PES_RReliableTopic: cross-language permit leasing and
// reliable topic delivery.
func TestJavaInterop_PES_RReliableTopic(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)

	// --- PermitExpirableSemaphore: shared counter across languages. ---
	pesName := uniqueKey(t, "jio-pes")
	t.Cleanup(func() {
		interopCleanup(t, pesName, "{"+pesName+"}:timeout")
	})
	s := client.GetPermitExpirableSemaphore(pesName)

	if reply, err := javaSend("pes_set " + pesName + " 1"); err != nil || reply["ok"] != true {
		t.Fatalf("java pes set = %v, %v", reply, err)
	}
	// Java leases the only permit; Go must see none available.
	if reply, err := javaSend("pes_acquire " + pesName); err != nil {
		t.Fatal(err)
	} else if reply["permit"] == nil {
		t.Fatalf("java acquire failed: %v", reply)
	}
	n, err := s.AvailablePermits(testCtx)
	if err != nil || n != 0 {
		t.Fatalf("Go available after java lease = %d, %v; want 0", n, err)
	}
	if pid, _ := s.TryAcquire(testCtx, time.Minute); pid != "" {
		t.Fatal("Go TryAcquire succeeded while java holds the only permit")
	}
	// Java releases; Go acquires through the shared pool.
	mustJava(t, "pes_release", pesName)
	pid, err := s.TryAcquire(testCtx, time.Minute)
	if err != nil || pid == "" {
		t.Fatalf("Go acquire after java release = %q, %v", pid, err)
	}
	if reply, err := javaSend("pes_available " + pesName); err != nil || !numEq(reply["value"], 0) {
		t.Fatalf("java available after Go lease = %v, %v", reply, err)
	}

	// --- ReliableTopic: Java listener receives Go publish. ---
	rtName := uniqueKey(t, "jio-rtopic")
	t.Cleanup(func() {
		interopCleanup(t, rtName, "{"+rtName+"}:timeout")
	})
	mustJava(t, "rtopic_listen", rtName)
	// Go also subscribes; both languages' groups must each get the message.
	got := make(chan any, 1)
	if _, err := client.GetReliableTopic(rtName).Subscribe(func(msg any) { got <- msg }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // let subscriptions settle

	if reply, err := javaSend("rtopic_publish " + rtName + ` "ping-from-go"`); err != nil {
		t.Fatal(err)
	} else if subs, ok := reply["subscribers"].(float64); ok && subs < 2 {
		t.Fatalf("publish saw %v subscriber groups, want >= 2", reply["subscribers"])
	}
	select {
	case v := <-got:
		if v != "ping-from-go" {
			t.Fatalf("Go listener got %v", v)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Go listener not notified")
	}
	if reply, err := javaSend("rtopic_collect"); err != nil || reply["value"] != "ping-from-go" {
		t.Fatalf("java listener = %v, %v; want ping-from-go", reply, err)
	}
}
