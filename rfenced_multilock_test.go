package redi_test

import (
	"testing"
	"time"
)

func TestRFencedLock(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "fenced")
	l := client.GetFencedLock(name)
	t.Cleanup(func() { interopCleanup(t, name, "redisson_lock_token:{"+name+"}") })

	// Every acquisition - including re-entries - increments the token.
	t1, err := l.TryLockAndGetToken(testCtx, "svc:1", time.Minute)
	if err != nil || t1 != 1 {
		t.Fatalf("first token = %d, %v; want 1", t1, err)
	}
	t2, err := l.TryLockAndGetToken(testCtx, "svc:1", time.Minute)
	if err != nil || t2 != 2 {
		t.Fatalf("re-entry token = %d, %v; want 2 (re-entries also increment)", t2, err)
	}
	cur, _ := l.GetToken(testCtx)
	if cur != 2 {
		t.Fatalf("GetToken = %d, want 2", cur)
	}

	// A different holder is blocked; the token does not move.
	if tok, _ := l.TryLockAndGetToken(testCtx, "svc:2", time.Minute); tok != 0 {
		t.Fatalf("contended token = %d, want 0", tok)
	}
	cur, _ = l.GetToken(testCtx)
	if cur != 2 {
		t.Fatalf("token moved on contention: %d", cur)
	}

	// Full release, then the next holder gets a higher token.
	_ = l.Unlock(testCtx, "svc:1")
	_ = l.Unlock(testCtx, "svc:1")
	t3, err := l.TryLockAndGetToken(testCtx, "svc:2", time.Minute)
	if err != nil || t3 != 3 {
		t.Fatalf("next holder token = %d, %v; want 3 (monotonic)", t3, err)
	}
	_ = l.Unlock(testCtx, "svc:2")
}

func TestRMultiLock(t *testing.T) {
	client := newTestClient(t)
	n1 := uniqueKey(t, "ml-1")
	n2 := uniqueKey(t, "ml-2")
	t.Cleanup(func() { interopCleanup(t, n1, n2) })

	// All-or-nothing: block one member, the whole TryLock fails and the
	// already-acquired member is rolled back.
	locked := client.GetLock(n2)
	if err := locked.Lock(testCtx, "other:1", time.Minute); err != nil {
		t.Fatal(err)
	}
	ml := client.NewMultiLock(client.GetLock(n1), client.GetLock(n2))
	ok, err := ml.TryLock(testCtx, "me:1", time.Minute)
	if err != nil || ok {
		t.Fatalf("TryLock with blocked member = %v, %v; want false", ok, err)
	}
	// Member 1 must have been rolled back.
	held, _ := client.GetLock(n1).IsHeldBy(testCtx, "me:1")
	if held {
		t.Fatal("acquired member not rolled back")
	}
	_ = locked.Unlock(testCtx, "other:1")

	// With both members free the multilock succeeds and unlocks fully.
	if err := ml.Lock(testCtx, "me:1", time.Minute, 5*time.Second); err != nil {
		t.Fatal("Lock:", err)
	}
	for _, n := range []string{n1, n2} {
		held, _ := client.GetLock(n).IsHeldBy(testCtx, "me:1")
		if !held {
			t.Fatalf("member %s not held after multilock Lock", n)
		}
	}
	if err := ml.Unlock(testCtx, "me:1"); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{n1, n2} {
		held, _ := client.GetLock(n).IsHeldBy(testCtx, "me:1")
		if held {
			t.Fatalf("member %s still held after multilock Unlock", n)
		}
	}
}

// TestWire_FencedLockTokenKey locks the companion key name and format.
func TestWire_FencedLockTokenKey(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-fenced")
	l := client.GetFencedLock(name)
	t.Cleanup(func() { interopCleanup(t, name, "redisson_lock_token:{"+name+"}") })
	rc := rawClient(t)

	if _, err := l.TryLockAndGetToken(testCtx, "w:1", time.Minute); err != nil {
		t.Fatal(err)
	}
	v, err := rc.Get(testCtx, "redisson_lock_token:{"+name+"}").Result()
	if err != nil {
		t.Fatalf("token key missing: %v", err)
	}
	if v != "1" {
		t.Fatalf("token value = %q, want plain decimal 1 (StringCodec)", v)
	}
}

// TestJavaInterop_RFencedLock: shared fencing token across Go and Redisson.
func TestJavaInterop_RFencedLock(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-fenced")
	t.Cleanup(func() { interopCleanup(t, name, "redisson_lock_token:{"+name+"}") })
	l := client.GetFencedLock(name)
	holder := client.HolderID("1")

	tok, err := l.TryLockAndGetToken(testCtx, holder, time.Minute)
	if err != nil || tok != 1 {
		t.Fatalf("Go first token = %d, %v; want 1", tok, err)
	}
	if reply, err := javaSend("fenced_try " + name); err != nil {
		t.Fatal(err)
	} else if reply["token"] != nil {
		t.Fatalf("Java acquired while Go holds: %v", reply)
	}
	if reply, err := javaSend("fenced_token " + name); err != nil || !numEq(reply["token"], 1) {
		t.Fatalf("Java getToken = %v, %v; want 1", reply, err)
	}
	if err := l.Unlock(testCtx, holder); err != nil {
		t.Fatal(err)
	}

	if reply, err := javaSend("fenced_try " + name); err != nil {
		t.Fatal(err)
	} else if !numEq(reply["token"], 2) {
		t.Fatalf("Java token after Go release = %v; want 2", reply)
	}
	if tok, _ := l.TryLockAndGetToken(testCtx, holder, time.Minute); tok != 0 {
		t.Fatalf("Go acquired while Java holds: %d", tok)
	}
	if cur, _ := l.GetToken(testCtx); cur != 2 {
		t.Fatalf("Go GetToken = %d, want 2", cur)
	}
	mustJava(t, "fenced_release")
}
