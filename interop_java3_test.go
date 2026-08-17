package redi_test

import (
	"testing"
	"time"
)

func TestJavaInterop_RAtomicDouble(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-adouble")
	t.Cleanup(func() { interopCleanup(t, name) })
	a := client.GetAtomicDouble(name)

	if reply, err := javaSend("adouble_add " + name + " 1.5"); err != nil || !floatEq(reply["value"], 1.5) {
		t.Fatalf("java add = %v, %v", reply, err)
	}
	v, err := a.Get(testCtx)
	if err != nil || v != 1.5 {
		t.Fatalf("Go read = %v, %v; want 1.5", v, err)
	}
	if _, err := a.AddAndGet(testCtx, 0.5); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("adouble_get " + name); err != nil || !floatEq(reply["value"], 2.0) {
		t.Fatalf("java read = %v, %v; want 2", reply, err)
	}
}

func TestJavaInterop_RSpinLock(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-spin")
	t.Cleanup(func() { interopCleanup(t, name) })
	lock := client.GetSpinLock(name)
	holder := client.HolderID("1")

	if reply, err := javaSend("spin_hold " + name); err != nil || reply["acquired"] != true {
		t.Fatalf("java spin hold = %v, %v", reply, err)
	}
	ok, err := lock.TryLock(testCtx, holder, time.Second)
	if err != nil || ok {
		t.Fatalf("Go TryLock while Java holds = %v, %v", ok, err)
	}
	mustJava(t, "spin_release")
	ok, err = lock.TryLock(testCtx, holder, time.Second)
	if err != nil || !ok {
		t.Fatalf("Go TryLock after release = %v, %v", ok, err)
	}
	if reply, err := javaSend("spin_try " + name); err != nil || reply["acquired"] != false {
		t.Fatalf("java try while Go holds = %v, %v", reply, err)
	}
	if err := lock.Unlock(testCtx, holder); err != nil {
		t.Fatal(err)
	}
}

func TestJavaInterop_RNonReentrantLock(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-nrl")
	t.Cleanup(func() { interopCleanup(t, name) })
	lock := client.GetNonReentrantLock(name)
	holder := client.HolderID("1")

	if reply, err := javaSend("nrl_hold " + name); err != nil || reply["acquired"] != true {
		t.Fatalf("java nrl hold = %v, %v", reply, err)
	}
	ok, err := lock.TryLock(testCtx, holder, time.Second)
	if err != nil || ok {
		t.Fatalf("Go TryLock while Java holds = %v, %v", ok, err)
	}
	mustJava(t, "nrl_release")

	ok, err = lock.TryLock(testCtx, holder, time.Second)
	if err != nil || !ok {
		t.Fatalf("Go TryLock = %v, %v", ok, err)
	}
	// Same holder cannot re-enter.
	ok, err = lock.TryLock(testCtx, holder, time.Second)
	if err != nil || ok {
		t.Fatalf("Go reentrant TryLock = %v, %v; want false", ok, err)
	}
	if reply, err := javaSend("nrl_try " + name); err != nil || reply["acquired"] != false {
		t.Fatalf("java try while Go holds = %v, %v", reply, err)
	}
	if err := lock.Unlock(testCtx, holder); err != nil {
		t.Fatal(err)
	}
}

func TestJavaInterop_RBoundedBlockingQueue(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-bbq")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	q := client.GetBoundedBlockingQueue(name)

	if reply, err := javaSend("bbq_capacity " + name + " 2"); err != nil || reply["ok"] != true {
		t.Fatalf("java capacity = %v, %v", reply, err)
	}
	mustJava(t, "bbq_offer", name, `"one"`)
	v, err := q.Poll(testCtx)
	if err != nil || v != "one" {
		t.Fatalf("Go Poll = %v, %v", v, err)
	}
	if ok, err := q.Offer(testCtx, "two"); err != nil || !ok {
		t.Fatalf("Go Offer = %v, %v", ok, err)
	}
	if reply, err := javaSend("bbq_poll " + name); err != nil || reply["value"] != "two" {
		t.Fatalf("java poll = %v, %v", reply, err)
	}
	if reply, err := javaSend("bbq_size " + name); err != nil || !numEq(reply["size"], 0) {
		t.Fatalf("java size = %v, %v", reply, err)
	}
}
