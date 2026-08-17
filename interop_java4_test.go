package redi_test

import (
	"testing"
	"time"
)

func TestJavaInterop_RSetMultimapCacheNative(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-smmcn")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	m := client.GetSetMultimapCacheNative(name)

	if reply, err := javaSend("smmcn_put " + name + ` "k" "jv"`); err != nil {
		if skipNativeTTL(t, err) {
			return
		}
		t.Fatalf("java put = %v, %v", reply, err)
	}
	if reply, err := javaSend("smmcn_expire " + name + ` "k" 60000`); err != nil || reply["ok"] != true {
		if err != nil && skipNativeTTL(t, err) {
			return
		}
		t.Fatalf("java expire = %v, %v", reply, err)
	}
	vals, err := m.Get(testCtx, "k")
	if err != nil || len(vals) != 1 || vals[0] != "jv" {
		t.Fatalf("Go Get = %v, %v", vals, err)
	}
	if _, err := m.Put(testCtx, "k", "gv"); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.ExpireKey(testCtx, "k", time.Minute); err != nil || !ok {
		if err != nil && skipNativeTTL(t, err) {
			return
		}
		t.Fatalf("Go ExpireKey = %v, %v", ok, err)
	}
	reply, err := javaSend("smmcn_getall " + name + ` "k"`)
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := reply["values"].([]any)
	if len(arr) < 2 {
		t.Fatalf("java getall = %v; want both jv and gv", reply)
	}
}

func TestJavaInterop_RListMultimapCacheNative(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-lmmcn")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	m := client.GetListMultimapCacheNative(name)

	if _, err := javaSend("lmmcn_put " + name + ` "k" "a"`); err != nil {
		if skipNativeTTL(t, err) {
			return
		}
		t.Fatal(err)
	}
	if reply, err := javaSend("lmmcn_expire " + name + ` "k" 60000`); err != nil || reply["ok"] != true {
		if err != nil && skipNativeTTL(t, err) {
			return
		}
		t.Fatalf("java expire = %v, %v", reply, err)
	}
	if _, err := m.Put(testCtx, "k", "b"); err != nil {
		t.Fatal(err)
	}
	reply, err := javaSend("lmmcn_getall " + name + ` "k"`)
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := reply["values"].([]any)
	if len(arr) != 2 || arr[0] != "a" || arr[1] != "b" {
		t.Fatalf("java getall order = %v; want [a b]", arr)
	}
}

func TestJavaInterop_RRingBuffer(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-rb")
	capKey := "redisson_rb:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, capKey) })
	rb := client.GetRingBuffer(name)

	if reply, err := javaSend("rb_capacity " + name + " 2"); err != nil || reply["ok"] != true {
		t.Fatalf("java capacity = %v, %v", reply, err)
	}
	mustJava(t, "rb_add", name, `"one"`)
	v, err := rb.Poll(testCtx)
	if err != nil || v != "one" {
		t.Fatalf("Go Poll = %v, %v", v, err)
	}
	if err := rb.Add(testCtx, "two"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("rb_poll " + name); err != nil || reply["value"] != "two" {
		t.Fatalf("java poll = %v, %v", reply, err)
	}
	if reply, err := javaSend("rb_size " + name); err != nil || !numEq(reply["size"], 0) {
		t.Fatalf("java size = %v, %v", reply, err)
	}
}
