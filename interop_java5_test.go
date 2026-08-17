package redi_test

import (
	"testing"
	"time"
)

func TestJavaInterop_RSetCache(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-sc")
	idleKey := "redisson__idle__set:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, idleKey) })
	sc := client.GetSetCache(name)

	if reply, err := javaSend("sc_add " + name + ` "jv" 60000`); err != nil || reply["added"] != true {
		t.Fatalf("java sc_add = %v, %v", reply, err)
	}
	ok, err := sc.Contains(testCtx, "jv")
	if err != nil || !ok {
		t.Fatalf("Go Contains = %v, %v", ok, err)
	}
	added, err := sc.Add(testCtx, "gv", time.Minute, 0)
	if err != nil || !added {
		t.Fatalf("Go Add = %v, %v", added, err)
	}
	if reply, err := javaSend("sc_contains " + name + ` "gv"`); err != nil || reply["contains"] != true {
		t.Fatalf("java contains = %v, %v", reply, err)
	}
	if reply, err := javaSend("sc_size " + name); err != nil || !numEq(reply["size"], 2) {
		t.Fatalf("java size = %v, %v", reply, err)
	}
}

func TestJavaInterop_RSetMultimapCache(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-smmc")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	m := client.GetSetMultimapCache(name)

	if _, err := javaSend("smmc_put " + name + ` "k" "jv"`); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("smmc_expire " + name + ` "k" 60000`); err != nil || reply["ok"] != true {
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
		t.Fatalf("Go ExpireKey = %v, %v", ok, err)
	}
	reply, err := javaSend("smmc_getall " + name + ` "k"`)
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := reply["values"].([]any)
	if len(arr) < 2 {
		t.Fatalf("java getall = %v; want jv+gv", reply)
	}
}

func TestJavaInterop_RListMultimapCache(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-lmmc")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	m := client.GetListMultimapCache(name)

	if _, err := javaSend("lmmc_put " + name + ` "k" "a"`); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("lmmc_expire " + name + ` "k" 60000`); err != nil || reply["ok"] != true {
		t.Fatalf("java expire = %v, %v", reply, err)
	}
	if _, err := m.Put(testCtx, "k", "b"); err != nil {
		t.Fatal(err)
	}
	reply, err := javaSend("lmmc_getall " + name + ` "k"`)
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := reply["values"].([]any)
	if len(arr) != 2 || arr[0] != "a" || arr[1] != "b" {
		t.Fatalf("java getall order = %v; want [a b]", arr)
	}
}
