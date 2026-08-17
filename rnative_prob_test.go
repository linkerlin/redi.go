package redi_test

import (
	"strings"
	"testing"
	"time"
)

func TestMapCacheNative_PutTTL(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "mcn")
	t.Cleanup(func() { interopCleanup(t, name) })

	m := client.GetMapCacheNative(name)
	prev, err := m.PutWithTTL(testCtx, "a", "v1", time.Minute)
	if err != nil {
		if skipNativeTTL(t, err) {
			return
		}
		t.Fatal(err)
	}
	if prev != nil {
		t.Fatalf("prev = %v", prev)
	}
	got, err := m.Get(testCtx, "a")
	if err != nil || got != "v1" {
		t.Fatalf("Get = %v, %v", got, err)
	}
	ttl, err := m.RemainTTLForKey(testCtx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("RemainTTLForKey = %v", ttl)
	}
	ok, err := m.FastPutIfAbsentWithTTL(testCtx, "a", "x", time.Minute)
	if err != nil || ok {
		t.Fatalf("FastPutIfAbsentWithTTL existing = %v, %v", ok, err)
	}
}

func TestMultimapCacheNative_ExpireKey(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "mmcn")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })

	m := client.GetSetMultimapCacheNative(name)
	if _, err := m.Put(testCtx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	ok, err := m.ExpireKey(testCtx, "k", time.Minute)
	if err != nil {
		if skipNativeTTL(t, err) {
			return
		}
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ExpireKey = false")
	}
}

func TestBloomFilterNative_Basic(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "bfn")
	t.Cleanup(func() { interopCleanup(t, name) })

	f := client.GetBloomFilterNative(name)
	ok, err := f.TryInit(testCtx, 0.01, 100)
	if err != nil {
		if skipProbabilistic(t, err) {
			return
		}
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("TryInit")
	}
	added, err := f.Add(testCtx, "x")
	if err != nil || !added {
		t.Fatalf("Add = %v, %v", added, err)
	}
	has, err := f.Contains(testCtx, "x")
	if err != nil || !has {
		t.Fatalf("Contains = %v, %v", has, err)
	}
}

func TestCuckooTopKTdigest_Smoke(t *testing.T) {
	client := newTestClient(t)

	cfName := uniqueKey(t, "cf")
	t.Cleanup(func() { interopCleanup(t, cfName) })
	cf := client.GetCuckooFilter(cfName)
	if _, err := cf.TryInit(testCtx, 100); err != nil {
		if skipProbabilistic(t, err) {
			return
		}
		t.Fatal(err)
	}
	if _, err := cf.Add(testCtx, "a"); err != nil {
		t.Fatal(err)
	}

	tkName := uniqueKey(t, "topk")
	t.Cleanup(func() { interopCleanup(t, tkName) })
	tk := client.GetTopK(tkName)
	if _, err := tk.TryInit(testCtx, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := tk.Add(testCtx, "a", "b", "a"); err != nil {
		t.Fatal(err)
	}

	tdName := uniqueKey(t, "td")
	t.Cleanup(func() { interopCleanup(t, tdName) })
	td := client.GetTDigest(tdName)
	if _, err := td.TryCreate(testCtx); err != nil {
		t.Fatal(err)
	}
	if err := td.Add(testCtx, 1, 2, 3, 4, 5); err != nil {
		t.Fatal(err)
	}
	qs, err := td.Quantile(testCtx, 0.5)
	if err != nil || len(qs) != 1 {
		t.Fatalf("Quantile = %v, %v", qs, err)
	}
}

func TestGcra_TryAcquire(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "gcra")
	t.Cleanup(func() { interopCleanup(t, name) })

	g := client.GetGcra(name)
	res, err := g.TryAcquire(testCtx, 2, 2, time.Second, 1)
	if err != nil {
		if skipProbabilistic(t, err) {
			return
		}
		t.Fatal(err)
	}
	if res == nil || !res.Allowed {
		t.Fatalf("first acquire = %#v", res)
	}
}

func skipNativeTTL(t *testing.T, err error) bool {
	t.Helper()
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "unknown command") ||
		strings.Contains(s, "hpexpire") ||
		strings.Contains(s, "hpttl") {
		t.Skip("Redis hash-field TTL unavailable:", err)
		return true
	}
	return false
}

func skipProbabilistic(t *testing.T, err error) bool {
	t.Helper()
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "unknown command") ||
		strings.Contains(s, "bf.") ||
		strings.Contains(s, "cf.") ||
		strings.Contains(s, "topk") ||
		strings.Contains(s, "tdigest") ||
		strings.Contains(s, "gcra") {
		t.Skip("probabilistic/GCRA commands unavailable:", err)
		return true
	}
	return false
}
