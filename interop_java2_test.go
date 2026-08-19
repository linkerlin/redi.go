package redi_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

// interop_java2_test.go: the second wave of direct Go <-> Redisson 4.6.1
// interop cases (RWLock, List, ScoredSortedSet, LexSortedSet, Bucket,
// MapCache, DelayedQueue). Same JVM REPL probe as wave 1.

func TestJavaInterop_RReadWriteLock(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-rw")
	t.Cleanup(func() { interopCleanup(t, name) })
	rw := client.GetReadWriteLock(name)

	// Java holds write; Go read AND write must both be blocked.
	if reply, err := javaSend("rw_write_hold " + name); err != nil || reply["acquired"] != true {
		t.Fatalf("java rw hold = %v, %v", reply, err)
	}
	if ok, _ := rw.ReadLock().TryLock(testCtx, "go:1", time.Minute); ok {
		t.Fatal("Go read lock acquired while Redisson holds write")
	}
	if ok, _ := rw.WriteLock().TryLock(testCtx, "go:1", time.Minute); ok {
		t.Fatal("Go write lock acquired while Redisson holds write")
	}
	mustJava(t, "rw_release")

	// Go holds write; Java read AND write must both be blocked.
	if err := rw.WriteLock().Lock(testCtx, "go:1", time.Minute); err != nil {
		t.Fatal("Go write Lock after release:", err)
	}
	if reply, _ := javaSend("rw_read_try " + name); reply["acquired"] == true {
		t.Fatal("Redisson read lock acquired while Go holds write")
	}
	if reply, _ := javaSend("rw_write_try " + name); reply["acquired"] == true {
		t.Fatal("Redisson write lock acquired while Go holds write")
	}
	if err := rw.WriteLock().Unlock(testCtx, "go:1"); err != nil {
		t.Fatal(err)
	}

	// Shared mode: Java read + Go read coexist.
	if reply, err := javaSend("rw_read_try " + name); err != nil || reply["acquired"] != true {
		t.Fatalf("java read = %v, %v", reply, err)
	}
	if err := rw.ReadLock().Lock(testCtx, "go:1", time.Minute); err != nil {
		t.Fatal("Go read Lock should coexist with java reader:", err)
	}
	// ...but a writer is still excluded.
	if ok, _ := rw.WriteLock().TryLock(testCtx, "go:2", time.Minute); ok {
		t.Fatal("Go write lock acquired while two readers hold the lock")
	}
	_ = rw.ReadLock().Unlock(testCtx, "go:1")
}

func TestJavaInterop_RFairLock(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-fair")
	queueKey := "redisson_lock_queue:{" + name + "}"
	timeoutKey := "redisson_lock_timeout:{" + name + "}"
	t.Cleanup(func() {
		_, _ = javaSend("fair_unlock")
		interopCleanup(t, name, queueKey, timeoutKey)
	})
	lock := client.GetFairLock(name)

	if err := lock.Lock(testCtx, "go:fair", time.Minute); err != nil {
		t.Fatal("Go fair Lock:", err)
	}
	if reply, err := javaSend("fair_try " + name); err != nil || reply["acquired"] != false {
		t.Fatalf("java fair_try while Go holds = %v, %v", reply, err)
	}
	if err := lock.Unlock(testCtx, "go:fair"); err != nil {
		t.Fatal("Go fair Unlock:", err)
	}

	if reply, err := javaSend("fair_hold " + name); err != nil || reply["acquired"] != true {
		t.Fatalf("java fair_hold = %v, %v", reply, err)
	}
	if reply, err := javaSend("fair_held " + name); err != nil || reply["held"] != true {
		t.Fatalf("java fair_held = %v, %v", reply, err)
	}
	if acquired, err := lock.TryLock(testCtx, "go:fair", time.Minute); err != nil || acquired {
		t.Fatalf("Go TryLock while Java holds = %v, %v", acquired, err)
	}
	if reply, err := javaSend("fair_unlock"); err != nil || reply["ok"] != true {
		t.Fatalf("java fair_unlock = %v, %v", reply, err)
	}
}

func TestJavaInterop_RList(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-list")
	t.Cleanup(func() { interopCleanup(t, name) })
	l := client.GetList(name)

	// Java appends; Go reads by index.
	mustJava(t, "list_add", name, `"alpha"`)
	mustJava(t, "list_add", name, `"beta"`)
	v, err := l.Get(testCtx, 1)
	if err != nil || v != "beta" {
		t.Fatalf("Go Get(1) after java adds = %v, %v", v, err)
	}

	// Go appends (typed values); Java reads.
	if err := l.Add(testCtx, 42); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("list_get " + name + " 2"); err != nil || !numEq(reply["value"], 42) {
		t.Fatalf("java get(2) of Go int = %v, %v", reply, err)
	}
	if reply, err := javaSend("list_size " + name); err != nil || !numEq(reply["size"], 3) {
		t.Fatalf("java size = %v, %v", reply, err)
	}
}

func TestJavaInterop_RScoredSortedSet(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-zset")
	t.Cleanup(func() { interopCleanup(t, name) })
	z := client.GetScoredSortedSet(name)

	// Java adds with score; Go reads score + rank.
	if reply, err := javaSend("zset_add " + name + " 90.5 \"alice\""); err != nil || reply["ok"] != true {
		t.Fatalf("java zadd alice = %v, %v", reply, err)
	}
	mustJava(t, "zset_add", name, "85.0", `"bob"`)
	score, err := z.Score(testCtx, "alice")
	if err != nil || score != 90.5 {
		t.Fatalf("Go score(alice) = %v, %v; want 90.5", score, err)
	}
	rank, err := z.Rank(testCtx, "alice")
	if err != nil || rank != 1 {
		t.Fatalf("Go rank(alice) = %d, %v; want 1 (bob=85 first)", rank, err)
	}

	// Go adds; Java reads score.
	if _, err := z.Add(testCtx, "carol", 99); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("zset_score " + name + ` "carol"`); err != nil {
		t.Fatal(err)
	} else if !floatEq(reply["value"], 99) {
		t.Fatalf("java score(carol) = %#v", reply["value"])
	}
}

func TestJavaInterop_RLexSortedSet(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-lex")
	t.Cleanup(func() { interopCleanup(t, name) })
	s := client.GetLexSortedSet(name)

	// Java adds raw members; Go sees them in lex order.
	for _, e := range []string{"banana", "apple"} {
		mustJava(t, "lex_add", name, e)
	}
	first, err := s.First(testCtx)
	if err != nil || first != "apple" {
		t.Fatalf("Go first = %q, %v; want apple", first, err)
	}

	// Go adds; Java range queries return raw members.
	if _, err := s.Add(testCtx, "cherry"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("lex_range " + name + " apple false cherry true"); err != nil {
		t.Fatal(err)
	} else {
		vals, _ := reply["values"].([]any)
		if len(vals) != 2 || vals[0] != "banana" || vals[1] != "cherry" {
			t.Fatalf("java range (apple, cherry] = %v", vals)
		}
	}
}

func TestJavaInterop_RBucket(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-bucket")
	t.Cleanup(func() { interopCleanup(t, name) })
	b := client.GetBucket(name)

	mustJava(t, "bucket_set", name, `"from-java"`)
	v, err := b.Get(testCtx)
	if err != nil || v != "from-java" {
		t.Fatalf("Go get = %v, %v", v, err)
	}

	if err := b.Set(testCtx, map[string]any{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("bucket_get " + name); err != nil {
		t.Fatal(err)
	} else {
		obj, ok := reply["value"].(map[string]any)
		if !ok || obj["k"] != "v" {
			t.Fatalf("java get of Go map = %#v", reply["value"])
		}
	}

	if err := b.Set(testCtx, "ttl-me"); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("bucket_getex " + name + " 60000"); err != nil || reply["value"] != "ttl-me" {
		t.Fatalf("java getAndExpire = %v, %v", reply, err)
	}
	if ttl, err := b.RemainTTL(testCtx); err != nil || ttl <= 0 {
		t.Fatalf("Go RemainTTL after Java getAndExpire = %v, %v", ttl, err)
	}
}

func TestJavaInterop_RBinaryStream(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-binary")
	t.Cleanup(func() { interopCleanup(t, name) })
	stream := client.GetBinaryStream(name)

	fromJava := []byte{0x00, 0xff, 'J', 'a', 'v', 'a'}
	if reply, err := javaSend("binary_set " + name + " " +
		base64.StdEncoding.EncodeToString(fromJava)); err != nil || !numEq(reply["size"], int64(len(fromJava))) {
		t.Fatalf("java binary_set = %v, %v", reply, err)
	}
	got, err := stream.Get(testCtx)
	if err != nil || !bytes.Equal(got, fromJava) {
		t.Fatalf("Go Get after Java set = %v, %v; want %v", got, err, fromJava)
	}

	fromGo := []byte{'G', 'o', 0x00, 0xfe}
	if err := stream.Set(testCtx, fromGo); err != nil {
		t.Fatal("Go Set:", err)
	}
	reply, err := javaSend("binary_get " + name)
	if err != nil {
		t.Fatal("java binary_get:", err)
	}
	encoded, _ := reply["value"].(string)
	javaRead, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || !bytes.Equal(javaRead, fromGo) {
		t.Fatalf("Java Get after Go set = %v, %v; want %v", javaRead, err, fromGo)
	}

	patch := []byte{0xaa, 0xbb}
	reply, err = javaSend("binary_channel_write " + name + " 1 " +
		base64.StdEncoding.EncodeToString(patch))
	if err != nil || !numEq(reply["written"], int64(len(patch))) {
		t.Fatalf("java binary_channel_write = %v, %v", reply, err)
	}
	got, err = stream.Get(testCtx)
	want := []byte{'G', 0xaa, 0xbb, 0xfe}
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Go Get after Java channel write = %v, %v; want %v", got, err, want)
	}
}

func TestJavaInterop_RMapCache(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-mapcache")
	t.Cleanup(func() {
		interopCleanup(t, name,
			"redisson__timeout__set:{"+name+"}",
			"redisson__idle__set:{"+name+"}")
	})
	mc := client.GetMapCache(name)

	// Java writes with TTL; Go reads the packed value.
	mustJava(t, "mapcache_put", name, `"jk"`, `"jv"`, "60000")
	v, err := mc.Get(testCtx, "jk")
	if err != nil || v != "jv" {
		t.Fatalf("Go read of java mapcache entry = %v, %v", v, err)
	}

	// Go writes with TTL; Java reads the packed value back.
	if err := mc.Put(testCtx, "gk", "gv", time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("mapcache_get " + name + ` "gk"`); err != nil || reply["value"] != "gv" {
		t.Fatalf("java read of Go mapcache entry = %v, %v", reply, err)
	}

	if reply, err := javaSend("mapcache_ttl " + name + ` "jk"`); err != nil {
		t.Fatal(err)
	} else if !ttlPositive(reply["ttl"]) {
		t.Fatalf("java remainTimeToLive = %v", reply["ttl"])
	}
	if rem, err := mc.RemainTTLForKey(testCtx, "jk"); err != nil || rem <= 0 {
		t.Fatalf("Go RemainTTLForKey = %v, %v", rem, err)
	}
	if reply, err := javaSend("mapcache_get_ttl_only " + name + ` "gk"`); err != nil || reply["value"] != "gv" {
		t.Fatalf("java getWithTTLOnly = %v, %v", reply, err)
	}
}

func TestJavaInterop_RDelayedQueue(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-dq")
	t.Cleanup(func() {
		interopCleanup(t, name, "redisson_delay_queue:{"+name+"}",
			"redisson_delay_queue_timeout:{"+name+"}")
	})

	// Java offers with 300ms delay; Go's target queue receives it.
	mustJava(t, "dq_offer", name, `"delayed-job"`, "300")
	q := client.GetQueue(name)
	if !eventual(t, 5*time.Second, func() bool {
		v, _ := q.Peek(testCtx)
		return v == "delayed-job"
	}) {
		t.Fatal("element from Redisson delayed queue never arrived on Go side")
	}
	// Consume it so the later peek assertion is unambiguous.
	if v, _ := q.Poll(testCtx); v != "delayed-job" {
		t.Fatalf("poll = %v", v)
	}

	// Go offers into the same delayed set; Java's target queue receives it.
	dq := client.GetDelayedQueue(name)
	if err := dq.Offer(testCtx, "go-delayed", 250*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !eventual(t, 5*time.Second, func() bool {
		reply, err := javaSend("dq_peek " + name)
		return err == nil && reply["value"] == "go-delayed"
	}) {
		t.Fatal("java peek never saw Go's migrated element")
	}
}

// floatEq compares a decoded JSON number against a float64.
func floatEq(v any, want float64) bool {
	switch n := v.(type) {
	case float64:
		return n == want
	case int:
		return float64(n) == want
	case int64:
		return float64(n) == want
	case json.Number:
		f, err := n.Float64()
		return err == nil && f == want
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return err == nil && f == want
	}
	return false
}
