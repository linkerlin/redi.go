package redi_test

import (
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

func TestRKeys(t *testing.T) {
	client := newTestClient(t)
	keys := client.GetKeys()
	prefix := uniqueKey(t, "keys") + ":"

	for _, suffix := range []string{"a", "b", "c"} {
		if err := client.GetBucket(prefix+suffix).Set(testCtx, "v"); err != nil {
			t.Fatal(err)
		}
	}
	defer keys.DeleteByPattern(testCtx, prefix+"*") //nolint:errcheck

	n, err := keys.CountExists(testCtx, prefix+"a", prefix+"b", prefix+"missing")
	if err != nil || n != 2 {
		t.Fatalf("CountExists = %d, %v; want 2", n, err)
	}

	typ, err := keys.Type(testCtx, prefix+"a")
	if err != nil || typ != "string" {
		t.Fatalf("Type = %q, %v", typ, err)
	}

	got, err := keys.Keys(testCtx, prefix+"*")
	if err != nil || len(got) != 3 {
		t.Fatalf("Keys = %v, %v; want 3", got, err)
	}

	// Copy.
	ok, err := keys.Copy(testCtx, prefix+"a", prefix+"copy", true)
	if err != nil || !ok {
		t.Fatalf("Copy = %v, %v", ok, err)
	}
	v, _ := client.GetBucket(prefix + "copy").Get(testCtx)
	if v != "v" {
		t.Fatalf("copied value = %v", v)
	}

	// DeleteByPattern removes all four.
	deleted, err := keys.DeleteByPattern(testCtx, prefix+"*")
	if err != nil || deleted != 4 {
		t.Fatalf("DeleteByPattern = %d, %v; want 4", deleted, err)
	}
	got, _ = keys.Keys(testCtx, prefix+"*")
	if len(got) != 0 {
		t.Fatalf("keys remain after pattern delete: %v", got)
	}
}

func TestRBuckets(t *testing.T) {
	client := newTestClient(t)
	buckets := client.GetBuckets()
	keys := client.GetKeys()
	base := uniqueKey(t, "buckets")
	k1, k2, k3 := base+":1", base+":2", base+":3"
	defer keys.DeleteByPattern(testCtx, base+":*") //nolint:errcheck

	if err := buckets.Set(testCtx, map[string]any{k1: "a", k2: int64(42)}, 0); err != nil {
		t.Fatal("Set:", err)
	}
	got, err := buckets.Get(testCtx, k1, k2, k3)
	if err != nil {
		t.Fatal("Get:", err)
	}
	if got[k1] != "a" || !numEq(got[k2], 42) {
		t.Fatalf("Get = %v", got)
	}
	if _, has := got[k3]; has {
		t.Fatal("missing key present in Get result")
	}

	// TrySet fails while keys exist.
	ok, err := buckets.TrySet(testCtx, map[string]any{k3: "c", k1: "overwrite"})
	if err != nil || ok {
		t.Fatalf("TrySet with existing key = %v, %v; want false", ok, err)
	}
	// TrySet over fresh keys succeeds.
	ok, _ = buckets.TrySet(testCtx, map[string]any{k3: "c"})
	if !ok {
		t.Fatal("TrySet on fresh keys = false")
	}

	// TTL variant.
	if err := buckets.Set(testCtx, map[string]any{k3: "expiring"}, 250*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !eventual(t, 2*time.Second, func() bool {
		got, _ := buckets.Get(testCtx, k3)
		_, alive := got[k3]
		return !alive
	}) {
		t.Fatal("TTL batch set did not expire")
	}
}

func TestRScript(t *testing.T) {
	client := newTestClient(t)
	script := client.GetScript()
	name := uniqueKey(t, "script")
	defer client.GetKeys().Delete(testCtx, name) //nolint:errcheck

	// INTEGER return with keys + values.
	v, err := script.Eval(testCtx,
		`redis.call('set', KEYS[1], ARGV[1]); return 42`,
		redi.ScriptReturnInteger, []string{name}, "hello")
	if err != nil {
		t.Fatal("Eval:", err)
	}
	if v != int64(42) {
		t.Fatalf("Eval result = %#v", v)
	}

	// BOOLEAN cast.
	v, err = script.Eval(testCtx, `return 1`, redi.ScriptReturnBoolean, nil)
	if err != nil || v != true {
		t.Fatalf("boolean cast = %#v, %v", v, err)
	}

	// Lua nil decodes to Go nil.
	v, err = script.Eval(testCtx, `return nil`, redi.ScriptReturnValue, nil)
	if err != nil || v != nil {
		t.Fatalf("nil reply = %#v, %v", v, err)
	}

	// ScriptLoad + EvalSha round-trip.
	sha, err := script.ScriptLoad(testCtx, `return redis.call('get', KEYS[1])`)
	if err != nil {
		t.Fatal("ScriptLoad:", err)
	}
	exists, err := script.ScriptExists(testCtx, sha)
	if err != nil || len(exists) != 1 || !exists[0] {
		t.Fatalf("ScriptExists = %v, %v", exists, err)
	}
	v, err = script.EvalSha(testCtx, sha, redi.ScriptReturnValue, []string{name})
	if err != nil {
		t.Fatal("EvalSha:", err)
	}
	if v != "hello" {
		t.Fatalf("EvalSha result = %#v", v)
	}
}
