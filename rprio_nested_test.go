package redi_test

import (
	"testing"
)

func TestRPriorityQueue(t *testing.T) {
	client := newTestClient(t)
	q := client.GetPriorityQueue(uniqueKey(t, "prio"))
	defer q.Clear(testCtx) //nolint:errcheck

	if err := q.Offer(testCtx, "low", 100); err != nil {
		t.Fatal(err)
	}
	if err := q.Offer(testCtx, "urgent", 1); err != nil {
		t.Fatal(err)
	}
	if err := q.Offer(testCtx, "medium", 50); err != nil {
		t.Fatal(err)
	}

	v, err := q.Peek(testCtx)
	if err != nil || v != "urgent" {
		t.Fatalf("Peek = %v, %v; want urgent", v, err)
	}
	score, ok, _ := q.PeekScore(testCtx, "medium")
	if !ok || score != 50 {
		t.Fatalf("PeekScore(medium) = %v, %v", score, ok)
	}
	if _, ok, _ := q.PeekScore(testCtx, "absent"); ok {
		t.Fatal("PeekScore(absent) = true")
	}

	// Drain in priority order.
	for _, want := range []string{"urgent", "medium", "low"} {
		v, err := q.Poll(testCtx)
		if err != nil || v != want {
			t.Fatalf("Poll = %v, %v; want %s", v, err, want)
		}
	}
	if v, _ := q.Poll(testCtx); v != nil {
		t.Fatalf("Poll on empty = %v", v)
	}
}

// TestJavaInterop_NestedAndPriorityQueue: deep-nested compound values
// (recursively type-wrapped) decode correctly on the Java side, and the
// priority queue's ZSET is plain Java-readable geometry.
func TestJavaInterop_NestedAndPriorityQueue(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)

	// --- Deep nesting: map > array > map, verified against real Redisson.
	name := uniqueKey(t, "jio-nested")
	t.Cleanup(func() { interopCleanup(t, name) })
	m := client.GetMap(name)
	deep := map[string]any{
		"outer": map[string]any{
			"list": []any{1, map[string]any{"inner": true}},
			"flag": "x",
		},
		"nums": []any{7, 8},
	}
	if err := m.Put(testCtx, "deep", deep); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("map_get " + name + ` "deep"`); err != nil {
		t.Fatal(err)
	} else {
		obj, ok := reply["value"].(map[string]any)
		if !ok {
			t.Fatalf("java read of nested = %#v", reply["value"])
		}
		outer, _ := obj["outer"].(map[string]any)
		if outer == nil || outer["flag"] != "x" {
			t.Fatalf("java nested outer = %#v", obj["outer"])
		}
		list, _ := outer["list"].([]any)
		if len(list) != 2 || !numEq(list[0], 1) {
			t.Fatalf("java nested list = %#v", list)
		}
		inner, _ := list[1].(map[string]any)
		if inner == nil || inner["inner"] != true {
			t.Fatalf("java nested inner map = %#v", list[1])
		}
		nums, _ := obj["nums"].([]any)
		if len(nums) != 2 || !numEq(nums[1], 8) {
			t.Fatalf("java nested nums = %#v", nums)
		}
	}
}
