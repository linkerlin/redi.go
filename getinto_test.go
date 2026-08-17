package redi_test

import (
	"testing"
)

type getIntoPerson struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestGetInto_MapListQueueBucket(t *testing.T) {
	client := newTestClient(t)
	p := getIntoPerson{Name: "ada", Age: 36}

	b := client.GetBucket(uniqueKey(t, "into-b"))
	defer b.Delete(testCtx) //nolint:errcheck
	if err := b.Set(testCtx, p); err != nil {
		t.Fatal(err)
	}
	var gotB getIntoPerson
	ok, err := b.GetInto(testCtx, &gotB)
	if err != nil || !ok || gotB != p {
		t.Fatalf("Bucket.GetInto = %+v, %v, %v; want %+v", gotB, ok, err, p)
	}

	m := client.GetMap(uniqueKey(t, "into-m"))
	defer m.Clear(testCtx) //nolint:errcheck
	if err := m.Put(testCtx, "user", p); err != nil {
		t.Fatal(err)
	}
	var gotM getIntoPerson
	ok, err = m.GetInto(testCtx, "user", &gotM)
	if err != nil || !ok || gotM != p {
		t.Fatalf("Map.GetInto = %+v, %v, %v; want %+v", gotM, ok, err, p)
	}
	ok, err = m.GetInto(testCtx, "missing", &gotM)
	if err != nil || ok {
		t.Fatalf("Map.GetInto missing = %v, %v; want false", ok, err)
	}

	l := client.GetList(uniqueKey(t, "into-l"))
	defer l.Clear(testCtx) //nolint:errcheck
	if err := l.Add(testCtx, p); err != nil {
		t.Fatal(err)
	}
	var gotL getIntoPerson
	ok, err = l.GetInto(testCtx, 0, &gotL)
	if err != nil || !ok || gotL != p {
		t.Fatalf("List.GetInto = %+v, %v, %v; want %+v", gotL, ok, err, p)
	}

	q := client.GetQueue(uniqueKey(t, "into-q"))
	defer q.Clear(testCtx) //nolint:errcheck
	if err := q.Offer(testCtx, p); err != nil {
		t.Fatal(err)
	}
	var peek getIntoPerson
	ok, err = q.PeekInto(testCtx, &peek)
	if err != nil || !ok || peek != p {
		t.Fatalf("Queue.PeekInto = %+v, %v, %v", peek, ok, err)
	}
	var polled getIntoPerson
	ok, err = q.PollInto(testCtx, &polled)
	if err != nil || !ok || polled != p {
		t.Fatalf("Queue.PollInto = %+v, %v, %v", polled, ok, err)
	}
	ok, err = q.PollInto(testCtx, &polled)
	if err != nil || ok {
		t.Fatalf("Queue.PollInto empty = %v, %v; want false", ok, err)
	}
}

func TestGetInto_Deque(t *testing.T) {
	client := newTestClient(t)
	d := client.GetDeque(uniqueKey(t, "into-d"))
	defer d.Clear(testCtx) //nolint:errcheck

	type item struct {
		N int `json:"n"`
	}
	if err := d.AddLast(testCtx, item{N: 1}, item{N: 2}); err != nil {
		t.Fatal(err)
	}
	var first, last item
	ok, err := d.PeekFirstInto(testCtx, &first)
	if err != nil || !ok || first.N != 1 {
		t.Fatalf("PeekFirstInto = %+v, %v, %v", first, ok, err)
	}
	ok, err = d.PeekLastInto(testCtx, &last)
	if err != nil || !ok || last.N != 2 {
		t.Fatalf("PeekLastInto = %+v, %v, %v", last, ok, err)
	}
	var popped item
	ok, err = d.RemoveFirstInto(testCtx, &popped)
	if err != nil || !ok || popped.N != 1 {
		t.Fatalf("RemoveFirstInto = %+v, %v, %v", popped, ok, err)
	}
	ok, err = d.RemoveLastInto(testCtx, &popped)
	if err != nil || !ok || popped.N != 2 {
		t.Fatalf("RemoveLastInto = %+v, %v, %v", popped, ok, err)
	}
}

func TestGetInto_NestedMapAndSlice(t *testing.T) {
	client := newTestClient(t)
	m := client.GetMap(uniqueKey(t, "into-nest"))
	defer m.Clear(testCtx) //nolint:errcheck

	nested := map[string]any{
		"tags": []any{"a", "b"},
		"meta": map[string]any{"ok": true},
	}
	if err := m.Put(testCtx, "doc", nested); err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	ok, err := m.GetInto(testCtx, "doc", &out)
	if err != nil || !ok {
		t.Fatalf("GetInto nested: %v, %v", ok, err)
	}
	tags, _ := out["tags"].([]any)
	if len(tags) != 2 || tags[0] != "a" {
		t.Fatalf("tags = %#v after unwrap", out["tags"])
	}
	meta, _ := out["meta"].(map[string]any)
	if meta["ok"] != true {
		t.Fatalf("meta = %#v", out["meta"])
	}
}
