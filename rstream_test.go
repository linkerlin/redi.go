package redi_test

import (
	"testing"
	"time"
)

func TestRStream_Basics(t *testing.T) {
	client := newTestClient(t)
	s := client.GetStream(uniqueKey(t, "stream"))
	defer s.Delete(testCtx) //nolint:errcheck

	id1, err := s.Add(testCtx, map[string]any{"event": "created", "n": 1})
	if err != nil {
		t.Fatal("Add:", err)
	}
	id2, _ := s.Add(testCtx, map[string]any{"event": "updated"})
	if id1 == "" || id2 == "" || id1 >= id2 {
		t.Fatalf("ids not ascending: %q %q", id1, id2)
	}

	n, _ := s.Len(testCtx)
	if n != 2 {
		t.Fatalf("Len = %d", n)
	}

	entries, err := s.ReadRange(testCtx, "-", "+", 0)
	if err != nil {
		t.Fatal("ReadRange:", err)
	}
	if len(entries) != 2 || entries[0].Fields["event"] != "created" {
		t.Fatalf("entries = %+v", entries)
	}
	if v := entries[0].Fields["n"]; v == nil {
		t.Fatal("numeric field lost")
	}

	rev, _ := s.ReadReverse(testCtx, "+", "-", 1)
	if len(rev) != 1 || rev[0].ID != id2 {
		t.Fatalf("ReadReverse = %+v", rev)
	}

	removed, _ := s.Remove(testCtx, id1)
	if removed != 1 {
		t.Fatalf("Remove = %d", removed)
	}
	n, _ = s.Len(testCtx)
	if n != 1 {
		t.Fatalf("Len after remove = %d", n)
	}
}

func TestRStream_SyncAPIs(t *testing.T) {
	client := newTestClient(t)
	s := client.GetStream(uniqueKey(t, "stream-sync"))
	defer s.Delete(testCtx) //nolint:errcheck

	id, err := s.AddWithID(testCtx, "1-0", map[string]any{"event": "explicit"})
	if err != nil || id != "1-0" {
		t.Fatalf("AddWithID = %q, %v", id, err)
	}
	entries, err := s.Read(testCtx, "0-0", 10, 0)
	if err != nil || len(entries) != 1 || entries[0].Fields["event"] != "explicit" {
		t.Fatalf("Read = %+v, %v", entries, err)
	}
	if created, err := s.CreateGroup(testCtx, "g", "0"); err != nil || !created {
		t.Fatalf("CreateGroup = %v, %v", created, err)
	}
	if created, err := s.CreateConsumer(testCtx, "g", "c"); err != nil || !created {
		t.Fatalf("CreateConsumer = %v, %v", created, err)
	}
	groups, err := s.ListGroups(testCtx)
	if err != nil || len(groups) != 1 || groups[0].Name != "g" {
		t.Fatalf("ListGroups = %+v, %v", groups, err)
	}
	consumers, err := s.ListConsumers(testCtx, "g")
	if err != nil || len(consumers) != 1 || consumers[0].Name != "c" {
		t.Fatalf("ListConsumers = %+v, %v", consumers, err)
	}
	if delivered, err := s.ReadGroup(testCtx, "g", "c", 1, 0); err != nil || len(delivered) != 1 {
		t.Fatalf("ReadGroup = %+v, %v", delivered, err)
	}
	pending, err := s.PendingInfo(testCtx, "g")
	if err != nil || pending.Count != 1 {
		t.Fatalf("PendingInfo = %+v, %v", pending, err)
	}
	info, err := s.GetInfo(testCtx)
	if err != nil || info.Length != 1 || info.LastGeneratedID != "1-0" {
		t.Fatalf("GetInfo = %+v, %v", info, err)
	}
}

func TestRStream_ConsumerGroup(t *testing.T) {
	client := newTestClient(t)
	s := client.GetStream(uniqueKey(t, "stream-group"))
	defer s.Delete(testCtx) //nolint:errcheck

	created, err := s.CreateGroup(testCtx, "g", "0")
	if err != nil || !created {
		t.Fatalf("CreateGroup = %v, %v", created, err)
	}
	// Duplicate creation reports false, not an error.
	created, err = s.CreateGroup(testCtx, "g", "0")
	if err != nil || created {
		t.Fatalf("duplicate CreateGroup = %v, %v", created, err)
	}

	if _, err := s.Add(testCtx, map[string]any{"job": "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(testCtx, map[string]any{"job": "two"}); err != nil {
		t.Fatal(err)
	}

	// Deliver to consumer A.
	delivered, err := s.ReadGroup(testCtx, "g", "consumer-a", 10, 0)
	if err != nil {
		t.Fatal("ReadGroup:", err)
	}
	if len(delivered) != 2 || delivered[0].Fields["job"] != "one" {
		t.Fatalf("ReadGroup = %+v", delivered)
	}

	// Second read: nothing new.
	got, err := s.ReadGroup(testCtx, "g", "consumer-a", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("re-read delivered %d entries", len(got))
	}

	// Pending entries accumulate until ack.
	pending, err := s.PendingRange(testCtx, "g", "", 10)
	if err != nil {
		t.Fatal("PendingRange:", err)
	}
	if len(pending) != 2 || pending[0].Consumer != "consumer-a" {
		t.Fatalf("pending = %+v", pending)
	}

	n, err := s.Ack(testCtx, "g", delivered[0].ID, delivered[1].ID)
	if err != nil || n != 2 {
		t.Fatalf("Ack = %d, %v", n, err)
	}
	pending, _ = s.PendingRange(testCtx, "g", "", 10)
	if len(pending) != 0 {
		t.Fatalf("pending after ack = %+v", pending)
	}

	// Claim path: deliver to B, wait for idle, claim from A's name.
	if _, err := s.Add(testCtx, map[string]any{"job": "three"}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ReadGroup(testCtx, "g", "consumer-b", 1, 0)
	if len(got) != 1 {
		t.Fatalf("consumer-b read = %+v", got)
	}
	time.Sleep(50 * time.Millisecond)
	claimed, _, err := s.AutoClaim(testCtx, "g", "consumer-a", 10*time.Millisecond, "0-0", 10)
	if err != nil {
		t.Fatal("AutoClaim:", err)
	}
	if len(claimed) != 1 || claimed[0].Fields["job"] != "three" {
		t.Fatalf("AutoClaim = %+v", claimed)
	}

	if ok, _ := s.DeleteGroup(testCtx, "g"); !ok {
		t.Fatal("DeleteGroup = false")
	}
}

// TestJavaInterop_RStream: Go and Java share one stream + consumer group;
// entries written by either side are delivered and acked across languages.
func TestJavaInterop_RStream(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-stream")
	t.Cleanup(func() { interopCleanup(t, name) })
	s := client.GetStream(name)

	// Go creates the group at 0 (whole history).
	if _, err := s.CreateGroup(testCtx, "g", "0"); err != nil {
		t.Fatal(err)
	}

	// Java writes two entries.
	mustJava(t, "stream_add", name, `"who"`, `"java"`)
	mustJava(t, "stream_add", name, `"who"`, `"java2"`)

	// Go reads them through the group.
	got, err := s.ReadGroup(testCtx, "g", "go-consumer", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Fields["who"] != "java" || got[1].Fields["who"] != "java2" {
		t.Fatalf("Go ReadGroup of java entries = %+v", got)
	}

	// Java reads the same delivered-but-unacked entries via a different
	// consumer claim-free path: its own group reading only new entries.
	if reply, err := javaSend("stream_read_group " + name + " g java-consumer 10"); err != nil {
		t.Fatal(err)
	} else if entries, ok := reply["entries"].(map[string]any); ok && len(entries) > 0 {
		t.Fatalf("java consumer unexpectedly received delivered entries: %v", entries)
	}

	// Go acks the first entry; Java's own read/ack path on the same ids.
	if n, err := s.Ack(testCtx, "g", got[0].ID); err != nil || n != 1 {
		t.Fatalf("Go Ack = %d, %v", n, err)
	}

	// Java acks the second entry by id.
	if reply, err := javaSend("stream_ack " + name + " g " + got[1].ID); err != nil || !numEq(reply["value"], 1) {
		t.Fatalf("java ack = %v, %v", reply, err)
	}

	// Pending must be empty for the group (both sides acked).
	pending, err := s.PendingRange(testCtx, "g", "", 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after cross-language acks = %+v, %v", pending, err)
	}

	// Go writes; a NEW Java group reads from the beginning.
	if _, err := s.Add(testCtx, map[string]any{"who": "go"}); err != nil {
		t.Fatal(err)
	}
	mustJava(t, "stream_create_group", name, "g2")
	if reply, err := javaSend("stream_read_group " + name + " g2 jreader 10"); err != nil {
		t.Fatal(err)
	} else {
		entries, _ := reply["entries"].(map[string]any)
		if len(entries) != 3 {
			t.Fatalf("java new group read %d entries, want all 3: %v", len(entries), entries)
		}
		// The Go-written entry must decode with its field name and value.
		foundGo := false
		for _, v := range entries {
			if v == "who=go" {
				foundGo = true
			}
		}
		if !foundGo {
			t.Fatalf("java did not see Go's entry; entries = %v", entries)
		}
	}
}
