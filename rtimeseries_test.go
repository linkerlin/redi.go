package redi_test

import (
	"strconv"
	"testing"
	"time"
)

func TestRTimeSeries(t *testing.T) {
	client := newTestClient(t)
	s := client.GetTimeSeries(uniqueKey(t, "ts"))
	defer s.Delete(testCtx) //nolint:errcheck

	base := int64(1700000000000)
	if err := s.Add(testCtx, base, "cpu=90", "", 0); err != nil {
		t.Fatal("Add 1:", err)
	}
	if err := s.Add(testCtx, base+1000, "cpu=95", "host-a", 0); err != nil {
		t.Fatal("Add 2:", err)
	}
	// Same-timestamp entries coexist (sequence ids disambiguate members).
	if err := s.Add(testCtx, base+1000, "cpu=97", "host-b", 0); err != nil {
		t.Fatal("Add 3:", err)
	}

	n, _ := s.Size(testCtx)
	if n != 3 {
		t.Fatalf("Size = %d, want 3", n)
	}

	e, err := s.Get(testCtx, base)
	if err != nil || e == nil || e.Value != "cpu=90" || e.Label != "" {
		t.Fatalf("Get = %+v, %v", e, err)
	}
	// The FIRST stored entry at a timestamp wins in Java semantics; ours
	// reads the first unexpired member - same behavior for single entries.
	if e2, _ := s.Get(testCtx, base+1000); e2 == nil || e2.Label != "host-a" {
		t.Fatalf("Get labeled = %+v", e2)
	}

	// Range ascending across both same-ts entries.
	entries, err := s.Range(testCtx, base, base+1000, 0)
	if err != nil {
		t.Fatal("Range:", err)
	}
	if len(entries) != 3 || entries[0].Timestamp != base {
		t.Fatalf("Range = %+v", entries)
	}

	// TTL: a short-lived entry disappears after expiry.
	if err := s.Add(testCtx, base+2000, "ephemeral", "", 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !eventual(t, 2*time.Second, func() bool {
		e, _ := s.Get(testCtx, base+2000)
		return e == nil
	}) {
		t.Fatal("short-TTL entry did not expire")
	}
	// Range also filters expired entries.
	entries, _ = s.Range(testCtx, base, base+2000, 0)
	if len(entries) != 3 {
		t.Fatalf("Range after expiry = %d entries, want 3 (expired filtered)", len(entries))
	}

	// Remove one timestamp (both same-ts members).
	removed, err := s.Remove(testCtx, base+1000)
	if err != nil || removed != 2 {
		t.Fatalf("Remove = %d, %v; want 2", removed, err)
	}
	n, _ = s.Size(testCtx)
	if n != 1 {
		t.Fatalf("Size after remove = %d", n)
	}
}

// TestJavaInterop_RTimeSeries: Go-written entries decode through real
// Redisson, and vice versa.
func TestJavaInterop_RTimeSeries(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)
	name := uniqueKey(t, "jio-ts")
	t.Cleanup(func() {
		interopCleanupPattern(t, "redisson__ts_*:{"+name+"}*")
		interopCleanup(t, name)
	})
	s := client.GetTimeSeries(name)

	// Go writes; Java reads the exact timestamp (string value round-trips
	// through the codec as a bare JSON string).
	ts := time.Now().Add(-time.Minute).Truncate(time.Millisecond).UnixMilli()
	if err := s.Add(testCtx, ts, "temp=21.5", "", 0); err != nil {
		t.Fatal(err)
	}
	if reply, err := javaSend("ts_get " + name + " " + i64(ts)); err != nil {
		t.Fatal(err)
	} else if reply["value"] != "temp=21.5" {
		t.Fatalf("java read of Go entry = %#v", reply["value"])
	}

	// Java writes (plain value + auto timestamp); Go sees it via size and
	// range decode.
	if _, err := javaSend("ts_add " + name + " " + i64(ts+5000) + " \"from-java\""); err != nil {
		t.Fatal(err)
	}
	entries, err := s.Range(testCtx, ts, ts+5000, 10)
	if err != nil {
		t.Fatal(err)
	}
	foundJava, foundGo := false, false
	for _, e := range entries {
		if e.Value == "from-java" {
			foundJava = true
		}
		if e.Value == "temp=21.5" {
			foundGo = true
		}
	}
	if !foundJava || !foundGo {
		t.Fatalf("range = %+v; java=%v go=%v", entries, foundJava, foundGo)
	}
	if reply, err := javaSend("ts_range_size " + name); err != nil || !numEq(reply["value"], 2) {
		t.Fatalf("java size = %v, %v; want 2", reply, err)
	}
}

func i64(n int64) string { return strconv.FormatInt(n, 10) }
