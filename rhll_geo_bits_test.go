package redi_test

import (
	"encoding/json"
	"testing"
)

func TestRHyperLogLog(t *testing.T) {
	client := newTestClient(t)
	h := client.GetHyperLogLog(uniqueKey(t, "hll"))
	defer h.Delete(testCtx) //nolint:errcheck

	for i := 0; i < 100; i++ {
		if _, err := h.Add(testCtx, i); err != nil {
			t.Fatal(err)
		}
	}
	n, err := h.Count(testCtx)
	if err != nil {
		t.Fatal(err)
	}
	// HLL standard error ~0.81%: 100 ± 5 is generous.
	if n < 95 || n > 105 {
		t.Fatalf("count = %d, want ~100", n)
	}
}

func TestRGeo(t *testing.T) {
	client := newTestClient(t)
	g := client.GetGeo(uniqueKey(t, "geo"))
	defer g.Delete(testCtx) //nolint:errcheck

	// Beijing / Shanghai.
	if _, err := g.Add(testCtx, 116.40, 39.90, "beijing"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Add(testCtx, 121.47, 31.23, "shanghai"); err != nil {
		t.Fatal(err)
	}

	pos, err := g.Pos(testCtx, "beijing")
	if err != nil || len(pos) != 2 {
		t.Fatalf("Pos = %v, %v", pos, err)
	}
	// GEOPOS re-derives from geohash: ~1e-5 quantization is inherent.
	if abs(pos[0]-116.40) > 1e-4 || abs(pos[1]-39.90) > 1e-4 {
		t.Fatalf("Pos = %v", pos)
	}

	d, err := g.Dist(testCtx, "beijing", "shanghai", "km")
	if err != nil {
		t.Fatal(err)
	}
	if d < 1000 || d > 1100 { // real value ~1068km
		t.Fatalf("Dist = %v, want ~1068km", d)
	}

	entries, err := g.Search(testCtx, 116.40, 39.90, 50, "km", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Member != "beijing" {
		t.Fatalf("Search = %+v", entries)
	}

	// TryAdd on existing member is a no-op.
	ok, err := g.TryAdd(testCtx, 0, 0, "beijing")
	if err != nil || ok {
		t.Fatalf("TryAdd existing = %v, %v; want false", ok, err)
	}
	// Position unchanged after TryAdd (geohash quantization tolerance).
	pos, _ = g.Pos(testCtx, "beijing")
	if abs(pos[0]-116.40) > 1e-4 {
		t.Fatal("TryAdd overwrote existing member position")
	}
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func TestRBitSet(t *testing.T) {
	client := newTestClient(t)
	b := client.GetBitSet(uniqueKey(t, "bits"))
	defer b.ClearAll(testCtx) //nolint:errcheck

	prev, err := b.Set(testCtx, 0, true)
	if err != nil || prev {
		t.Fatalf("Set(0) prev = %v, %v", prev, err)
	}
	if _, err := b.Set(testCtx, 3, true); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Set(testCtx, 9, true); err != nil {
		t.Fatal(err)
	}

	got, _ := b.Get(testCtx, 0)
	if !got {
		t.Fatal("Get(0) = false")
	}
	got, _ = b.Get(testCtx, 1)
	if got {
		t.Fatal("Get(1) = true")
	}

	card, _ := b.Cardinality(testCtx)
	if card != 3 {
		t.Fatalf("Cardinality = %d, want 3", card)
	}
	// Java semantics: highest set bit + 1 → 10.
	length, _ := b.Length(testCtx)
	if length != 10 {
		t.Fatalf("Length = %d, want 10", length)
	}

	// Byte round-trip: bits 0,3,9 → MSB-first bytes 0x90 0x40
	// (bit0=0x80 | bit3=0x10; bit9 = byte1 bit1 = 0x40).
	raw, _ := b.ToByteArray(testCtx)
	if len(raw) != 2 || raw[0] != 0x90 || raw[1] != 0x40 {
		t.Fatalf("bytes = % x, want 90 40", raw)
	}
	b2 := client.GetBitSet(uniqueKey(t, "bits2"))
	defer b2.ClearAll(testCtx) //nolint:errcheck
	if err := b2.FromByteArray(testCtx, raw); err != nil {
		t.Fatal(err)
	}
	got, _ = b2.Get(testCtx, 9)
	if !got {
		t.Fatal("FromByteArray round-trip lost bit 9")
	}

	// BITOP OR.
	b3 := client.GetBitSet(uniqueKey(t, "bits3"))
	defer b3.ClearAll(testCtx) //nolint:errcheck
	if _, err := b3.Set(testCtx, 20, true); err != nil {
		t.Fatal(err)
	}
	if err := b2.Or(testCtx, b3.Name()); err != nil {
		t.Fatal(err)
	}
	card, _ = b2.Cardinality(testCtx)
	if card != 4 {
		t.Fatalf("after OR cardinality = %d, want 4", card)
	}
}

// TestJavaInterop_RHyperLogLog / RGeo / RBitSet: direct Redisson validation.
func TestJavaInterop_HLL_Geo_BitSet(t *testing.T) {
	javaProbe(t)
	client := newTestClient(t)

	// --- HLL ---
	hllName := uniqueKey(t, "jio-hll")
	t.Cleanup(func() { interopCleanup(t, hllName) })
	mustJava(t, "hll_add", hllName, `"shared"`)
	mustJava(t, "hll_add", hllName, `"java-only"`)
	h := client.GetHyperLogLog(hllName)
	if _, err := h.Add(testCtx, "go-only"); err != nil {
		t.Fatal(err)
	}
	// Java must see all three registers (union count).
	if reply, err := javaSend("hll_count " + hllName); err != nil || !numEq(reply["count"], 3) {
		t.Fatalf("java count = %v, %v; want 3", reply, err)
	}
	n, _ := h.Count(testCtx)
	if n != 3 {
		t.Fatalf("Go count = %d, want 3", n)
	}

	// --- Geo ---
	geoName := uniqueKey(t, "jio-geo")
	t.Cleanup(func() { interopCleanup(t, geoName) })
	g := client.GetGeo(geoName)
	if _, err := g.Add(testCtx, 116.40, 39.90, "beijing"); err != nil {
		t.Fatal(err)
	}
	// Java adds Shanghai and reads Go's Beijing position.
	mustJava(t, "geo_add", geoName, "121.47", "31.23", `"shanghai"`)
	if reply, err := javaSend("geo_pos " + geoName + ` "beijing"`); err != nil {
		t.Fatal(err)
	} else if !closeTo(jsonNum(reply["lon"]), 116.40) || !closeTo(jsonNum(reply["lat"]), 39.90) {
		t.Fatalf("java pos(beijing) = %v", reply)
	}
	// Go reads the distance Java's member participates in.
	d, err := g.Dist(testCtx, "beijing", "shanghai", "km")
	if err != nil || d < 1000 || d > 1100 {
		t.Fatalf("Go dist = %v, %v", d, err)
	}
	if reply, err := javaSend("geo_dist " + geoName + ` "beijing" "shanghai" METERS`); err != nil {
		t.Fatal(err)
	} else {
		meters := jsonNum(reply["value"])
		if meters < 1e6 || meters > 1.1e6 {
			t.Fatalf("java dist meters = %v", reply["value"])
		}
	}

	// --- BitSet ---
	bsName := uniqueKey(t, "jio-bits")
	t.Cleanup(func() { interopCleanup(t, bsName) })
	b := client.GetBitSet(bsName)
	if _, err := b.Set(testCtx, 5, true); err != nil {
		t.Fatal(err)
	}
	mustJava(t, "bitset_set", bsName, "70")
	// Java reads Go's bit; Go reads Java's bit.
	if reply, err := javaSend("bitset_get " + bsName + " 5"); err != nil || reply["value"] != true {
		t.Fatalf("java get(5) = %v, %v", reply, err)
	}
	got, _ := b.Get(testCtx, 70)
	if !got {
		t.Fatal("Go get(70) = false")
	}
	// Java computes cardinality over both writers' bits.
	if reply, err := javaSend("bitset_cardinality " + bsName); err != nil || !numEq(reply["value"], 2) {
		t.Fatalf("java cardinality = %v, %v", reply, err)
	}
	if reply, err := javaSend("bitset_length " + bsName); err != nil || !numEq(reply["value"], 71) {
		t.Fatalf("java length = %v, %v; want 71", reply, err)
	}
	length, _ := b.Length(testCtx)
	if length != 71 {
		t.Fatalf("Go length = %d, want 71", length)
	}
}

func jsonNum(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	}
	return 0
}

// closeTo tolerates GEOPOS geohash quantization (~1e-5).
func closeTo(got, want float64) bool { return abs(got-want) <= 1e-4 }
