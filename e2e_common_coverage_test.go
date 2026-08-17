package redi_test

import (
	"testing"
	"time"
)

func TestE2E_MapAndMapCache_AdditionalSurface(t *testing.T) {
	client := newTestClient(t)

	mapName := uniqueKey(t, "e2e-map-more")
	t.Cleanup(func() { interopCleanup(t, mapName) })
	m := client.GetMap(mapName)
	if err := m.Put(testCtx, "a", "one"); err != nil {
		t.Fatal(err)
	}
	if got, err := m.GetAllKeys(testCtx, "a", "missing"); err != nil ||
		len(got) != 1 || got["a"] != "one" {
		t.Fatalf("GetAllKeys = %#v, %v", got, err)
	}
	if ok, err := m.ContainsValue(testCtx, "one"); err != nil || !ok {
		t.Fatalf("ContainsValue = %v, %v", ok, err)
	}
	if ok, err := m.PutIfAbsent(testCtx, "b", "two"); err != nil || !ok {
		t.Fatalf("PutIfAbsent new = %v, %v", ok, err)
	}
	if ok, err := m.FastPutIfAbsent(testCtx, "b", "three"); err != nil || ok {
		t.Fatalf("FastPutIfAbsent existing = %v, %v", ok, err)
	}
	if ok, err := m.PutIfExists(testCtx, "a", "four"); err != nil || !ok {
		t.Fatalf("PutIfExists existing = %v, %v", ok, err)
	}
	if ok, err := m.PutIfExists(testCtx, "missing", "four"); err != nil || ok {
		t.Fatalf("PutIfExists missing = %v, %v", ok, err)
	}

	cacheName := uniqueKey(t, "e2e-mapcache-more")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+cacheName+"*") })
	cache := client.GetMapCache(cacheName)
	if err := cache.PutAll(testCtx, map[string]any{"a": "one", "b": "two"}); err != nil {
		t.Fatal(err)
	}
	if got, err := cache.GetAllKeys(testCtx, "a", "missing"); err != nil ||
		len(got) != 1 || got["a"] != "one" {
		t.Fatalf("GetAllKeys = %#v, %v", got, err)
	}
	if keys, err := cache.Keys(testCtx); err != nil || len(keys) != 2 {
		t.Fatalf("Keys = %v, %v", keys, err)
	}
	if values, err := cache.Values(testCtx); err != nil || len(values) != 2 {
		t.Fatalf("Values = %v, %v", values, err)
	}
	if ok, err := cache.ContainsKey(testCtx, "a"); err != nil || !ok {
		t.Fatalf("ContainsKey = %v, %v", ok, err)
	}
	if ok, err := cache.ContainsValue(testCtx, "two"); err != nil || !ok {
		t.Fatalf("ContainsValue = %v, %v", ok, err)
	}
	if old, err := cache.Replace(testCtx, "a", "three"); err != nil || old != "one" {
		t.Fatalf("Replace = %v, %v", old, err)
	}
	if old, err := cache.Replace(testCtx, "missing", "three"); err != nil || old != nil {
		t.Fatalf("Replace missing = %v, %v", old, err)
	}
	if ok, err := cache.ReplaceIf(testCtx, "a", "one", "four"); err != nil || ok {
		t.Fatalf("ReplaceIf mismatch = %v, %v", ok, err)
	}
	if ok, err := cache.ReplaceIf(testCtx, "a", "three", "four"); err != nil || !ok {
		t.Fatalf("ReplaceIf match = %v, %v", ok, err)
	}
	if ok, err := cache.PutIfExists(testCtx, "b", "five"); err != nil || !ok {
		t.Fatalf("PutIfExists = %v, %v", ok, err)
	}
	if ok, err := cache.FastPutIfExists(testCtx, "missing", "five"); err != nil || ok {
		t.Fatalf("FastPutIfExists missing = %v, %v", ok, err)
	}
	if ok, err := cache.FastReplace(testCtx, "b", "six"); err != nil || !ok {
		t.Fatalf("FastReplace = %v, %v", ok, err)
	}
	if err := cache.Delete(testCtx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := cache.Unlink(testCtx); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_ObjectGeoBitSet_AdditionalSurface(t *testing.T) {
	client := newTestClient(t)

	oldName := uniqueKey(t, "e2e-object-old")
	newName := uniqueKey(t, "e2e-object-new")
	t.Cleanup(func() { interopCleanup(t, oldName, newName) })
	bucket := client.GetBucket(oldName)
	if err := bucket.Set(testCtx, "value"); err != nil {
		t.Fatal(err)
	}
	if ok, err := bucket.Touch(testCtx); err != nil || !ok {
		t.Fatalf("Touch = %v, %v", ok, err)
	}
	if ok, err := bucket.ExpireAt(testCtx, time.Now().Add(time.Minute)); err != nil || !ok {
		t.Fatalf("ExpireAt = %v, %v", ok, err)
	}
	if ok, err := bucket.ClearExpire(testCtx); err != nil || !ok {
		t.Fatalf("ClearExpire = %v, %v", ok, err)
	}
	if err := bucket.Rename(testCtx, newName); err != nil {
		t.Fatal(err)
	}
	if bucket.Name() != newName {
		t.Fatalf("Name after Rename = %q", bucket.Name())
	}
	if err := bucket.Unlink(testCtx); err != nil {
		t.Fatal(err)
	}

	geoName := uniqueKey(t, "e2e-geo-more")
	t.Cleanup(func() { interopCleanup(t, geoName) })
	geo := client.GetGeo(geoName)
	if _, err := geo.Add(testCtx, 2.3522, 48.8566, "paris"); err != nil {
		t.Fatal(err)
	}
	if _, err := geo.Add(testCtx, 2.295, 48.8738, "arc"); err != nil {
		t.Fatal(err)
	}
	if hash, err := geo.Hash(testCtx, "paris"); err != nil || hash == "" {
		t.Fatalf("Hash = %q, %v", hash, err)
	}
	if entries, err := geo.SearchByMember(
		testCtx, "paris", 10, "km", true, true,
	); err != nil || len(entries) != 2 {
		t.Fatalf("SearchByMember = %v, %v", entries, err)
	}
	if size, err := geo.Size(testCtx); err != nil || size != 2 {
		t.Fatalf("Size = %d, %v", size, err)
	}
	if err := geo.Remove(testCtx, "arc"); err != nil {
		t.Fatal(err)
	}

	aName := uniqueKey(t, "e2e-bits-a")
	bName := uniqueKey(t, "e2e-bits-b")
	cName := uniqueKey(t, "e2e-bits-c")
	t.Cleanup(func() { interopCleanup(t, aName, bName, cName) })
	a, b, c := client.GetBitSet(aName), client.GetBitSet(bName), client.GetBitSet(cName)
	if err := a.SetMany(testCtx, true, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := b.SetMany(testCtx, true, 2, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Set(testCtx, 3, true); err != nil {
		t.Fatal(err)
	}
	if err := a.And(testCtx, b.Name()); err != nil {
		t.Fatal(err)
	}
	if err := a.Xor(testCtx, c.Name()); err != nil {
		t.Fatal(err)
	}
	if old, err := a.Clear(testCtx, 2); err != nil || !old {
		t.Fatalf("Clear = %v, %v", old, err)
	}
}

func TestE2E_ScoredSortedSet_AdditionalSurface(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-scored-more")
	t.Cleanup(func() { interopCleanup(t, name) })
	set := client.GetScoredSortedSet(name)

	for score, member := range map[float64]string{1: "a", 2: "b", 3: "c", 4: "d"} {
		if _, err := set.Add(testCtx, member, score); err != nil {
			t.Fatal(err)
		}
	}
	if rank, err := set.RevRank(testCtx, "c"); err != nil || rank != 1 {
		t.Fatalf("RevRank = %d, %v", rank, err)
	}
	if first, err := set.First(testCtx); err != nil || first != "a" {
		t.Fatalf("First = %v, %v", first, err)
	}
	if last, err := set.Last(testCtx); err != nil || last != "d" {
		t.Fatalf("Last = %v, %v", last, err)
	}
	if values, err := set.RangeReversed(testCtx, 0, 1); err != nil ||
		len(values) != 2 || values[0] != "d" {
		t.Fatalf("RangeReversed = %v, %v", values, err)
	}
	if values, err := set.RangeByScore(testCtx, 2, 3); err != nil || len(values) != 2 {
		t.Fatalf("RangeByScore = %v, %v", values, err)
	}
	if value, err := set.PollFirst(testCtx); err != nil || value != "a" {
		t.Fatalf("PollFirst = %v, %v", value, err)
	}
	if value, err := set.PollLast(testCtx); err != nil || value != "d" {
		t.Fatalf("PollLast = %v, %v", value, err)
	}
	if removed, err := set.RemoveRangeByRank(testCtx, 0, 0); err != nil || removed != 1 {
		t.Fatalf("RemoveRangeByRank = %d, %v", removed, err)
	}
	if _, err := set.Add(testCtx, "x", 10); err != nil {
		t.Fatal(err)
	}
	if removed, err := set.RemoveRangeByScore(testCtx, 9, 11); err != nil || removed != 1 {
		t.Fatalf("RemoveRangeByScore = %d, %v", removed, err)
	}
	if err := set.Remove(testCtx, "c"); err != nil {
		t.Fatal(err)
	}
}
