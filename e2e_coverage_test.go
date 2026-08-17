package redi_test

import (
	"context"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

// E2E coverage-oriented tests: hit Redis end-to-end for previously thin surfaces.

func TestE2E_RArray_FullSurface(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-array")
	arr := client.GetArray(name)
	t.Cleanup(func() { _ = arr.Clear(testCtx) })

	n, err := arr.Set(testCtx, 0, "z")
	if skipArray(t, err) {
		return
	}
	if err != nil || n != 1 {
		t.Fatalf("Set = %d, %v", n, err)
	}
	if _, err := arr.SetAll(testCtx, 1, "a", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := arr.SetEntries(testCtx, map[int64]any{10: "x", 11: "y"}); err != nil {
		t.Fatal(err)
	}
	v, err := arr.Get(testCtx, 10)
	if err != nil || v != "x" {
		t.Fatalf("Get = %v, %v", v, err)
	}
	multi, err := arr.GetMulti(testCtx, 0, 1, 99)
	if err != nil || multi[0] != "z" || multi[1] != "a" || multi[2] != nil {
		t.Fatalf("GetMulti = %#v, %v", multi, err)
	}
	rng, err := arr.Range(testCtx, 0, 2)
	if err != nil || len(rng) != 3 || rng[0] != "z" {
		t.Fatalf("Range = %#v, %v", rng, err)
	}
	if c, err := arr.Count(testCtx); err != nil || c < 5 {
		t.Fatalf("Count = %d, %v", c, err)
	}
	if ln, err := arr.Length(testCtx); err != nil || ln < 12 {
		t.Fatalf("Length = %d, %v", ln, err)
	}
	if _, err := arr.DeleteIndexes(testCtx, 11); err != nil {
		t.Fatal(err)
	}
	if _, err := arr.DeleteRange(testCtx, 1, 2); err != nil {
		t.Fatal(err)
	}
	if ok, err := arr.Seek(testCtx, 20); err != nil || !ok {
		t.Fatalf("Seek = %v, %v", ok, err)
	}
	if next, err := arr.Next(testCtx); err != nil || next != 20 {
		t.Fatalf("Next = %d, %v", next, err)
	}
	if idx, err := arr.Insert(testCtx, "ins"); err != nil || idx != 20 {
		t.Fatalf("Insert = %d, %v", idx, err)
	}
	// Negative index errors.
	if _, err := arr.Set(testCtx, -1, "bad"); err == nil {
		t.Fatal("expected negative index error")
	}
}

func TestE2E_MapCacheNative_Full(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-mcn")
	t.Cleanup(func() { interopCleanup(t, name) })
	m := client.GetMapCacheNative(name)

	prev, err := m.PutWithTTL(testCtx, "k", "v0", time.Minute)
	if err != nil {
		if skipNativeTTL(t, err) {
			return
		}
		t.Fatal(err)
	}
	if prev != nil {
		t.Fatalf("prev = %v", prev)
	}
	prev, err = m.PutWithTTL(testCtx, "k", "v1", time.Minute)
	if err != nil || prev != "v0" {
		t.Fatalf("Put overwrite prev = %v, %v", prev, err)
	}
	ok, err := m.FastPutWithTTL(testCtx, "k2", "x", time.Minute)
	if err != nil || !ok {
		t.Fatalf("FastPutWithTTL = %v, %v", ok, err)
	}
	ok, err = m.FastPutWithTTL(testCtx, "k2", "y", time.Minute)
	if err != nil || ok {
		t.Fatalf("FastPutWithTTL update = %v, %v", ok, err)
	}
	old, err := m.PutIfAbsentWithTTL(testCtx, "k2", "z", time.Minute)
	if err != nil || old != "y" {
		t.Fatalf("PutIfAbsentWithTTL existing = %v, %v", old, err)
	}
	ok, err = m.FastPutIfAbsentWithTTL(testCtx, "k3", "n", time.Minute)
	if err != nil || !ok {
		t.Fatalf("FastPutIfAbsent new = %v, %v", ok, err)
	}
	until := time.Now().Add(2 * time.Minute)
	if _, err := m.PutUntil(testCtx, "k4", "u", until); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.ExpireEntry(testCtx, "k3", 30*time.Second); err != nil || !ok {
		t.Fatalf("ExpireEntry = %v, %v", ok, err)
	}
	// Already has TTL — NX should fail.
	if ok, err := m.ExpireEntryIfNotSet(testCtx, "k3", time.Hour); err != nil || ok {
		t.Fatalf("ExpireEntryIfNotSet = %v, %v", ok, err)
	}
	if err := m.Put(testCtx, "bare", "no-ttl"); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.ExpireEntryIfNotSet(testCtx, "bare", time.Minute); err != nil || !ok {
		t.Fatalf("ExpireEntryIfNotSet bare = %v, %v", ok, err)
	}
	ttl, err := m.RemainTTLForKey(testCtx, "k")
	if err != nil || ttl <= 0 {
		t.Fatalf("RemainTTLForKey = %v, %v", ttl, err)
	}
	if _, err := m.PutWithTTL(testCtx, "neg", "x", -time.Second); err == nil {
		t.Fatal("expected negative ttl error")
	}
}

func TestE2E_MultimapCache_Surface(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-mmc")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+name+"*") })
	cache := client.GetSetMultimapCache(name)

	if _, err := cache.PutAll(testCtx, "k1", "a", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Put(testCtx, "k2", "c"); err != nil {
		t.Fatal(err)
	}
	if ok, err := cache.ExpireKey(testCtx, "k1", time.Minute); err != nil || !ok {
		t.Fatalf("ExpireKey = %v, %v", ok, err)
	}
	if ok, err := cache.ContainsKey(testCtx, "k1"); err != nil || !ok {
		t.Fatalf("ContainsKey = %v, %v", ok, err)
	}
	if ok, err := cache.ContainsEntry(testCtx, "k1", "a"); err != nil || !ok {
		t.Fatalf("ContainsEntry = %v, %v", ok, err)
	}
	if ok, err := cache.ContainsValue(testCtx, "c"); err != nil || !ok {
		t.Fatalf("ContainsValue = %v, %v", ok, err)
	}
	vals, err := cache.Values(testCtx)
	if err != nil || len(vals) < 3 {
		t.Fatalf("Values = %v, %v", vals, err)
	}
	ents, err := cache.Entries(testCtx)
	if err != nil || len(ents) < 3 {
		t.Fatalf("Entries = %v, %v", ents, err)
	}
	keys, err := cache.KeySet(testCtx)
	if err != nil || len(keys) != 2 {
		t.Fatalf("KeySet = %v, %v", keys, err)
	}
	if n, err := cache.KeySize(testCtx); err != nil || n != 2 {
		t.Fatalf("KeySize = %d, %v", n, err)
	}
	if n, err := cache.Size(testCtx); err != nil || n < 3 {
		t.Fatalf("Size = %d, %v", n, err)
	}
	if empty, err := cache.IsEmpty(testCtx); err != nil || empty {
		t.Fatalf("IsEmpty = %v, %v", empty, err)
	}
	prev, err := cache.ReplaceValues(testCtx, "k1", "z")
	if err != nil || len(prev) < 1 {
		t.Fatalf("ReplaceValues = %v, %v", prev, err)
	}
	if keys, err := cache.ReadAllKeySet(testCtx); err != nil || len(keys) != 2 {
		t.Fatalf("ReadAllKeySet = %v, %v", keys, err)
	}
	if ok, err := cache.Expire(testCtx, time.Minute); err != nil || !ok {
		t.Fatalf("Expire = %v, %v", ok, err)
	}
	if ok, err := cache.RemoveAll(testCtx, "k2"); err != nil || !ok {
		t.Fatalf("RemoveAll = %v, %v", ok, err)
	}
	if ok, err := cache.RemoveAll(testCtx, "k2"); err != nil || ok {
		t.Fatalf("RemoveAll missing = %v, %v", ok, err)
	}
	if old, err := cache.ReplaceValues(testCtx, "k1"); err != nil || len(old) != 1 {
		t.Fatalf("ReplaceValues empty = %v, %v", old, err)
	}
	if empty, err := cache.IsEmpty(testCtx); err != nil || !empty {
		t.Fatalf("IsEmpty after removals = %v, %v", empty, err)
	}
	if _, err := cache.Put(testCtx, "delete", "value"); err != nil {
		t.Fatal(err)
	}
	if err := cache.Delete(testCtx); err != nil {
		t.Fatal(err)
	}
	listName := uniqueKey(t, "e2e-mmc-list")
	t.Cleanup(func() { interopCleanupPattern(t, "*"+listName+"*") })
	lc := client.GetListMultimapCache(listName)
	if _, err := lc.PutAll(testCtx, "lk", "1", "2", "1"); err != nil {
		t.Fatal(err)
	}
	got, err := lc.Get(testCtx, "lk")
	if err != nil || len(got) != 3 {
		t.Fatalf("list Get = %v, %v", got, err)
	}
	native := client.GetListMultimapCacheNative(uniqueKey(t, "e2e-mmcn"))
	t.Cleanup(func() { interopCleanupPattern(t, "*"+native.Name()+"*") })
	if _, err := native.Put(testCtx, "nk", "nv"); err != nil {
		t.Fatal(err)
	}
	if ok, err := native.ExpireKey(testCtx, "nk", time.Minute); err != nil {
		if skipNativeTTL(t, err) {
			return
		}
		t.Fatal(err)
	} else if !ok {
		t.Fatal("native ExpireKey")
	}
}

func TestE2E_Probabilistic_Full(t *testing.T) {
	client := newTestClient(t)

	bf := client.GetBloomFilterNative(uniqueKey(t, "e2e-bfn"))
	t.Cleanup(func() { _ = bf.Delete(testCtx) })
	if ok, err := bf.TryInit(testCtx, 0.01, 200); err != nil {
		if skipProbabilistic(t, err) {
			return
		}
		t.Fatal(err)
	} else if !ok {
		t.Fatal("BF TryInit")
	}
	if ok, err := bf.TryInit(testCtx, 0.01, 200); err != nil || ok {
		t.Fatalf("BF duplicate TryInit = %v, %v", ok, err)
	}
	flags, err := bf.AddAll(testCtx, "a", "b", "a")
	if err != nil || len(flags) != 3 {
		t.Fatalf("AddAll = %v, %v", flags, err)
	}
	has, err := bf.ContainsAll(testCtx, "a", "missing")
	if err != nil || len(has) != 2 || !has[0] || has[1] {
		t.Fatalf("ContainsAll = %v, %v", has, err)
	}
	if n, err := bf.Card(testCtx); err != nil || n < 1 {
		t.Fatalf("Card = %d, %v", n, err)
	}

	cfName := uniqueKey(t, "e2e-cf")
	t.Cleanup(func() { interopCleanup(t, cfName) })
	cf := client.GetCuckooFilter(cfName)
	if _, err := cf.TryInit(testCtx, 200); err != nil {
		t.Fatal(err)
	}
	if ok, err := cf.TryInit(testCtx, 200); err != nil || ok {
		t.Fatalf("CF duplicate TryInit = %v, %v", ok, err)
	}
	if _, err := cf.Add(testCtx, "x"); err != nil {
		t.Fatal(err)
	}
	if ok, err := cf.AddIfAbsent(testCtx, "x"); err != nil || ok {
		t.Fatalf("AddIfAbsent existing = %v, %v", ok, err)
	}
	if ok, err := cf.Contains(testCtx, "x"); err != nil || !ok {
		t.Fatalf("Contains = %v, %v", ok, err)
	}
	if n, err := cf.Count(testCtx, "x"); err != nil || n < 1 {
		t.Fatalf("Count = %d, %v", n, err)
	}
	if ok, err := cf.Delete(testCtx, "x"); err != nil || !ok {
		t.Fatalf("Delete = %v, %v", ok, err)
	}

	tk := client.GetTopK(uniqueKey(t, "e2e-topk"))
	t.Cleanup(func() { _ = tk.Delete(testCtx) })
	if _, err := tk.TryInit(testCtx, 3); err != nil {
		t.Fatal(err)
	}
	if ok, err := tk.TryInit(testCtx, 3); err != nil || ok {
		t.Fatalf("TopK duplicate TryInit = %v, %v", ok, err)
	}
	if _, err := tk.Add(testCtx, "a", "b", "c", "a", "a"); err != nil {
		t.Fatal(err)
	}
	q, err := tk.Query(testCtx, "a", "z")
	if err != nil || len(q) != 2 || !q[0] {
		t.Fatalf("Query = %v, %v", q, err)
	}
	if list, err := tk.List(testCtx); err != nil || len(list) == 0 {
		t.Fatalf("List = %v, %v", list, err)
	}
	if m, err := tk.ListWithCount(testCtx); err != nil || len(m) == 0 {
		t.Fatalf("ListWithCount = %v, %v", m, err)
	}

	td := client.GetTDigest(uniqueKey(t, "e2e-td"))
	t.Cleanup(func() { _ = td.Delete(testCtx) })
	if _, err := td.TryCreateWithCompression(testCtx, 100); err != nil {
		// already created path: try plain create then compression may fail
		if _, err2 := td.TryCreate(testCtx); err2 != nil {
			t.Fatal(err)
		}
	}
	if ok, err := td.TryCreate(testCtx); err != nil || ok {
		t.Fatalf("TDigest duplicate TryCreate = %v, %v", ok, err)
	}
	if err := td.Add(testCtx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10); err != nil {
		t.Fatal(err)
	}
	if qs, err := td.Quantile(testCtx, 0.5, 0.9); err != nil || len(qs) != 2 {
		t.Fatalf("Quantile = %v, %v", qs, err)
	}
	if cdf, err := td.CDF(testCtx, 5); err != nil || len(cdf) != 1 {
		t.Fatalf("CDF = %v, %v", cdf, err)
	}
	if mn, err := td.Min(testCtx); err != nil || mn > 2 {
		t.Fatalf("Min = %v, %v", mn, err)
	}
	if mx, err := td.Max(testCtx); err != nil || mx < 9 {
		t.Fatalf("Max = %v, %v", mx, err)
	}

	g := client.GetGcra(uniqueKey(t, "e2e-gcra"))
	t.Cleanup(func() { _ = g.Delete(testCtx) })
	r1, err := g.TryAcquire(testCtx, 1, 1, time.Second, 1)
	if err != nil {
		if skipProbabilistic(t, err) {
			return
		}
		t.Fatal(err)
	}
	if r1 == nil || !r1.Allowed {
		t.Fatalf("first = %#v", r1)
	}
	r2, err := g.TryAcquire(testCtx, 1, 1, time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	// burst=1: second may be denied
	_ = r2
}

func TestE2E_Gcra_Validation(t *testing.T) {
	g := newTestClient(t).GetGcra(uniqueKey(t, "e2e-gcra-validation"))
	tests := []struct {
		name            string
		maxBurst        int64
		tokensPerPeriod int64
		period          time.Duration
		tokens          int64
	}{
		{name: "negative burst", maxBurst: -1, tokensPerPeriod: 1, period: time.Second, tokens: 1},
		{name: "zero rate", maxBurst: 1, tokensPerPeriod: 0, period: time.Second, tokens: 1},
		{name: "zero period", maxBurst: 1, tokensPerPeriod: 1, period: 0, tokens: 1},
		{name: "zero tokens", maxBurst: 1, tokensPerPeriod: 1, period: time.Second, tokens: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := g.TryAcquire(
				testCtx, tc.maxBurst, tc.tokensPerPeriod, tc.period, tc.tokens,
			); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestE2E_HyperLogLog_Merge(t *testing.T) {
	client := newTestClient(t)
	a := uniqueKey(t, "e2e-hll-a")
	b := uniqueKey(t, "e2e-hll-b")
	dest := uniqueKey(t, "e2e-hll-d")
	t.Cleanup(func() { interopCleanup(t, a, b, dest) })

	ha, hb := client.GetHyperLogLog(a), client.GetHyperLogLog(b)
	if _, err := ha.AddAll(testCtx, "1", "2", "3"); err != nil {
		t.Fatal(err)
	}
	if _, err := hb.AddAll(testCtx, "3", "4", "5"); err != nil {
		t.Fatal(err)
	}
	n, err := ha.CountWith(testCtx, b)
	if err != nil || n < 4 {
		t.Fatalf("CountWith = %d, %v", n, err)
	}
	if err := ha.MergeInto(testCtx, dest, b); err != nil {
		t.Fatal(err)
	}
	hd := client.GetHyperLogLog(dest)
	if n, err := hd.Count(testCtx); err != nil || n < 4 {
		t.Fatalf("dest Count = %d, %v", n, err)
	}
	if err := ha.MergeWith(testCtx, b); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_NonReentrantLock_WaitForceTTL(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-nrl")
	t.Cleanup(func() { interopCleanup(t, name) })
	lock := client.GetNonReentrantLock(name)

	if ok, err := lock.TryLock(testCtx, "a", time.Second); err != nil || !ok {
		t.Fatalf("TryLock = %v, %v", ok, err)
	}
	if held, err := lock.IsHeldBy(testCtx, "a"); err != nil || !held {
		t.Fatalf("IsHeldBy = %v, %v", held, err)
	}
	if locked, err := lock.IsLocked(testCtx); err != nil || !locked {
		t.Fatalf("IsLocked = %v, %v", locked, err)
	}
	ttl, err := lock.RemainTimeToLive(testCtx)
	if err != nil || ttl <= 0 {
		t.Fatalf("RemainTTL = %v, %v", ttl, err)
	}
	ok, err := lock.TryLockWait(testCtx, "b", 80*time.Millisecond, time.Second)
	if err != nil || ok {
		t.Fatalf("TryLockWait contended = %v, %v", ok, err)
	}
	forced, err := lock.ForceUnlock(testCtx)
	if err != nil || !forced {
		t.Fatalf("ForceUnlock = %v, %v", forced, err)
	}
	ctx, cancel := context.WithTimeout(testCtx, time.Second)
	defer cancel()
	if err := lock.Lock(ctx, "b", 0); err != nil {
		t.Fatal(err)
	}
	if err := lock.Unlock(testCtx, "b"); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_ClientSideCaching_Factories(t *testing.T) {
	parent := newTestClient(t)
	csc, err := parent.GetClientSideCachingWithOptions(&redi.ClientSideCachingOptions{
		MaxEntries:     64,
		MaxMemoryBytes: 1 << 20,
		DrainInterval:  time.Millisecond,
		MaxStaleness:   time.Second,
	})
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(func() { _ = csc.Destroy() })

	prefix := uniqueKey(t, "e2e-csc")
	t.Cleanup(func() { interopCleanupPattern(t, prefix+"*") })

	_ = csc.GetMap(prefix + ":m")
	_ = csc.GetSet(prefix + ":s")
	_ = csc.GetList(prefix + ":l")
	_ = csc.GetQueue(prefix + ":q")
	_ = csc.GetDeque(prefix + ":d")
	_ = csc.GetBlockingQueue(prefix + ":bq")
	_ = csc.GetBlockingDeque(prefix + ":bd")
	_ = csc.GetScoredSortedSet(prefix + ":z")
	_ = csc.GetStream(prefix + ":st")
	_ = csc.GetGeo(prefix + ":g")
	al := csc.GetAtomicLong(prefix + ":al")
	if _, err := al.AddAndGet(testCtx, 3); err != nil {
		t.Fatal(err)
	}
	ad := csc.GetAtomicDouble(prefix + ":ad")
	if _, err := ad.AddAndGet(testCtx, 1.5); err != nil {
		t.Fatal(err)
	}
	bs := csc.GetBitSet(prefix + ":bs")
	if _, err := bs.Set(testCtx, 1, true); err != nil {
		t.Fatal(err)
	}
	hll := csc.GetHyperLogLog(prefix + ":h")
	if _, err := hll.Add(testCtx, "x"); err != nil {
		t.Fatal(err)
	}
	b := csc.GetBucket(prefix + ":b")
	if err := b.Set(testCtx, "ok"); err != nil {
		t.Fatal(err)
	}
	if v, err := b.Get(testCtx); err != nil || v != "ok" {
		t.Fatalf("Get = %v, %v", v, err)
	}
}

func TestE2E_Atomic_GetAndFamily(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "e2e-atom")
	t.Cleanup(func() { interopCleanup(t, name, name+"d") })

	al := client.GetAtomicLong(name)
	if _, err := al.Set(testCtx, 10); err != nil {
		t.Fatal(err)
	}
	if v, err := al.GetAndAdd(testCtx, 5); err != nil || v != 10 {
		t.Fatalf("GetAndAdd = %d, %v", v, err)
	}
	if v, err := al.GetAndIncrement(testCtx); err != nil || v != 15 {
		t.Fatalf("GetAndIncrement = %d, %v", v, err)
	}
	if v, err := al.GetAndDecrement(testCtx); err != nil || v != 16 {
		t.Fatalf("GetAndDecrement = %d, %v", v, err)
	}
	if v, err := al.GetAndSet(testCtx, 1); err != nil || v != 15 {
		t.Fatalf("GetAndSet = %d, %v", v, err)
	}
	if v, err := al.GetAndDelete(testCtx); err != nil || v != 1 {
		t.Fatalf("GetAndDelete = %d, %v", v, err)
	}

	ad := client.GetAtomicDouble(name + "d")
	if _, err := ad.Set(testCtx, 2.5); err != nil {
		t.Fatal(err)
	}
	if v, err := ad.GetAndSet(testCtx, 3.5); err != nil || v != 2.5 {
		t.Fatalf("Double GetAndSet = %v, %v", v, err)
	}
	if v, err := ad.DecrementAndGet(testCtx); err != nil || v != 2.5 {
		t.Fatalf("DecrementAndGet = %v, %v", v, err)
	}
	if v, err := ad.GetAndDelete(testCtx); err != nil || v != 2.5 {
		t.Fatalf("Double GetAndDelete = %v, %v", v, err)
	}
}
