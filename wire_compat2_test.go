package redi_test

import (
	"math"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// wire_compat2_test.go: contract assertions for the structures whose wire
// formats are otherwise only guarded by the Java interop suite (which
// skips on CI, where no JVM exists). These run everywhere Redis does.

func TestWire_RReadWriteLockLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-rw")
	rw := client.GetReadWriteLock(name)
	t.Cleanup(func() { interopCleanup(t, name) })
	rc := rawClient(t)

	if err := rw.ReadLock().Lock(testCtx, "uuid-r:1", time.Minute); err != nil {
		t.Fatal(err)
	}
	// Single HASH; mode field + plain read entry (no suffix).
	typ, _ := rc.Type(testCtx, name).Result()
	if typ != "hash" {
		t.Fatalf("rwlock key type = %q", typ)
	}
	mode, _ := rc.HGet(testCtx, name, "mode").Result()
	if mode != "read" {
		t.Fatalf("mode = %q, want read", mode)
	}
	if ok, _ := rc.HExists(testCtx, name, "uuid-r:1").Result(); !ok {
		t.Fatal("read entry must be the bare holder id")
	}
	_ = rw.ReadLock().Unlock(testCtx, "uuid-r:1")

	// Write entry carries the ":write" suffix and flips the mode.
	if err := rw.WriteLock().Lock(testCtx, "uuid-w:9", time.Minute); err != nil {
		t.Fatal(err)
	}
	mode, _ = rc.HGet(testCtx, name, "mode").Result()
	if mode != "write" {
		t.Fatalf("mode after write = %q", mode)
	}
	if ok, _ := rc.HExists(testCtx, name, "uuid-w:9:write").Result(); !ok {
		t.Fatal("write entry must be {id}:write")
	}
}

func TestWire_SemaphoreAndLatchKeys(t *testing.T) {
	client := newTestClient(t)
	semName := uniqueKey(t, "wire-sem")
	latchName := uniqueKey(t, "wire-latch")
	t.Cleanup(func() { interopCleanup(t, semName, latchName, semName+":total") })
	rc := rawClient(t)

	if _, err := client.GetSemaphore(semName).TrySetPermits(testCtx, 3); err != nil {
		t.Fatal(err)
	}
	v, _ := rc.Get(testCtx, semName).Result()
	if v != "3" {
		t.Fatalf("semaphore counter = %q, want plain 3 (bare name)", v)
	}

	latch := client.GetCountDownLatch(latchName)
	if _, err := latch.TrySetCount(testCtx, 1); err != nil {
		t.Fatal(err)
	}
	v, _ = rc.Get(testCtx, latchName).Result()
	if v != "1" {
		t.Fatalf("latch counter = %q", v)
	}
	// Channel name is the Redisson literal (double underscores, braces).
	// The zero-count publish fires when the count REACHES zero (2->1
	// publishes nothing). Wait for the subscription ack first so the
	// publish cannot race past an unregistered subscriber.
	sub := rc.Subscribe(testCtx, "redisson_countdownlatch__channel__{"+latchName+"}")
	defer sub.Close() //nolint:errcheck // test teardown
	if _, err := sub.ReceiveTimeout(testCtx, 2*time.Second); err != nil {
		t.Fatal("subscribe ack:", err)
	}
	if err := latch.CountDown(testCtx); err != nil {
		t.Fatal(err)
	}
	msg, err := nextMessage(sub)
	if err != nil {
		t.Fatal("no zero-count message:", err)
	}
	if pm, ok := msg.(*redis.Message); !ok || pm.Payload != "0" {
		t.Fatalf("latch payload = %#v, want message 0", msg)
	}
}

func TestWire_PermitExpirableSemaphoreLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-pes")
	s := client.GetPermitExpirableSemaphore(name)
	t.Cleanup(func() { interopCleanup(t, name, "{"+name+"}:timeout") })
	rc := rawClient(t)

	if _, err := s.TrySetPermits(testCtx, 2); err != nil {
		t.Fatal(err)
	}
	pid, err := s.TryAcquire(testCtx, time.Hour)
	if err != nil || pid == "" {
		t.Fatal(err)
	}

	// Permit lives in the {name}:timeout ZSET with an absolute-expiry score.
	typ, _ := rc.Type(testCtx, "{"+name+"}:timeout").Result()
	if typ != "zset" {
		t.Fatalf("timeout key type = %q, want zset", typ)
	}
	n, _ := rc.ZCard(testCtx, "{"+name+"}:timeout").Result()
	if n != 1 {
		t.Fatalf("timeout zcard = %d", n)
	}
	score, err := rc.ZScore(testCtx, "{"+name+"}:timeout", pid).Result()
	if err != nil {
		t.Fatalf("permit id %q not the zset member: %v", pid, err)
	}
	now := time.Now().Add(time.Hour).UnixMilli()
	lo := float64(now - 60_000)
	hi := float64(now + 60_000)
	if score < lo || score > hi {
		t.Fatalf("permit score = %v, want ~now+1h", score)
	}
}

func TestWire_RStreamFieldM(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-stream")
	s := client.GetStream(name)
	t.Cleanup(func() { interopCleanup(t, name) })
	rc := rawClient(t)

	if _, err := s.Add(testCtx, map[string]any{"payload": "x"}); err != nil {
		t.Fatal(err)
	}
	// Redisson's stream entries carry the codec value under the literal
	// field name "m" (verified in RedissonReliableTopic.publishAsync's
	// xadd and RStream's convention).
	msgs, err := rc.XRange(testCtx, name, "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("xrange = %v, %v", msgs, err)
	}
	// Stream field NAMES are codec-encoded too (Redisson RStream encodes
	// both sides - same convention as RMap fields).
	fv, ok := msgs[0].Values[`"payload"`]
	if !ok || fv != `"x"` {
		t.Fatalf("stream fields = %#v, want JSON-encoded name and value", msgs[0].Values)
	}
}

func TestWire_ReliableTopicLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-rtopic")
	topic := client.GetReliableTopic(name)
	t.Cleanup(func() {
		interopCleanupPattern(t, "{"+name+"}*")
		interopCleanup(t, name)
	})
	rc := rawClient(t)

	id, err := topic.Subscribe(func(any) {})
	if err != nil {
		t.Fatal(err)
	}
	defer topic.Unsubscribe(id)

	if _, err := topic.Publish(testCtx, "hello"); err != nil {
		t.Fatal(err)
	}

	// Messages sit in the stream under field "m" with the codec payload.
	msgs, err := rc.XRange(testCtx, name, "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("stream = %v, %v", msgs, err)
	}
	if v, ok := msgs[0].Values["m"].(string); !ok || v != `"hello"` {
		t.Fatalf("field m = %#v", msgs[0].Values["m"])
	}

	// ONE consumer group per subscriber, named by subscriber id.
	groups, err := rc.XInfoGroups(testCtx, name).Result()
	if err != nil || len(groups) != 1 {
		t.Fatalf("groups = %v, %v", groups, err)
	}
	if groups[0].Name != id {
		t.Fatalf("group name = %q, want subscriber id %q", groups[0].Name, id)
	}
	// Liveness entry in the timeout zset.
	n, _ := rc.ZCard(testCtx, "{"+name+"}:timeout").Result()
	if n != 1 {
		t.Fatalf("timeout zset = %d, want 1", n)
	}
}

func TestWire_LongAdderKeys(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-adder")
	a := client.GetLongAdder(name)
	defer a.Destroy()
	t.Cleanup(func() { interopCleanupPattern(t, "{"+name+"}:*") })
	rc := rawClient(t)

	a.Add(7)
	sum, err := a.Sum(testCtx)
	if err != nil || sum != 7 {
		t.Fatalf("Sum = %d, %v", sum, err)
	}

	// The topic channel exists under the Redisson companion name (verified
	// by publishing: our own subscriber must still be counted).
	n, err := rc.Publish(testCtx, "{"+name+"}:adder-topic", "1:probe").Result()
	if err != nil || n != 1 {
		t.Fatalf("adder topic receivers = %d, %v; want 1 (own subscription)", n, err)
	}
	time.Sleep(50 * time.Millisecond)
	// The probe request flushed our buffer and released the barrier.
	sem, _ := rc.Get(testCtx, "{"+name+"}:probe:semaphore").Result()
	if sem != "1" {
		t.Fatalf("barrier permits = %q, want 1", sem)
	}
	counter, _ := rc.Get(testCtx, "{"+name+"}:probe:counter").Result()
	if counter != "7" {
		t.Fatalf("flush counter = %q, want 7 (our buffer flushed by request)", counter)
	}
	// Sum() must have cleaned up its own coordination keys.
	time.Sleep(50 * time.Millisecond)
	interopCleanupPattern(t, "{"+name+"}:*")
}

func TestWire_RBitSetBytes(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-bits")
	b := client.GetBitSet(name)
	t.Cleanup(func() { interopCleanup(t, name) })
	rc := rawClient(t)

	if _, err := b.Set(testCtx, 0, true); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Set(testCtx, 9, true); err != nil {
		t.Fatal(err)
	}
	// Raw bitmap must be MSB-first Redis bytes (bit0 -> 0x80), identical
	// to what Java RedissonBitSet.toByteArray produces.
	raw, err := rc.Get(testCtx, name).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 2 || raw[0] != 0x80 || raw[1] != 0x40 {
		t.Fatalf("bitmap bytes = % x, want 80 40 (MSB-first, no reversal)", raw)
	}
}

func TestWire_LocalCachedMapDataIsRMapFormat(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-lcm")
	m := client.GetLocalCachedMap(name)
	t.Cleanup(func() { interopCleanup(t, name, name+":inval") })
	rc := rawClient(t)

	if err := m.Put(testCtx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	// The Redis data layer is the plain RMap wire format: JSON field and
	// JSON value (Java-readable).
	raw, err := rc.HGet(testCtx, name, `"k"`).Result()
	if err != nil {
		t.Fatalf("field not stored as JSON: %v", err)
	}
	if raw != `"v"` {
		t.Fatalf("value = %q, want JSON-encoded", raw)
	}
}

func TestWire_RSetCacheLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-sc")
	idleKey := "redisson__idle__set:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, idleKey) })
	s := client.GetSetCache(name)
	rc := rawClient(t)

	if _, err := s.Add(testCtx, "forever", 0, 0); err != nil {
		t.Fatal(err)
	}
	score, err := rc.ZScore(testCtx, name, `"forever"`).Result()
	if err != nil {
		t.Fatal(err)
	}
	// Redis ZSET scores are float64; MaxInt64 is the no-TTL sentinel (lossy but stable).
	if math.Abs(score-float64(math.MaxInt64)) > 1 {
		t.Fatalf("no-TTL score = %v, want ~MaxInt64", score)
	}

	if _, err := s.Add(testCtx, "idleish", time.Hour, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	idle, err := rc.ZScore(testCtx, idleKey, `"idleish"`).Result()
	if err != nil {
		t.Fatalf("idle companion missing: %v", err)
	}
	if idle != float64((5 * time.Second).Milliseconds()) {
		t.Fatalf("idle score = %v, want 5000 (duration ms)", idle)
	}
}

func TestWire_RRingBufferLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-rb")
	settings := "redisson_rb:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, settings) })
	r := client.GetRingBuffer(name)
	rc := rawClient(t)

	ok, err := r.TrySetCapacity(testCtx, 3)
	if err != nil || !ok {
		t.Fatalf("TrySetCapacity = %v, %v", ok, err)
	}
	capRaw, err := rc.Get(testCtx, settings).Result()
	if err != nil || capRaw != "3" {
		t.Fatalf("capacity key %q = %q, %v; want decimal \"3\"", settings, capRaw, err)
	}
	if err := r.Add(testCtx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(testCtx, "b"); err != nil {
		t.Fatal(err)
	}
	typ, err := rc.Type(testCtx, name).Result()
	if err != nil || typ != "list" {
		t.Fatalf("ring type = %v, %v; want list", typ, err)
	}
	n, _ := rc.LLen(testCtx, name).Result()
	if n != 2 {
		t.Fatalf("llen = %d, want 2", n)
	}
}

func TestWire_RIdGeneratorLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-id")
	allocKey := "{" + name + "}:allocation"
	t.Cleanup(func() { interopCleanup(t, name, allocKey) })
	g := client.GetIdGenerator(name)
	rc := rawClient(t)

	ok, err := g.TryInit(testCtx, 100, 50)
	if err != nil || !ok {
		t.Fatalf("TryInit = %v, %v", ok, err)
	}
	cur, err := rc.Get(testCtx, name).Result()
	if err != nil || cur != "100" {
		t.Fatalf("counter = %q, %v; want \"100\"", cur, err)
	}
	alloc, err := rc.Get(testCtx, allocKey).Result()
	if err != nil || alloc != "50" {
		t.Fatalf("allocation = %q, %v; want \"50\"", alloc, err)
	}
}

func TestWire_RSetMultimapLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-mm")
	t.Cleanup(func() {
		interopCleanup(t, name)
		interopCleanupPattern(t, "{"+name+"}:*")
	})
	mm := client.GetSetMultimap(name)
	rc := rawClient(t)

	if _, err := mm.Put(testCtx, "lang", "go"); err != nil {
		t.Fatal(err)
	}
	// HASH field = codec(key); value = HighwayHash-128 base64 id.
	id, err := rc.HGet(testCtx, name, `"lang"`).Result()
	if err != nil || id == "" {
		t.Fatalf("internal id missing: %q, %v", id, err)
	}
	coll := "{" + name + "}:" + id
	typ, err := rc.Type(testCtx, coll).Result()
	if err != nil || typ != "set" {
		t.Fatalf("collection %q type = %v, %v; want set", coll, typ, err)
	}
	ok, err := rc.SIsMember(testCtx, coll, `"go"`).Result()
	if err != nil || !ok {
		t.Fatalf("collection member missing: %v, %v", ok, err)
	}
}

func TestWire_RTimeSeriesLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-ts")
	ttlKey := "redisson__ts_ttl:{" + name + "}"
	seqKey := "redisson__ts_seq:{" + name + "}"
	t.Cleanup(func() { interopCleanup(t, name, ttlKey, seqKey) })
	ts := client.GetTimeSeries(name)
	rc := rawClient(t)

	stamp := time.Now().UnixMilli()
	if err := ts.Add(testCtx, stamp, "v1", "", 0); err != nil {
		t.Fatal(err)
	}
	if typ, _ := rc.Type(testCtx, name).Result(); typ != "zset" {
		t.Fatalf("series type = %q, want zset", typ)
	}
	if typ, _ := rc.Type(testCtx, ttlKey).Result(); typ != "zset" {
		t.Fatalf("ttl companion type = %q, want zset", typ)
	}
	seq, err := rc.Get(testCtx, seqKey).Result()
	if err != nil {
		t.Fatal("seq companion:", err)
	}
	// Stored as decimal; Lua zero-pads only when formatting the member id.
	if seq != "1" {
		t.Fatalf("seq = %q, want \"1\" after first Add", seq)
	}
	members, err := rc.ZRange(testCtx, name, 0, 0).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("members = %v, %v", members, err)
	}
	if members[0][0] != 4 {
		t.Fatalf("packed member type byte = %d, want 4", members[0][0])
	}
}
