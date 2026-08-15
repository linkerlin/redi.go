package redi_test

import (
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	redi "github.com/linkerlin/redi.go"
)

// wire_compat_test.go locks the Redis-side layout of every structure so the
// Redisson interop claim stays true as the code evolves (the redi.py
// test_wire_compat.py pattern). Any change that breaks these assertions is a
// wire-format break.

func rawClient(t *testing.T) *redis.Client {
	t.Helper()
	rc := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rc.Ping(testCtx).Err(); err != nil {
		rc.Close()
		t.Skip("Redis not available:", err)
	}
	t.Cleanup(func() { _ = rc.Close() })
	return rc
}

func TestWire_RLockLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-lock")
	l := client.GetRLock(name)
	if err := l.Lock(testCtx, "uuid-1:33", time.Minute); err != nil {
		t.Fatal(err)
	}
	defer l.Unlock(testCtx, "uuid-1:33") //nolint:errcheck

	rc := rawClient(t)
	typ, err := rc.Type(testCtx, name).Result()
	if err != nil || typ != "hash" {
		t.Fatalf("lock key type = %v, %v; want hash", typ, err)
	}
	n, _ := rc.HLen(testCtx, name).Result()
	if n != 1 {
		t.Fatalf("lock hash fields = %d, want 1", n)
	}
	held, _ := rc.HExists(testCtx, name, "uuid-1:33").Result()
	if !held {
		t.Fatal("lock field must be the raw clientID")
	}
	// Re-entry increments the counter in place.
	if err := l.Lock(testCtx, "uuid-1:33", time.Minute); err != nil {
		t.Fatal(err)
	}
	cnt, _ := rc.HGet(testCtx, name, "uuid-1:33").Result()
	if cnt != "2" {
		t.Fatalf("re-entrant count = %q, want 2", cnt)
	}
	_ = l.Unlock(testCtx, "uuid-1:33")
	_ = l.Unlock(testCtx, "uuid-1:33")

	// Channel name: redisson_lock__channel:{name}
	sub := rc.Subscribe(testCtx, "redisson_lock__channel:{"+name+"}")
	defer sub.Close()
	if err := l.Lock(testCtx, "uuid-9:1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := l.Unlock(testCtx, "uuid-9:1"); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.ReceiveTimeout(testCtx, 2*time.Second)
	for i := 0; err == nil && i < 3; i++ {
		if _, ok := msg.(*redis.Message); ok {
			break
		}
		msg, err = sub.ReceiveTimeout(testCtx, 2*time.Second)
	}
	if err != nil {
		t.Fatalf("no unlock message on redisson channel: %v", err)
	}
	if pm, ok := msg.(*redis.Message); !ok || pm.Payload != "0" {
		t.Fatalf("unlock payload = %#v, want message 0", msg)
	}
}

func TestWire_RMapEncoding(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-map")
	m := client.GetRMap(name)
	defer m.Clear(testCtx) //nolint:errcheck

	if err := m.Put(testCtx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	rc := rawClient(t)

	// Field and value are JSON-encoded (JsonJacksonCodec-compatible).
	raw, err := rc.HGet(testCtx, name, `"k"`).Result()
	if err != nil {
		t.Fatalf("field not stored as JSON %q: %v", `"k"`, err)
	}
	if raw != `"v"` {
		t.Fatalf("value = %q, want JSON string %q", raw, `"v"`)
	}

	// Big ints use the java.lang.Long wrapper.
	if err := m.Put(testCtx, "big", 5000000000); err != nil {
		t.Fatal(err)
	}
	raw, _ = rc.HGet(testCtx, name, `"big"`).Result()
	if raw != `["java.lang.Long",5000000000]` {
		t.Fatalf("long wrap = %q", raw)
	}

	// Compound values carry JsonJacksonCodec type ids (verified: Redisson
	// cannot read bare JSON objects — default typing requires them).
	if err := m.Put(testCtx, "obj", map[string]any{"n": 1}); err != nil {
		t.Fatal(err)
	}
	raw, _ = rc.HGet(testCtx, name, `"obj"`).Result()
	if raw != `{"@class":"java.util.LinkedHashMap","n":1}` {
		t.Fatalf("map type id = %q", raw)
	}
	if err := m.Put(testCtx, "arr", []any{1, "x"}); err != nil {
		t.Fatal(err)
	}
	raw, _ = rc.HGet(testCtx, name, `"arr"`).Result()
	if raw != `["java.util.ArrayList",[1,"x"]]` {
		t.Fatalf("list type id = %q", raw)
	}

	// Decoding tolerates Redisson @class type info.
	if err := rc.HSet(testCtx, name, `"jk"`, `{"@class":"com.x.Foo","a":1}`).Err(); err != nil {
		t.Fatal(err)
	}
	v, err := m.Get(testCtx, "jk")
	if err != nil {
		t.Fatal(err)
	}
	mv, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("decoded = %#v, want map", v)
	}
	if _, has := mv["@class"]; has {
		t.Fatal("@class not stripped")
	}
	if n, ok := mv["a"].(json.Number); !ok || n.String() != "1" {
		t.Fatalf("a = %#v, want json.Number 1", mv["a"])
	}
}

func TestWire_RAtomicLongPlainDecimal(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-along")
	a := client.GetRAtomicLong(name)
	defer a.Delete(testCtx) //nolint:errcheck

	if _, err := a.AddAndGet(testCtx, 41); err != nil {
		t.Fatal(err)
	}
	rc := rawClient(t)
	raw, err := rc.Get(testCtx, name).Result()
	if err != nil {
		t.Fatal(err)
	}
	if raw != "41" {
		t.Fatalf("raw = %q, want plain decimal 41", raw)
	}
}

func TestWire_RateLimiterLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-rate")
	r := client.GetRateLimiter(name)
	defer r.Delete(testCtx) //nolint:errcheck

	if _, err := r.TrySetRate(testCtx, redi.RateTypeOverall, 3, time.Second); err != nil {
		t.Fatal(err)
	}
	rc := rawClient(t)

	typ, _ := rc.Type(testCtx, name).Result()
	if typ != "hash" {
		t.Fatalf("config type = %v, want hash", typ)
	}
	rate, _ := rc.HGet(testCtx, name, "rate").Result()
	if rate != "3" {
		t.Fatalf("config rate = %q", rate)
	}
	rtype, _ := rc.HGet(testCtx, name, "type").Result()
	if rtype != "0" {
		t.Fatalf("config type = %q, want enum ordinal 0", rtype)
	}

	if ok, _ := r.TryAcquire(testCtx, 2); !ok {
		t.Fatal("acquire 2 failed")
	}
	permits, err := rc.ZRangeWithScores(testCtx, "{"+name+"}:permits", 0, -1).Result()
	if err != nil || len(permits) != 1 {
		t.Fatalf("permits zset = %v, %v", permits, err)
	}
	member, _ := permits[0].Member.(string)
	if len(member) != 21 || member[0] != 16 {
		t.Fatalf("permits member layout wrong: %d bytes, first=%d", len(member), member[0])
	}
	le := binary.LittleEndian.Uint32([]byte(member[17:21]))
	if le != 2 {
		t.Fatalf("permits payload = %d, want LE uint32 2", le)
	}
	val, _ := rc.Get(testCtx, "{"+name+"}:value").Result()
	if val != "1" {
		t.Fatalf("value = %q, want 1", val)
	}
}

func TestWire_DelayedQueueLayout(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-dq")
	q := client.GetDelayedQueue(name)
	defer q.Delete(testCtx) //nolint:errcheck

	if err := q.Offer(testCtx, "x", time.Hour); err != nil {
		t.Fatal(err)
	}
	rc := rawClient(t)
	n, err := rc.ZCard(testCtx, "redisson_delay_queue:{"+name+"}").Result()
	if err != nil || n != 1 {
		t.Fatalf("redisson_delay_queue zset = %d, %v; want 1", n, err)
	}
	member, _ := rc.ZRange(testCtx, "redisson_delay_queue:{"+name+"}", 0, 0).Result()
	if len(member) != 1 || member[0] != `"x"` {
		t.Fatalf("delayed member = %v, want JSON-encoded", member)
	}
}

func TestWire_BloomFilterConfig(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-bloom")
	f := client.GetBloomFilter(name)
	defer f.Delete(testCtx) //nolint:errcheck

	if _, err := f.TryInit(testCtx, 1000, 0.03); err != nil {
		t.Fatal(err)
	}
	rc := rawClient(t)
	cfg, err := rc.HGetAll(testCtx, "{"+name+"}:config").Result()
	if err != nil || len(cfg) == 0 {
		t.Fatalf("config key {name}:config missing: %v", err)
	}
	// m = -(1000 * ln 0.03)/ln2^2 = 7298.08… truncated by Java d2l → 7298
	if cfg["size"] != "7298" {
		t.Fatalf("bloom size = %q, want 7298 (Java truncation)", cfg["size"])
	}
}

func TestWire_MapCacheStructValue(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "wire-mc")
	m := client.GetMapCache(name)
	defer m.Clear(testCtx) //nolint:errcheck

	if err := m.Put(testCtx, "k", "v", time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	rc := rawClient(t)

	// Packed value: LE float64(maxIdle=0) + LE uint64(len) + JSON bytes.
	raw, err := rc.HGet(testCtx, name, `"k"`).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < 16 {
		t.Fatalf("packed value too short: %q", raw)
	}
	length := binary.LittleEndian.Uint64([]byte(raw[8:16]))
	if int(length) != len(raw)-16 {
		t.Fatalf("struct length = %d, want %d", length, len(raw)-16)
	}
	if raw[16:] != `"v"` {
		t.Fatalf("codec part = %q", raw[16:])
	}
	// Timeout set key.
	n, err := rc.ZCard(testCtx, "redisson__timeout__set:{"+name+"}").Result()
	if err != nil || n != 1 {
		t.Fatalf("timeout zset = %d, %v; want 1", n, err)
	}
}
