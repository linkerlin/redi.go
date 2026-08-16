package redi_test

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	redi "github.com/linkerlin/redi.go"
)

func TestRMapCache_Events(t *testing.T) {
	client := newTestClient(t)
	m := client.GetMapCache(uniqueKey(t, "mcev"))
	defer m.Clear(testCtx) //nolint:errcheck

	events := make(chan [4]any, 8)
	if _, err := m.AddListener("", nil); err == nil {
		t.Fatal("empty kind should be rejected")
	}

	for _, kind := range []string{redi.EventCreated, redi.EventUpdated, redi.EventRemoved, redi.EventExpired} {
		k := kind
		if _, err := m.AddListener(k, func(kind, key, value, oldValue any) {
			events <- [4]any{kind, key, value, oldValue}
		}); err != nil {
			t.Fatalf("AddListener(%s): %v", k, err)
		}
	}

	// created
	if err := m.Put(testCtx, "k", "v1", time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	// updated
	if err := m.Put(testCtx, "k", "v2", time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	// removed
	if err := m.Remove(testCtx, "k"); err != nil {
		t.Fatal(err)
	}
	// expired (a second key with short TTL)
	if err := m.Put(testCtx, "k2", "v3", 250*time.Millisecond, 0); err != nil {
		t.Fatal(err)
	}

	seen := map[string][4]any{}
	// Poll Get to drive lazy eviction of the short-TTL entry.
	if !eventual(t, 4*time.Second, func() bool {
		_, _ = m.Get(testCtx, "k2")
		select {
		case ev := <-events:
			if _, dup := seen[ev[0].(string)]; !dup {
				seen[ev[0].(string)] = ev
			}
		default:
		}
		return len(seen) == 4
	}) {
		t.Fatalf("expected 4 event kinds, got %v", keysOf(seen))
	}

	if ev := seen[redi.EventCreated]; ev[1] != "k" || ev[2] != "v1" || ev[3] != nil {
		t.Fatalf("created = %v", ev)
	}
	if ev := seen[redi.EventUpdated]; ev[1] != "k" || ev[2] != "v2" || ev[3] != "v1" {
		t.Fatalf("updated = %v", ev)
	}
	if ev := seen[redi.EventRemoved]; ev[1] != "k" || ev[2] != "v2" || ev[3] != nil {
		t.Fatalf("removed = %v", ev)
	}
	if ev := seen[redi.EventExpired]; ev[1] != "k2" || ev[2] != "v3" || ev[3] != nil {
		t.Fatalf("expired = %v", ev)
	}
}

func keysOf(m map[string][4]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestWire_MapCacheEventFormat locks the Redisson-verified event wire format:
// channel redisson_map_cache_{kind}:{name}, payload = sequence of
// (LE uint64 length + codec bytes) segments.
func TestWire_MapCacheEventFormat(t *testing.T) {
	client := newTestClient(t)
	name := uniqueKey(t, "mcwire")
	m := client.GetMapCache(name)
	defer m.Clear(testCtx) //nolint:errcheck

	rc := rawClient(t)
	sub := rc.Subscribe(testCtx,
		"redisson_map_cache_created:{"+name+"}",
		"redisson_map_cache_updated:{"+name+"}")
	defer sub.Close() //nolint:errcheck // connection teardown
	if _, err := sub.ReceiveTimeout(testCtx, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	if err := m.Put(testCtx, "k", "v1", 0, 0); err != nil {
		t.Fatal(err)
	}
	msg, err := nextMessage(sub)
	if err != nil {
		t.Fatal("no created event:", err)
	}
	assertEventPayload(t, msg, `"k"`, `"v1"`)

	if err := m.Put(testCtx, "k", "v2", 0, 0); err != nil {
		t.Fatal(err)
	}
	msg, err = nextMessage(sub)
	if err != nil {
		t.Fatal("no updated event:", err)
	}
	assertEventPayload(t, msg, `"k"`, `"v2"`, `"v1"`)
}

// nextMessage waits for the first real pub/sub message, skipping the
// subscribe acknowledgements.
func nextMessage(sub *redis.PubSub) (any, error) {
	for i := 0; i < 4; i++ {
		msg, err := sub.ReceiveTimeout(testCtx, 2*time.Second)
		if err != nil {
			return nil, err
		}
		if _, ok := msg.(*redis.Message); ok {
			return msg, nil
		}
	}
	return nil, redis.Nil
}

func assertEventPayload(t *testing.T, msg any, want ...string) {
	t.Helper()
	pm, ok := msg.(*redis.Message)
	if !ok {
		t.Fatalf("event = %#v, want *redis.Message", msg)
	}
	raw := []byte(pm.Payload)
	for _, w := range want {
		if len(raw) < 8 {
			t.Fatalf("payload truncated: %q", pm.Payload)
		}
		n := binary.LittleEndian.Uint64(raw[:8])
		raw = raw[8:]
		if uint64(len(raw)) < n || string(raw[:n]) != w {
			t.Fatalf("segment = %q, want %q (payload %q)", safePrefix(raw, n), w, pm.Payload)
		}
		raw = raw[n:]
	}
}

func safePrefix(b []byte, n uint64) string {
	if uint64(len(b)) < n {
		return string(b)
	}
	return string(b[:n])
}

// TestRReadWriteLock_Watchdog verifies ttl<=0 locks are renewed
// indefinitely, keeping competitors out.
func TestRReadWriteLock_Watchdog(t *testing.T) {
	cfg := redi.DefaultConfig()
	cfg.LockWatchdogTimeout = 1500 * time.Millisecond
	client, err := redi.NewClient(cfg)
	if err != nil {
		t.Skip("Redis not available:", err)
	}
	defer client.Close() //nolint:errcheck // test cleanup

	rw := client.GetReadWriteLock(uniqueKey(t, "rwwd"))
	w := rw.WriteLock()
	if err := w.Lock(testCtx, "holder", 0); err != nil {
		t.Fatal("write Lock:", err)
	}
	defer w.Unlock(testCtx, "holder") //nolint:errcheck

	time.Sleep(4 * time.Second) // ~3 renewal intervals

	got, err := rw.ReadLock().TryLock(testCtx, "intruder", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("watchdog failed: intruder read-locked an actively renewed write lock")
	}
}
