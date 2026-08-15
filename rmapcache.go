package redi

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrIDAlloc is returned when the id generator fails to allocate a range.
var ErrIDAlloc = errors.New("redi: id range allocation failed")

// RMapCache is an RMap with per-entry TTL and maxIdle, wire-compatible with
// Redisson's RedissonMapCache:
//
//	HASH {name}                              field -> packed value
//	ZSET  redisson__timeout__set:{name}      expiry deadlines (score = ts ms)
//	ZSET  redisson__idle__set:{name}         idle deadlines (score = ts ms)
//
// The packed value format is Redisson's struct layout:
// little-endian float64 (maxIdle ms) + little-endian uint64 (codec length)
// + codec bytes.
//
// Expired entries are purged lazily on access and by an optional background
// sweeper (StartAutoEviction). Entry events (created/updated/removed/expired)
// are published on Redisson-compatible channels and can be consumed with
// AddListener.
type RMapCache struct {
	*RMap
	ttlKey  string
	idleKey string

	evictOnce sync.Once

	mu        sync.Mutex
	listeners map[string]map[int]func(kind, key, value, oldValue any) // kind -> id -> cb
	nextID    int
	sub       *redis.PubSub
	subWG     sync.WaitGroup
}

// MapCache event kinds (channel suffixes).
const (
	EventCreated = "created"
	EventUpdated = "updated"
	EventRemoved = "removed"
	EventExpired = "expired"
)

func newRMapCache(c *Client, name string) *RMapCache {
	return &RMapCache{
		RMap:      newRMap(c, name),
		listeners: make(map[string]map[int]func(kind, key, value, oldValue any)),
		ttlKey:    prefixName("redisson__timeout__set", name),
		idleKey:   prefixName("redisson__idle__set", name),
	}
}

// packValue packs codec bytes in the Redisson struct format.
func packValue(codecValue string, maxIdleDelta float64) string {
	buf := make([]byte, 16+len(codecValue))
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(maxIdleDelta))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(codecValue)))
	copy(buf[16:], codecValue)
	return string(buf)
}

// unpackValue splits a packed value; ok reports a valid struct header.
func unpackValue(raw string) (maxIdle float64, codecValue string, ok bool) {
	if len(raw) < 16 {
		return 0, raw, false
	}
	length := binary.LittleEndian.Uint64([]byte(raw[8:16]))
	if int(length) != len(raw)-16 {
		return 0, raw, false
	}
	maxIdle = math.Float64frombits(binary.LittleEndian.Uint64([]byte(raw[0:8])))
	return maxIdle, raw[16:], true
}

var mapCacheEvictScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local out = {}
local function purge(set_key)
    local expired = redis.call('zrangebyscore', set_key, 0, now)
    if #expired > 0 then
        for i = 1, #expired do
            local v = redis.call('hget', KEYS[1], expired[i])
            if v ~= false then
                out[#out + 1] = expired[i]
                out[#out + 1] = v
            end
        end
        redis.call('hdel', KEYS[1], unpack(expired))
        redis.call('zrem', KEYS[2], unpack(expired))
        redis.call('zrem', KEYS[3], unpack(expired))
    end
end
purge(KEYS[2])
purge(KEYS[3])
return out
`)

var mapCachePutScript = redis.NewScript(`
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
if tonumber(ARGV[3]) > 0 then
    redis.call('zadd', KEYS[2], ARGV[3], ARGV[1])
else
    redis.call('zrem', KEYS[2], ARGV[1])
end
if tonumber(ARGV[4]) > 0 then
    redis.call('zadd', KEYS[3], ARGV[4], ARGV[1])
else
    redis.call('zrem', KEYS[3], ARGV[1])
end
return 1
`)

var mapCachePutIfAbsentScript = redis.NewScript(`
if (redis.call('hexists', KEYS[1], ARGV[1]) == 1) then
    return 0
end
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
if tonumber(ARGV[3]) > 0 then
    redis.call('zadd', KEYS[2], ARGV[3], ARGV[1])
else
    redis.call('zrem', KEYS[2], ARGV[1])
end
if tonumber(ARGV[4]) > 0 then
    redis.call('zadd', KEYS[3], ARGV[4], ARGV[1])
else
    redis.call('zrem', KEYS[3], ARGV[1])
end
return 1
`)

var mapCacheRemoveScript = redis.NewScript(`
redis.call('hdel', KEYS[1], ARGV[1])
redis.call('zrem', KEYS[2], ARGV[1])
redis.call('zrem', KEYS[3], ARGV[1])
return 1
`)

// EvictExpired purges expired entries now, returning how many were removed.
func (m *RMapCache) EvictExpired(ctx context.Context) (int, error) {
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	res, err := mapCacheEvictScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey}, now).Result()
	if err != nil {
		return 0, err
	}
	flat, ok := res.([]any)
	if !ok {
		return 0, nil
	}
	for i := 0; i+1 < len(flat); i += 2 {
		ek, _ := flat[i].(string)
		raw, _ := flat[i+1].(string)
		m.publishEvent(ctx, EventExpired, ek, codecPart(raw))
	}
	return len(flat) / 2, nil
}

// codecPart strips the struct-pack header, leaving the codec (JSON) part.
func codecPart(raw string) string {
	if _, rest, ok := unpackValue(raw); ok {
		return rest
	}
	return raw
}

// Put sets field with optional per-entry ttl and maxIdle (0 = disabled).
// Publishes a created or updated entry event.
func (m *RMapCache) Put(ctx context.Context, field string, value any, ttl, maxIdle time.Duration) error {
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return err
	}
	if _, err := m.EvictExpired(ctx); err != nil {
		return err
	}
	ek := encodeKey(m.c.codec, field)
	prev, err := m.rc().HGet(ctx, m.name, ek).Result()
	isNew := err == redis.Nil
	if err != nil && !isNew {
		return err
	}
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return err
	}
	expiry, idleExpiry := int64(0), int64(0)
	if ttl > 0 {
		expiry = now + ttl.Milliseconds()
	}
	if maxIdle > 0 {
		idleExpiry = now + maxIdle.Milliseconds()
	}
	if _, err := mapCachePutScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey},
		ek, packValue(enc, float64(maxIdle.Milliseconds())), expiry, idleExpiry).Result(); err != nil {
		return err
	}
	if isNew {
		m.publishEvent(ctx, EventCreated, ek, enc)
	} else {
		m.publishEvent(ctx, EventUpdated, ek, enc, codecPart(prev))
	}
	return nil
}

// PutIfAbsent sets field only when absent, with optional ttl/maxIdle.
// Returns true when set.
func (m *RMapCache) PutIfAbsent(ctx context.Context, field string, value any, ttl, maxIdle time.Duration) (bool, error) {
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	if _, err := m.EvictExpired(ctx); err != nil {
		return false, err
	}
	ek := encodeKey(m.c.codec, field)
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	expiry, idleExpiry := int64(0), int64(0)
	if ttl > 0 {
		expiry = now + ttl.Milliseconds()
	}
	if maxIdle > 0 {
		idleExpiry = now + maxIdle.Milliseconds()
	}
	n, err := mapCachePutIfAbsentScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey},
		ek, packValue(enc, float64(maxIdle.Milliseconds())), expiry, idleExpiry).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Get returns the value for field (nil when absent or expired). Access
// refreshes the maxIdle deadline.
func (m *RMapCache) Get(ctx context.Context, field string) (any, error) {
	if _, err := m.EvictExpired(ctx); err != nil {
		return nil, err
	}
	ek := encodeKey(m.c.codec, field)
	raw, err := m.rc().HGet(ctx, m.name, ek).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	maxIdle, codecValue, ok := unpackValue(raw)
	if !ok {
		return m.c.codec.Decode(raw)
	}
	if maxIdle > 0 {
		now, err := m.serverNowMs(ctx)
		if err != nil {
			return nil, err
		}
		if err := m.rc().ZAdd(ctx, m.idleKey, redis.Z{
			Score:  float64(now + int64(maxIdle)),
			Member: ek,
		}).Err(); err != nil {
			return nil, err
		}
	}
	return m.c.codec.Decode(codecValue)
}

// Remove deletes field and its deadline entries, publishing a removed event.
func (m *RMapCache) Remove(ctx context.Context, field string) error {
	ek := encodeKey(m.c.codec, field)
	prev, err := m.rc().HGet(ctx, m.name, ek).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if _, err := mapCacheRemoveScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey}, ek).Result(); err != nil {
		return err
	}
	if err != redis.Nil {
		m.publishEvent(ctx, EventRemoved, ek, codecPart(prev))
	}
	return nil
}

// Size returns the live entry count (after eviction).
func (m *RMapCache) Size(ctx context.Context) (int64, error) {
	if _, err := m.EvictExpired(ctx); err != nil {
		return 0, err
	}
	return m.rc().HLen(ctx, m.name).Result()
}

// RemainTTLForKey returns the remaining per-entry TTL (-2 when absent).
func (m *RMapCache) RemainTTLForKey(ctx context.Context, field string) (time.Duration, error) {
	ek := encodeKey(m.c.codec, field)
	score, err := m.rc().ZScore(ctx, m.ttlKey, ek).Result()
	if err == redis.Nil {
		return -2, nil
	}
	if err != nil {
		return 0, err
	}
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	remaining := time.Duration((score - float64(now)) * float64(time.Millisecond))
	if remaining < 0 {
		return -2, nil
	}
	return remaining, nil
}

// Clear removes the map and both deadline sets.
func (m *RMapCache) Clear(ctx context.Context) error {
	return m.rc().Del(ctx, m.name, m.ttlKey, m.idleKey).Err()
}

// StartAutoEviction runs a background sweeper every interval until the
// client closes. Repeated calls are no-ops.
func (m *RMapCache) StartAutoEviction(interval time.Duration) {
	m.evictOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-m.c.ctx.Done():
					return
				case <-ticker.C:
					ctx, cancel := context.WithTimeout(m.c.ctx, 3*time.Second)
					if _, err := m.EvictExpired(ctx); err != nil {
						m.c.logf("map cache %q evict: %v", m.name, err)
					}
					cancel()
				}
			}
		}()
	})
}

// eventChannel returns the Redisson channel for an event kind:
// redisson_map_cache_{kind}:{name}.
func (m *RMapCache) eventChannel(kind string) string {
	return prefixName("redisson_map_cache_"+kind, m.name)
}

// encodeEvent packs length-prefixed segments (Redisson 4.6.1 verified):
// each segment = LE uint64 length + codec-encoded bytes.
func encodeEvent(values ...string) string {
	out := make([]byte, 0, 16*len(values))
	for _, v := range values {
		var lenBuf [8]byte
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(v)))
		out = append(out, lenBuf[:]...)
		out = append(out, v...)
	}
	return string(out)
}

// parseEvent splits length-prefixed segments.
func parseEvent(data string) []string {
	var parts []string
	raw := []byte(data)
	for i := 0; i+8 <= len(raw); {
		n := binary.LittleEndian.Uint64(raw[i : i+8])
		i += 8
		if uint64(len(raw)-i) < n {
			break
		}
		parts = append(parts, string(raw[i:i+int(n)]))
		i += int(n)
	}
	return parts
}

// publishEvent fires an entry event on the kind's channel. Publish errors
// are logged, not returned: event delivery must never fail the mutation.
func (m *RMapCache) publishEvent(ctx context.Context, kind string, values ...string) {
	if err := m.rc().Publish(ctx, m.eventChannel(kind), encodeEvent(values...)).Err(); err != nil {
		m.c.logf("map cache %q event %s: %v", m.name, kind, err)
	}
}

// AddListener registers an entry event listener for the given kind
// (EventCreated/EventUpdated/EventRemoved/EventExpired). The callback
// receives (kind, key, value, oldValue); oldValue is non-nil only for
// EventUpdated. Returns the listener id for RemoveListener.
func (m *RMapCache) AddListener(kind string, cb func(kind, key, value, oldValue any)) (int, error) {
	switch kind {
	case EventCreated, EventUpdated, EventRemoved, EventExpired:
	default:
		return 0, fmt.Errorf("redi: unknown map cache event kind %q", kind)
	}
	m.mu.Lock()
	needSub := m.sub == nil
	m.mu.Unlock()
	if needSub {
		if err := m.startListening(); err != nil {
			return 0, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listeners[kind] == nil {
		m.listeners[kind] = make(map[int]func(kind, key, value, oldValue any))
	}
	id := m.nextID
	m.nextID++
	m.listeners[kind][id] = cb
	return id, nil
}

// RemoveListener unregisters a listener.
func (m *RMapCache) RemoveListener(kind string, id int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.listeners[kind][id]
	delete(m.listeners[kind], id)
	return ok
}

// startListening subscribes to this cache's event channels.
func (m *RMapCache) startListening() error {
	kinds := []string{EventCreated, EventUpdated, EventRemoved, EventExpired}
	channels := make([]string, 0, len(kinds))
	for _, k := range kinds {
		channels = append(channels, m.eventChannel(k))
	}
	sub := m.subscribe(m.c.ctx, channels...)
	ctx, cancel := context.WithTimeout(m.c.ctx, m.c.cfg.DialTimeout)
	defer cancel()
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return err
	}
	m.sub = sub

	m.subWG.Add(1)
	go func() {
		defer m.subWG.Done()
		defer sub.Close() //nolint:errcheck
		for {
			msg, err := sub.ReceiveMessage(m.c.ctx)
			if err != nil {
				return
			}
			kind := kindFromChannel(msg.Channel, m.name)
			if kind == "" {
				continue
			}
			parts := parseEvent(msg.Payload)
			var key, val, old any
			if len(parts) > 0 {
				key, _ = m.c.codec.Decode(parts[0])
			}
			if len(parts) > 1 {
				val, _ = m.c.codec.Decode(parts[1])
			}
			if len(parts) > 2 {
				old, _ = m.c.codec.Decode(parts[2])
			}
			m.mu.Lock()
			callbacks := make([]func(kind, key, value, oldValue any), 0, len(m.listeners[kind]))
			for _, cb := range m.listeners[kind] {
				callbacks = append(callbacks, cb)
			}
			m.mu.Unlock()
			for _, cb := range callbacks {
				cb(kind, key, val, old)
			}
		}
	}()
	return nil
}

// kindFromChannel extracts the event kind from a
// redisson_map_cache_{kind}:{name} channel.
func kindFromChannel(channel, name string) string {
	const p = "redisson_map_cache_"
	if !strings.HasPrefix(channel, p) {
		return ""
	}
	rest := channel[len(p):]
	if idx := strings.Index(rest, ":"); idx >= 0 {
		return rest[:idx]
	}
	return ""
}
