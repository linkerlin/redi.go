package redi

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var errNegativeTTL = errors.New("redi: ttl can't be negative")

// RMapCacheNative is a map with per-entry TTL using Redis hash-field
// expiration (HPEXPIRE / HPEXPIREAT / HPTTL), wire-compatible with
// RedissonMapCacheNative 4.6.1. Values are plain codec bytes (no MapCache
// packed header). Requires Redis ≥ 7.4 / 8.x field TTL support.
type RMapCacheNative struct {
	*RMap
}

func newRMapCacheNative(c *Client, name string) *RMapCacheNative {
	return &RMapCacheNative{RMap: newRMap(c, name)}
}

var mapCacheNativePutScript = redis.NewScript(`
local currValue = redis.call('hget', KEYS[1], ARGV[2])
redis.call('hset', KEYS[1], ARGV[2], ARGV[3])
redis.call(ARGV[4], KEYS[1], ARGV[1], 'fields', 1, ARGV[2])
return currValue
`)

var mapCacheNativeFastPutScript = redis.NewScript(`
local added = redis.call('hset', KEYS[1], ARGV[2], ARGV[3])
redis.call(ARGV[4], KEYS[1], ARGV[1], 'fields', 1, ARGV[2])
return added
`)

var mapCacheNativePutIfAbsentScript = redis.NewScript(`
local currValue = redis.call('hget', KEYS[1], ARGV[2])
if currValue ~= false then
    return currValue
end
redis.call('hset', KEYS[1], ARGV[2], ARGV[3])
redis.call(ARGV[4], KEYS[1], ARGV[1], 'fields', 1, ARGV[2])
return nil
`)

var mapCacheNativeFastPutIfAbsentScript = redis.NewScript(`
local currValue = redis.call('hget', KEYS[1], ARGV[2])
if currValue ~= false then
    return 0
end
redis.call('hset', KEYS[1], ARGV[2], ARGV[3])
redis.call(ARGV[4], KEYS[1], ARGV[1], 'fields', 1, ARGV[2])
return 1
`)

var mapCacheNativeExpireEntryScript = redis.NewScript(`
local expireSet = redis.call(ARGV[3], KEYS[1], ARGV[1], 'fields', 1, ARGV[2])
if #expireSet > 0 and expireSet[1] >= 1 then
    return 1
end
return 0
`)

var mapCacheNativeExpireEntryNXScript = redis.NewScript(`
local expireSet = redis.call(ARGV[4], KEYS[1], ARGV[2], ARGV[1], 'fields', 1, ARGV[3])
if #expireSet > 0 and expireSet[1] >= 1 then
    return 1
end
return 0
`)

func expireCmd(duration bool) string {
	if duration {
		return "hpexpire"
	}
	return "hpexpireat"
}

// PutWithTTL stores field with a relative TTL (HPEXPIRE).
func (m *RMapCacheNative) PutWithTTL(ctx context.Context, field string, value any, ttl time.Duration) (any, error) {
	return m.put(ctx, field, value, ttl.Milliseconds(), true)
}

// PutUntil stores field with an absolute expiry (HPEXPIREAT, unix ms).
func (m *RMapCacheNative) PutUntil(ctx context.Context, field string, value any, at time.Time) (any, error) {
	return m.put(ctx, field, value, at.UnixMilli(), false)
}

func (m *RMapCacheNative) put(ctx context.Context, field string, value any, ms int64, duration bool) (any, error) {
	if ms < 0 {
		return nil, errNegativeTTL
	}
	if ms == 0 {
		prev, err := m.Get(ctx, field)
		if err != nil {
			return nil, err
		}
		return prev, m.Put(ctx, field, value)
	}
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	ek := encodeKey(m.c.codec, field)
	res, err := mapCacheNativePutScript.Run(ctx, m.rc(), []string{m.name},
		ms, ek, enc, expireCmd(duration)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s, ok := res.(string)
	if !ok {
		return nil, nil
	}
	return m.c.codec.Decode(s)
}

// FastPutWithTTL sets field with TTL and reports whether the field was new.
func (m *RMapCacheNative) FastPutWithTTL(ctx context.Context, field string, value any, ttl time.Duration) (bool, error) {
	return m.fastPut(ctx, field, value, ttl.Milliseconds(), true)
}

func (m *RMapCacheNative) fastPut(ctx context.Context, field string, value any, ms int64, duration bool) (bool, error) {
	if ms <= 0 {
		return m.FastPut(ctx, field, value)
	}
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	n, err := mapCacheNativeFastPutScript.Run(ctx, m.rc(), []string{m.name},
		ms, encodeKey(m.c.codec, field), enc, expireCmd(duration)).Int()
	return n == 1, err
}

// PutIfAbsentWithTTL sets field only when absent, applying TTL on insert.
func (m *RMapCacheNative) PutIfAbsentWithTTL(ctx context.Context, field string, value any, ttl time.Duration) (any, error) {
	if ttl <= 0 {
		ok, err := m.PutIfAbsent(ctx, field, value)
		if err != nil || !ok {
			return m.Get(ctx, field)
		}
		return nil, nil
	}
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	res, err := mapCacheNativePutIfAbsentScript.Run(ctx, m.rc(), []string{m.name},
		ttl.Milliseconds(), encodeKey(m.c.codec, field), enc, "hpexpire").Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s, ok := res.(string)
	if !ok {
		return nil, nil
	}
	return m.c.codec.Decode(s)
}

// FastPutIfAbsentWithTTL returns true when the field was inserted with TTL.
func (m *RMapCacheNative) FastPutIfAbsentWithTTL(ctx context.Context, field string, value any, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return m.FastPutIfAbsent(ctx, field, value)
	}
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	n, err := mapCacheNativeFastPutIfAbsentScript.Run(ctx, m.rc(), []string{m.name},
		ttl.Milliseconds(), encodeKey(m.c.codec, field), enc, "hpexpire").Int()
	return n == 1, err
}

// ExpireEntry sets a relative TTL on an existing field.
func (m *RMapCacheNative) ExpireEntry(ctx context.Context, field string, ttl time.Duration) (bool, error) {
	n, err := mapCacheNativeExpireEntryScript.Run(ctx, m.rc(), []string{m.name},
		ttl.Milliseconds(), encodeKey(m.c.codec, field), "hpexpire").Int()
	return n == 1, err
}

// ExpireEntryIfNotSet sets TTL only when the field has no expiration (NX).
func (m *RMapCacheNative) ExpireEntryIfNotSet(ctx context.Context, field string, ttl time.Duration) (bool, error) {
	n, err := mapCacheNativeExpireEntryNXScript.Run(ctx, m.rc(), []string{m.name},
		"NX", ttl.Milliseconds(), encodeKey(m.c.codec, field), "hpexpire").Int()
	return n == 1, err
}

// RemainTTLForKey returns remaining field TTL. -2 absent, -1 no expiry.
func (m *RMapCacheNative) RemainTTLForKey(ctx context.Context, field string) (time.Duration, error) {
	res, err := m.rc().HPTTL(ctx, m.name, encodeKey(m.c.codec, field)).Result()
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return -2 * time.Millisecond, nil
	}
	switch res[0] {
	case -2:
		return -2 * time.Millisecond, nil
	case -1:
		return -1 * time.Millisecond, nil
	default:
		return time.Duration(res[0]) * time.Millisecond, nil
	}
}
