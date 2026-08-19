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
	ttlKey        string
	idleKey       string
	lastAccessKey string
	optionsKey    string

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

// EvictionMode controls which entries a size-limited map cache evicts.
type EvictionMode string

const (
	EvictionModeLRU EvictionMode = "LRU"
	EvictionModeLFU EvictionMode = "LFU"
)

func newRMapCache(c *Client, name string) *RMapCache {
	return &RMapCache{
		RMap:          newRMap(c, name),
		listeners:     make(map[string]map[int]func(kind, key, value, oldValue any)),
		ttlKey:        prefixName("redisson__timeout__set", name),
		idleKey:       prefixName("redisson__idle__set", name),
		lastAccessKey: prefixName("redisson__map_cache__last_access__set", name),
		optionsKey:    suffixName(name, "redisson_options"),
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
        redis.call('zrem', KEYS[4], unpack(expired))
    end
end
purge(KEYS[2])
purge(KEYS[3])
return out
`)

var mapCachePutScript = redis.NewScript(`
local added = redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
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
local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size'))
if maxSize ~= nil and maxSize ~= 0 then
    local mode = redis.call('hget', KEYS[5], 'mode')
    if mode == false or mode == 'LRU' then
        redis.call('zadd', KEYS[4], ARGV[5], ARGV[1])
    else
        redis.call('zincrby', KEYS[4], 1, ARGV[1])
    end
    if added == 1 then
        local excess = tonumber(redis.call('hlen', KEYS[1])) - maxSize
        if excess > 0 then
            local candidates = redis.call('zrange', KEYS[4], 0, -1)
            local removed = 0
            for _, candidate in ipairs(candidates) do
                if removed >= excess then
                    break
                end
                if candidate ~= ARGV[1] then
                    if redis.call('hdel', KEYS[1], candidate) == 1 then
                        redis.call('zrem', KEYS[2], candidate)
                        redis.call('zrem', KEYS[3], candidate)
                        removed = removed + 1
                    end
                    redis.call('zrem', KEYS[4], candidate)
                end
            end
        end
    end
end
return added
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
local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size'))
if maxSize ~= nil and maxSize ~= 0 then
    local mode = redis.call('hget', KEYS[5], 'mode')
    if mode == false or mode == 'LRU' then
        redis.call('zadd', KEYS[4], ARGV[5], ARGV[1])
    else
        redis.call('zincrby', KEYS[4], 1, ARGV[1])
    end
    local excess = tonumber(redis.call('hlen', KEYS[1])) - maxSize
    if excess > 0 then
        local candidates = redis.call('zrange', KEYS[4], 0, -1)
        local removed = 0
        for _, candidate in ipairs(candidates) do
            if removed >= excess then
                break
            end
            if candidate ~= ARGV[1] then
                if redis.call('hdel', KEYS[1], candidate) == 1 then
                    redis.call('zrem', KEYS[2], candidate)
                    redis.call('zrem', KEYS[3], candidate)
                    removed = removed + 1
                end
                redis.call('zrem', KEYS[4], candidate)
            end
        end
    end
end
return 1
`)

var mapCacheRemoveScript = redis.NewScript(`
local removed = redis.call('hdel', KEYS[1], ARGV[1])
redis.call('zrem', KEYS[2], ARGV[1])
redis.call('zrem', KEYS[3], ARGV[1])
redis.call('zrem', KEYS[4], ARGV[1])
return removed
`)

var mapCacheReplaceScript = redis.NewScript(`
local current = redis.call('hget', KEYS[1], ARGV[1])
if current == false then
    return nil
end
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
redis.call('zrem', KEYS[2], ARGV[1])
redis.call('zrem', KEYS[3], ARGV[1])
local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size'))
if maxSize ~= nil and maxSize ~= 0 then
    local mode = redis.call('hget', KEYS[5], 'mode')
    if mode == false or mode == 'LRU' then
        redis.call('zadd', KEYS[4], ARGV[3], ARGV[1])
    else
        redis.call('zincrby', KEYS[4], 1, ARGV[1])
    end
end
return current
`)

var mapCacheReplaceIfScript = redis.NewScript(`
local current = redis.call('hget', KEYS[1], ARGV[1])
if current == false then
    return 0
end
local t, value = struct.unpack('dLc0', current)
if value ~= ARGV[2] then
    return 0
end
redis.call('hset', KEYS[1], ARGV[1], ARGV[3])
redis.call('zrem', KEYS[2], ARGV[1])
redis.call('zrem', KEYS[3], ARGV[1])
local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size'))
if maxSize ~= nil and maxSize ~= 0 then
    local mode = redis.call('hget', KEYS[5], 'mode')
    if mode == false or mode == 'LRU' then
        redis.call('zadd', KEYS[4], ARGV[4], ARGV[1])
    else
        redis.call('zincrby', KEYS[4], 1, ARGV[1])
    end
end
return 1
`)

var mapCacheTrySetMaxSizeScript = redis.NewScript(`
redis.call('hsetnx', KEYS[1], 'max-size', ARGV[1])
return redis.call('hsetnx', KEYS[1], 'mode', ARGV[2])
`)

var mapCacheGetScript = redis.NewScript(`
local value = redis.call('hget', KEYS[1], ARGV[1])
if value == false then
    return nil
end
local maxIdle = struct.unpack('dLc0', value)
if maxIdle ~= 0 then
    redis.call('zadd', KEYS[2], maxIdle + tonumber(ARGV[2]), ARGV[1])
end
local maxSize = tonumber(redis.call('hget', KEYS[4], 'max-size'))
if maxSize ~= nil and maxSize ~= 0 then
    local mode = redis.call('hget', KEYS[4], 'mode')
    if mode == false or mode == 'LRU' then
        redis.call('zadd', KEYS[3], ARGV[2], ARGV[1])
    else
        redis.call('zincrby', KEYS[3], 1, ARGV[1])
    end
end
return value
`)

var mapCacheGetAllTTLOnlyScript = redis.NewScript(`
local result = {}
for i = 2, #ARGV do
    local value = redis.call('hget', KEYS[1], ARGV[i])
    if value ~= false then
        local expireDate = redis.call('zscore', KEYS[2], ARGV[i])
        if expireDate == false or tonumber(expireDate) > tonumber(ARGV[1]) then
            local t, val = struct.unpack('dLc0', value)
            result[#result + 1] = ARGV[i]
            result[#result + 1] = val
        end
    end
end
return result
`)

var mapCacheExpireEntryScript = redis.NewScript(`
local value = redis.call('hget', KEYS[1], ARGV[5])
local t, val
if value == false then
    return 0
else
    t, val = struct.unpack('dLc0', value)
    local expireDate = 92233720368547758
    local expireDateScore = redis.call('zscore', KEYS[2], ARGV[5])
    if expireDateScore ~= false then
        expireDate = tonumber(expireDateScore)
    end
    if t ~= 0 then
        local expireIdle = redis.call('zscore', KEYS[3], ARGV[5])
        if expireIdle ~= false then
            expireDate = math.min(expireDate, tonumber(expireIdle))
        end
    end
    if expireDate <= tonumber(ARGV[1]) then
        return 0
    end
    if ARGV[6] == '1' and expireDate ~= 92233720368547758 then
        return 0
    end
end

if tonumber(ARGV[2]) > 0 then
    redis.call('zadd', KEYS[2], ARGV[2], ARGV[5])
else
    redis.call('zrem', KEYS[2], ARGV[5])
end
if tonumber(ARGV[3]) > 0 then
    redis.call('zadd', KEYS[3], ARGV[3], ARGV[5])
else
    redis.call('zrem', KEYS[3], ARGV[5])
end

local packed = struct.pack('dLc0', ARGV[4], string.len(val), val)
redis.call('hset', KEYS[1], ARGV[5], packed)
return 1
`)

var mapCacheExpireEntriesScript = redis.NewScript(`
local counter = 0
for i = 6, #ARGV do
    local value = redis.call('hget', KEYS[1], ARGV[i])
    if value ~= false then
        local maxIdle, val = struct.unpack('dLc0', value)
        local expireDate = 92233720368547758
        local ttlScore = redis.call('zscore', KEYS[2], ARGV[i])
        if ttlScore ~= false then
            expireDate = tonumber(ttlScore)
        end
        if maxIdle ~= 0 then
            local idleScore = redis.call('zscore', KEYS[3], ARGV[i])
            if idleScore ~= false then
                expireDate = math.min(expireDate, tonumber(idleScore))
            end
        end
        local canSet = expireDate > tonumber(ARGV[1])
        if ARGV[5] == '1' and expireDate ~= 92233720368547758 then
            canSet = false
        end
        if canSet then
            if tonumber(ARGV[2]) > 0 then
                redis.call('zadd', KEYS[2], ARGV[2], ARGV[i])
            else
                redis.call('zrem', KEYS[2], ARGV[i])
            end
            if tonumber(ARGV[3]) > 0 then
                redis.call('zadd', KEYS[3], ARGV[3], ARGV[i])
            else
                redis.call('zrem', KEYS[3], ARGV[i])
            end
            redis.call('hset', KEYS[1], ARGV[i],
                struct.pack('dLc0', ARGV[4], string.len(val), val))
            counter = counter + 1
        end
    end
end
return counter
`)

// EvictExpired purges expired entries now, returning how many were removed.
func (m *RMapCache) EvictExpired(ctx context.Context) (int, error) {
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	res, err := mapCacheEvictScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey, m.lastAccessKey}, now).Result()
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

func mapCacheDurations(durations []time.Duration) (ttl, maxIdle time.Duration, err error) {
	switch len(durations) {
	case 0:
		return 0, 0, nil
	case 2:
		return durations[0], durations[1], nil
	default:
		return 0, 0, fmt.Errorf("redi: map cache Put requires both ttl and maxIdle")
	}
}

func validateEvictionMode(mode EvictionMode) error {
	if mode != EvictionModeLRU && mode != EvictionModeLFU {
		return fmt.Errorf("redi: unsupported map cache eviction mode %q", mode)
	}
	return nil
}

// TrySetMaxSize sets the maximum size with LRU eviction only when no size
// policy has been configured yet.
func (m *RMapCache) TrySetMaxSize(ctx context.Context, size int) (bool, error) {
	return m.TrySetMaxSizeMode(ctx, size, EvictionModeLRU)
}

// TrySetMaxSizeMode sets the maximum size and eviction mode once.
func (m *RMapCache) TrySetMaxSizeMode(
	ctx context.Context, size int, mode EvictionMode,
) (bool, error) {
	if size < 0 {
		return false, errors.New("redi: map cache max size must be non-negative")
	}
	if err := validateEvictionMode(mode); err != nil {
		return false, err
	}
	n, err := mapCacheTrySetMaxSizeScript.Run(
		ctx, m.rc(), []string{m.optionsKey}, size, string(mode),
	).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// SetMaxSize configures LRU eviction when the cache grows beyond size.
// A size of zero disables capacity eviction.
func (m *RMapCache) SetMaxSize(ctx context.Context, size int) error {
	return m.SetMaxSizeMode(ctx, size, EvictionModeLRU)
}

// SetMaxSizeMode configures the maximum size and LRU or LFU eviction.
func (m *RMapCache) SetMaxSizeMode(ctx context.Context, size int, mode EvictionMode) error {
	if size < 0 {
		return errors.New("redi: map cache max size must be non-negative")
	}
	if err := validateEvictionMode(mode); err != nil {
		return err
	}
	return m.rc().HSet(ctx, m.optionsKey,
		"max-size", size,
		"mode", string(mode),
	).Err()
}

// Put sets field with optional per-entry ttl and maxIdle (0 = disabled).
// Omitting both durations stores an entry without expiry. Publishes a created
// or updated entry event.
func (m *RMapCache) Put(ctx context.Context, field string, value any, durations ...time.Duration) error {
	ttl, maxIdle, err := mapCacheDurations(durations)
	if err != nil {
		return err
	}
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
		[]string{m.name, m.ttlKey, m.idleKey, m.lastAccessKey, m.optionsKey},
		ek, packValue(enc, float64(maxIdle.Milliseconds())), expiry, idleExpiry, now).Result(); err != nil {
		return err
	}
	if isNew {
		m.publishEvent(ctx, EventCreated, ek, enc)
	} else {
		m.publishEvent(ctx, EventUpdated, ek, enc, codecPart(prev))
	}
	return nil
}

// PutAll writes entries without expiry using the map-cache packed format.
func (m *RMapCache) PutAll(ctx context.Context, entries map[string]any) error {
	if len(entries) == 0 {
		return nil
	}
	type encodedEntry struct {
		field string
		value string
	}
	encoded := make([]encodedEntry, 0, len(entries))
	for field, value := range entries {
		enc, err := m.c.codec.Encode(value)
		if err != nil {
			return err
		}
		encoded = append(encoded, encodedEntry{
			field: encodeKey(m.c.codec, field),
			value: packValue(enc, 0),
		})
	}
	if _, err := m.EvictExpired(ctx); err != nil {
		return err
	}
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return err
	}
	for _, entry := range encoded {
		if _, err := mapCachePutScript.Run(ctx, m.rc(),
			[]string{m.name, m.ttlKey, m.idleKey, m.lastAccessKey, m.optionsKey},
			entry.field, entry.value, 0, 0, now).Result(); err != nil {
			return err
		}
	}
	return nil
}

// PutIfAbsent sets field only when absent, with optional ttl/maxIdle.
// Returns true when set.
func (m *RMapCache) PutIfAbsent(ctx context.Context, field string, value any, durations ...time.Duration) (bool, error) {
	ttl, maxIdle, err := mapCacheDurations(durations)
	if err != nil {
		return false, err
	}
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
		[]string{m.name, m.ttlKey, m.idleKey, m.lastAccessKey, m.optionsKey},
		ek, packValue(enc, float64(maxIdle.Milliseconds())), expiry, idleExpiry, now).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// FastPutIfAbsent stores only when absent, with optional ttl/maxIdle.
func (m *RMapCache) FastPutIfAbsent(
	ctx context.Context, field string, value any, durations ...time.Duration,
) (bool, error) {
	return m.PutIfAbsent(ctx, field, value, durations...)
}

func (m *RMapCache) expireEntry(
	ctx context.Context, field string, ttl, maxIdle time.Duration, ifNotSet bool,
) (bool, error) {
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	ttlTimeout, idleTimeout := int64(0), int64(0)
	if ttl > 0 {
		ttlTimeout = now + ttl.Milliseconds()
	}
	maxIdleDelta := int64(0)
	if maxIdle > 0 {
		maxIdleDelta = maxIdle.Milliseconds()
		idleTimeout = now + maxIdleDelta
	}
	onlyIfUnset := 0
	if ifNotSet {
		onlyIfUnset = 1
	}
	n, err := mapCacheExpireEntryScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey},
		now, ttlTimeout, idleTimeout, maxIdleDelta,
		encodeKey(m.c.codec, field), onlyIfUnset).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// ExpireEntry replaces the TTL/maxIdle policy of an existing live entry.
// A non-positive duration disables that expiration dimension.
func (m *RMapCache) ExpireEntry(
	ctx context.Context, field string, ttl, maxIdle time.Duration,
) (bool, error) {
	return m.expireEntry(ctx, field, ttl, maxIdle, false)
}

// UpdateEntryExpiration is the Redisson-style alias of ExpireEntry.
func (m *RMapCache) UpdateEntryExpiration(
	ctx context.Context, field string, ttl, maxIdle time.Duration,
) (bool, error) {
	return m.ExpireEntry(ctx, field, ttl, maxIdle)
}

// ExpireEntryIfNotSet sets expiration only when the entry has no TTL/maxIdle.
func (m *RMapCache) ExpireEntryIfNotSet(
	ctx context.Context, field string, ttl, maxIdle time.Duration,
) (bool, error) {
	return m.expireEntry(ctx, field, ttl, maxIdle, true)
}

func (m *RMapCache) expireEntries(
	ctx context.Context,
	fields []string,
	ttl, maxIdle time.Duration,
	ifNotSet bool,
) (int, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	ttlTimeout, idleTimeout := int64(0), int64(0)
	if ttl > 0 {
		ttlTimeout = now + ttl.Milliseconds()
	}
	maxIdleDelta := int64(0)
	if maxIdle > 0 {
		maxIdleDelta = maxIdle.Milliseconds()
		idleTimeout = now + maxIdleDelta
	}
	onlyIfUnset := 0
	if ifNotSet {
		onlyIfUnset = 1
	}
	args := []any{now, ttlTimeout, idleTimeout, maxIdleDelta, onlyIfUnset}
	for _, field := range fields {
		args = append(args, encodeKey(m.c.codec, field))
	}
	return mapCacheExpireEntriesScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey}, args...).Int()
}

// ExpireEntries replaces the TTL/maxIdle policy for live fields.
func (m *RMapCache) ExpireEntries(
	ctx context.Context, fields []string, ttl, maxIdle time.Duration,
) (int, error) {
	return m.expireEntries(ctx, fields, ttl, maxIdle, false)
}

// ExpireEntriesIfNotSet sets expiration only on fields without a policy.
func (m *RMapCache) ExpireEntriesIfNotSet(
	ctx context.Context, fields []string, ttl, maxIdle time.Duration,
) (int, error) {
	return m.expireEntries(ctx, fields, ttl, maxIdle, true)
}

// Get returns the value for field (nil when absent or expired). Access
// refreshes the maxIdle deadline.
func (m *RMapCache) Get(ctx context.Context, field string) (any, error) {
	codecValue, found, err := m.getCodecValue(ctx, field)
	if err != nil || !found {
		return nil, err
	}
	return m.c.codec.Decode(codecValue)
}

func (m *RMapCache) getCodecValue(ctx context.Context, field string) (string, bool, error) {
	if _, err := m.EvictExpired(ctx); err != nil {
		return "", false, err
	}
	ek := encodeKey(m.c.codec, field)
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return "", false, err
	}
	raw, err := mapCacheGetScript.Run(ctx, m.rc(),
		[]string{m.name, m.idleKey, m.lastAccessKey, m.optionsKey},
		ek, now).Text()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	_, codecValue, ok := unpackValue(raw)
	if !ok {
		codecValue = raw
	}
	return codecValue, true, nil
}

// GetInto decodes the value for field into target. Returns false when absent.
func (m *RMapCache) GetInto(ctx context.Context, field string, target any) (bool, error) {
	codecValue, found, err := m.getCodecValue(ctx, field)
	if err != nil || !found {
		return found, err
	}
	return true, decodeInto(m.c.codec, codecValue, target)
}

// GetAll returns all live field-value pairs with packed values decoded.
func (m *RMapCache) GetAll(ctx context.Context) (map[string]any, error) {
	if _, err := m.EvictExpired(ctx); err != nil {
		return nil, err
	}
	raw, err := m.rc().HGetAll(ctx, m.name).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		key, err := m.c.codec.Decode(k)
		if err != nil {
			return nil, err
		}
		val, err := m.c.codec.Decode(codecPart(v))
		if err != nil {
			return nil, err
		}
		if ks, ok := key.(string); ok {
			out[ks] = val
		}
	}
	return out, nil
}

// GetAllKeys returns live values for the requested fields.
func (m *RMapCache) GetAllKeys(ctx context.Context, fields ...string) (map[string]any, error) {
	if len(fields) == 0 {
		return map[string]any{}, nil
	}
	if _, err := m.EvictExpired(ctx); err != nil {
		return nil, err
	}
	encoded := make([]string, len(fields))
	for i, field := range fields {
		encoded[i] = encodeKey(m.c.codec, field)
	}
	values, err := m.rc().HMGet(ctx, m.name, encoded...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(fields))
	for i, value := range values {
		raw, ok := value.(string)
		if !ok {
			continue
		}
		decoded, err := m.c.codec.Decode(codecPart(raw))
		if err != nil {
			return nil, err
		}
		out[fields[i]] = decoded
	}
	return out, nil
}

// GetAllWithTTLOnly returns requested entries after checking only their
// explicit TTL. Max-idle expiration is intentionally ignored and not renewed.
func (m *RMapCache) GetAllWithTTLOnly(
	ctx context.Context, fields ...string,
) (map[string]any, error) {
	if len(fields) == 0 {
		return map[string]any{}, nil
	}
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return nil, err
	}
	args := make([]any, 0, len(fields)+1)
	args = append(args, now)
	for _, field := range fields {
		args = append(args, encodeKey(m.c.codec, field))
	}
	values, err := mapCacheGetAllTTLOnlyScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey}, args...).Slice()
	if err == redis.Nil {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(values)/2)
	for i := 0; i+1 < len(values); i += 2 {
		encodedField, _ := values[i].(string)
		encodedValue, _ := values[i+1].(string)
		field, err := m.c.codec.Decode(encodedField)
		if err != nil {
			return nil, err
		}
		value, err := m.c.codec.Decode(encodedValue)
		if err != nil {
			return nil, err
		}
		if key, ok := field.(string); ok {
			out[key] = value
		}
	}
	return out, nil
}

// Keys returns all live map fields. Order is undefined.
func (m *RMapCache) Keys(ctx context.Context) ([]string, error) {
	if _, err := m.EvictExpired(ctx); err != nil {
		return nil, err
	}
	raw, err := m.rc().HKeys(ctx, m.name).Result()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(raw))
	for _, key := range raw {
		decoded, err := m.c.codec.Decode(key)
		if err != nil {
			return nil, err
		}
		if field, ok := decoded.(string); ok {
			out = append(out, field)
		}
	}
	return out, nil
}

// Values returns all live map values with their packed headers removed.
func (m *RMapCache) Values(ctx context.Context) ([]any, error) {
	if _, err := m.EvictExpired(ctx); err != nil {
		return nil, err
	}
	raw, err := m.rc().HVals(ctx, m.name).Result()
	if err != nil {
		return nil, err
	}
	out := make([]any, len(raw))
	for i, value := range raw {
		decoded, err := m.c.codec.Decode(codecPart(value))
		if err != nil {
			return nil, err
		}
		out[i] = decoded
	}
	return out, nil
}

// ContainsKey reports whether the cache contains a live field.
func (m *RMapCache) ContainsKey(ctx context.Context, field string) (bool, error) {
	_, found, err := m.getCodecValue(ctx, field)
	return found, err
}

// ContainsValue reports whether a live field stores value (codec equality).
func (m *RMapCache) ContainsValue(ctx context.Context, value any) (bool, error) {
	encoded, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	if _, err := m.EvictExpired(ctx); err != nil {
		return false, err
	}
	values, err := m.rc().HVals(ctx, m.name).Result()
	if err != nil {
		return false, err
	}
	for _, value := range values {
		if codecPart(value) == encoded {
			return true, nil
		}
	}
	return false, nil
}

// FastPut stores an entry with optional ttl/maxIdle and reports whether new.
func (m *RMapCache) FastPut(
	ctx context.Context, field string, value any, durations ...time.Duration,
) (bool, error) {
	ttl, maxIdle, err := mapCacheDurations(durations)
	if err != nil {
		return false, err
	}
	encoded, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	if _, err := m.EvictExpired(ctx); err != nil {
		return false, err
	}
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
	n, err := mapCachePutScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey, m.lastAccessKey, m.optionsKey},
		encodeKey(m.c.codec, field),
		packValue(encoded, float64(maxIdle.Milliseconds())),
		expiry, idleExpiry, now).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (m *RMapCache) replaceExisting(ctx context.Context, field string, value any) (string, bool, error) {
	encoded, err := m.c.codec.Encode(value)
	if err != nil {
		return "", false, err
	}
	if _, err := m.EvictExpired(ctx); err != nil {
		return "", false, err
	}
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return "", false, err
	}
	result, err := mapCacheReplaceScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey, m.lastAccessKey, m.optionsKey},
		encodeKey(m.c.codec, field), packValue(encoded, 0), now).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	raw, ok := result.(string)
	if !ok {
		return "", false, nil
	}
	return codecPart(raw), true, nil
}

// Replace updates an existing live field without expiry and returns its old value.
func (m *RMapCache) Replace(ctx context.Context, field string, value any) (any, error) {
	old, replaced, err := m.replaceExisting(ctx, field, value)
	if err != nil || !replaced {
		return nil, err
	}
	return m.c.codec.Decode(old)
}

// ReplaceIf updates an existing live field when its decoded value matches.
func (m *RMapCache) ReplaceIf(ctx context.Context, field string, oldValue, newValue any) (bool, error) {
	oldEncoded, err := m.c.codec.Encode(oldValue)
	if err != nil {
		return false, err
	}
	newEncoded, err := m.c.codec.Encode(newValue)
	if err != nil {
		return false, err
	}
	if _, err := m.EvictExpired(ctx); err != nil {
		return false, err
	}
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	n, err := mapCacheReplaceIfScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey, m.lastAccessKey, m.optionsKey},
		encodeKey(m.c.codec, field), oldEncoded, packValue(newEncoded, 0), now).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// PutIfExists stores value without expiry only when field exists.
func (m *RMapCache) PutIfExists(ctx context.Context, field string, value any) (bool, error) {
	_, replaced, err := m.replaceExisting(ctx, field, value)
	return replaced, err
}

// FastPutIfExists is the Redisson-style alias of PutIfExists.
func (m *RMapCache) FastPutIfExists(ctx context.Context, field string, value any) (bool, error) {
	return m.PutIfExists(ctx, field, value)
}

// FastReplace is the Redisson-style alias of PutIfExists.
func (m *RMapCache) FastReplace(ctx context.Context, field string, value any) (bool, error) {
	return m.PutIfExists(ctx, field, value)
}

// Remove deletes field and its deadline entries, publishing a removed event.
func (m *RMapCache) Remove(ctx context.Context, field string) error {
	ek := encodeKey(m.c.codec, field)
	prev, err := m.rc().HGet(ctx, m.name, ek).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if _, err := mapCacheRemoveScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey, m.lastAccessKey}, ek).Result(); err != nil {
		return err
	}
	if err != redis.Nil {
		m.publishEvent(ctx, EventRemoved, ek, codecPart(prev))
	}
	return nil
}

// FastRemove deletes fields and their deadline entries.
func (m *RMapCache) FastRemove(ctx context.Context, fields ...string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	if _, err := m.EvictExpired(ctx); err != nil {
		return 0, err
	}
	var removed int64
	for _, field := range fields {
		n, err := mapCacheRemoveScript.Run(ctx, m.rc(),
			[]string{m.name, m.ttlKey, m.idleKey, m.lastAccessKey},
			encodeKey(m.c.codec, field)).Int64()
		if err != nil {
			return removed, err
		}
		removed += n
	}
	return removed, nil
}

// Delete removes fields, or the cache and companions when no fields are given.
func (m *RMapCache) Delete(ctx context.Context, fields ...string) error {
	if len(fields) == 0 {
		return m.Clear(ctx)
	}
	_, err := m.FastRemove(ctx, fields...)
	return err
}

// Size returns the live entry count (after eviction).
func (m *RMapCache) Size(ctx context.Context) (int64, error) {
	if _, err := m.EvictExpired(ctx); err != nil {
		return 0, err
	}
	return m.rc().HLen(ctx, m.name).Result()
}

// mapCacheRemainTTLScript matches RedissonMapCache.remainTimeToLive:
// missing/expired → -2; no TTL and no idle → -1; else min(ttl, idle) − now.
var mapCacheRemainTTLScript = redis.NewScript(`
local value = redis.call('hget', KEYS[1], ARGV[2])
if value == false then
    return -2
end
local t, val = struct.unpack('dLc0', value)
local expireDate = 92233720368547758
local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2])
if expireDateScore ~= false then
    expireDate = tonumber(expireDateScore)
end
if t ~= 0 then
    local expireIdle = redis.call('zscore', KEYS[3], ARGV[2])
    if expireIdle ~= false then
        expireDate = math.min(expireDate, tonumber(expireIdle))
    end
end
if expireDate == 92233720368547758 then
    return -1
end
if expireDate > tonumber(ARGV[1]) then
    return expireDate - tonumber(ARGV[1])
end
return -2
`)

// RemainTTLForKey returns remaining TTL as a duration (PTTL convention:
// -1ms = no expiry, -2ms = missing/expired). Idle timeout is included.
func (m *RMapCache) RemainTTLForKey(ctx context.Context, field string) (time.Duration, error) {
	now, err := m.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	ms, err := mapCacheRemainTTLScript.Run(ctx, m.rc(),
		[]string{m.name, m.ttlKey, m.idleKey},
		now, encodeKey(m.c.codec, field)).Int64()
	if err != nil {
		return 0, err
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// GetWithTTLOnly returns the value after checking only explicit TTL.
// Max-idle expiration is ignored and not renewed (Redisson getWithTTLOnly).
func (m *RMapCache) GetWithTTLOnly(ctx context.Context, field string) (any, error) {
	all, err := m.GetAllWithTTLOnly(ctx, field)
	if err != nil {
		return nil, err
	}
	return all[field], nil
}

// Clear removes the map and all deadline, access, and options companions.
func (m *RMapCache) Clear(ctx context.Context) error {
	return m.rc().Del(ctx,
		m.name, m.ttlKey, m.idleKey, m.lastAccessKey, m.optionsKey,
	).Err()
}

// Unlink asynchronously removes the cache and all companions.
func (m *RMapCache) Unlink(ctx context.Context) error {
	return m.rc().Unlink(ctx,
		m.name, m.ttlKey, m.idleKey, m.lastAccessKey, m.optionsKey,
	).Err()
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
		defer sub.Close() //nolint:errcheck // connection teardown //nolint:errcheck
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
