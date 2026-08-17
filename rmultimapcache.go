package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RSetMultimapCache stores an unordered value set per key with Redisson's
// per-key expiration metadata.
type RSetMultimapCache struct{ *rMultimapCache }

// RListMultimapCache stores an ordered value list per key with Redisson's
// per-key expiration metadata.
type RListMultimapCache struct{ *rMultimapCache }

// Redisson 4.6.1 MultimapCache expiration is per key, not per individual
// key/value association. This base intentionally exposes the faithful core:
// Put, PutAll, Get, Contains*, ExpireKey, RemoveAll and Clear.
type rMultimapCache struct {
	rObject
	multimap   RMultimap
	timeoutKey string
}

func newRSetMultimapCache(c *Client, name string) *RSetMultimapCache {
	return &RSetMultimapCache{newRMultimapCache(
		c, name, true, "redisson_set_multimap_ttl",
	)}
}

func newRListMultimapCache(c *Client, name string) *RListMultimapCache {
	return &RListMultimapCache{newRMultimapCache(
		c, name, false, "redisson_list_multimap_ttl",
	)}
}

func newRMultimapCache(
	c *Client, name string, isSet bool, timeoutSuffix string,
) *rMultimapCache {
	object := rObject{c: c, name: name}
	return &rMultimapCache{
		rObject: object,
		multimap: RMultimap{
			rObject: object,
			isSet:   isSet,
		},
		timeoutKey: suffixName(name, timeoutSuffix),
	}
}

var multimapCachePutScript = redis.NewScript(`
local expireDate = redis.call('zscore', KEYS[3], ARGV[2])
if expireDate ~= false and tonumber(expireDate) <= tonumber(ARGV[1]) then
    redis.call('hdel', KEYS[1], ARGV[2])
    redis.call('del', KEYS[2])
    redis.call('zrem', KEYS[3], ARGV[2])
end
redis.call('hset', KEYS[1], ARGV[2], ARGV[3])
if ARGV[4] == '1' then
    return redis.call('sadd', KEYS[2], ARGV[5])
end
redis.call('rpush', KEYS[2], ARGV[5])
return 1
`)

var multimapCacheGetScript = redis.NewScript(`
local expireDate = redis.call('zscore', KEYS[3], ARGV[2])
if expireDate ~= false and tonumber(expireDate) <= tonumber(ARGV[1]) then
    redis.call('hdel', KEYS[1], ARGV[2])
    redis.call('del', KEYS[2])
    redis.call('zrem', KEYS[3], ARGV[2])
    return {}
end
if redis.call('hexists', KEYS[1], ARGV[2]) == 0 then
    return {}
end
if ARGV[3] == '1' then
    return redis.call('smembers', KEYS[2])
end
return redis.call('lrange', KEYS[2], 0, -1)
`)

var multimapCacheExpireKeyScript = redis.NewScript(`
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    if tonumber(ARGV[1]) > 0 then
        redis.call('zadd', KEYS[2], ARGV[1], ARGV[2])
    else
        redis.call('zrem', KEYS[2], ARGV[2])
    end
    return 1
end
return 0
`)

var multimapCacheRemoveAllScript = redis.NewScript(`
local removed = redis.call('hdel', KEYS[1], ARGV[1])
redis.call('del', KEYS[2])
redis.call('zrem', KEYS[3], ARGV[1])
return removed
`)

var multimapCacheCleanupScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1])
for i = 1, #expired do
    local id = redis.call('hget', KEYS[1], expired[i])
    if id ~= false then
        redis.call('del', ARGV[2] .. id)
        redis.call('hdel', KEYS[1], expired[i])
    end
end
if #expired > 0 then
    redis.call('zrem', KEYS[2], unpack(expired))
end
return #expired
`)

var multimapCacheExpireScript = redis.NewScript(`
redis.call('zadd', KEYS[2], 92233720368547758, 'redisson__expiretag')
local ids = redis.call('hvals', KEYS[1])
for i = 1, #ids do
    redis.call('pexpire', ARGV[2] .. ids[i], ARGV[1])
end
redis.call('pexpire', KEYS[2], ARGV[1])
return redis.call('pexpire', KEYS[1], ARGV[1])
`)

func (m *rMultimapCache) cleanupExpired(ctx context.Context) error {
	err := multimapCacheCleanupScript.Run(ctx, m.rc(),
		[]string{m.name, m.timeoutKey},
		time.Now().UnixMilli(), m.multimap.collectionKey("")).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}

// Put associates value with key. For an expired key it first removes the old
// collection, so a new association starts a fresh non-expiring key.
func (m *rMultimapCache) Put(
	ctx context.Context, key, value any,
) (bool, error) {
	encodedKey, err := m.c.codec.Encode(key)
	if err != nil {
		return false, err
	}
	encodedValue, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	id := m.multimap.internalID(encodedKey)
	n, err := multimapCachePutScript.Run(ctx, m.rc(),
		[]string{m.name, m.multimap.collectionKey(id), m.timeoutKey},
		time.Now().UnixMilli(), encodedKey, id, m.multimap.collectionMode(),
		encodedValue).Int()
	return n == 1, err
}

// PutAll associates all values with key.
func (m *rMultimapCache) PutAll(
	ctx context.Context, key any, values ...any,
) (bool, error) {
	changed := false
	for _, value := range values {
		added, err := m.Put(ctx, key, value)
		if err != nil {
			return false, err
		}
		changed = changed || added
	}
	return changed, nil
}

// Get returns the current values. Expired keys are hidden and lazily removed
// from the HASH, timeout ZSET and per-key collection.
func (m *rMultimapCache) Get(ctx context.Context, key any) ([]any, error) {
	encodedKey, err := m.c.codec.Encode(key)
	if err != nil {
		return nil, err
	}
	id := m.multimap.internalID(encodedKey)
	result, err := multimapCacheGetScript.Run(ctx, m.rc(),
		[]string{m.name, m.multimap.collectionKey(id), m.timeoutKey},
		time.Now().UnixMilli(), encodedKey, m.multimap.collectionMode()).Slice()
	if err == redis.Nil {
		return []any{}, nil
	}
	if err != nil {
		return nil, err
	}
	return m.multimap.decodeValues(result)
}

// ExpireKey sets the lifetime of every value associated with key.
func (m *rMultimapCache) ExpireKey(
	ctx context.Context, key any, ttl time.Duration,
) (bool, error) {
	encodedKey, err := m.c.codec.Encode(key)
	if err != nil {
		return false, err
	}
	n, err := multimapCacheExpireKeyScript.Run(ctx, m.rc(),
		[]string{m.name, m.timeoutKey},
		time.Now().Add(ttl).UnixMilli(), encodedKey).Int()
	return n == 1, err
}

// ContainsKey reports whether key currently has values.
func (m *rMultimapCache) ContainsKey(ctx context.Context, key any) (bool, error) {
	values, err := m.Get(ctx, key)
	return len(values) > 0, err
}

// ContainsEntry reports whether the current key contains value.
func (m *rMultimapCache) ContainsEntry(
	ctx context.Context, key, value any,
) (bool, error) {
	encodedValue, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	values, err := m.Get(ctx, key)
	if err != nil {
		return false, err
	}
	for _, current := range values {
		encodedCurrent, encodeErr := m.c.codec.Encode(current)
		if encodeErr != nil {
			return false, encodeErr
		}
		if encodedCurrent == encodedValue {
			return true, nil
		}
	}
	return false, nil
}

// ContainsValue reports whether any live key is associated with value.
func (m *rMultimapCache) ContainsValue(ctx context.Context, value any) (bool, error) {
	if err := m.cleanupExpired(ctx); err != nil {
		return false, err
	}
	return m.multimap.ContainsValue(ctx, value)
}

// ReplaceValues replaces all live values associated with key and returns the
// previous values. The key keeps its existing per-key expiration.
func (m *rMultimapCache) ReplaceValues(
	ctx context.Context, key any, values ...any,
) ([]any, error) {
	if err := m.cleanupExpired(ctx); err != nil {
		return nil, err
	}
	old, err := m.multimap.ReplaceValues(ctx, key, values...)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		encodedKey, encodeErr := m.c.codec.Encode(key)
		if encodeErr != nil {
			return nil, encodeErr
		}
		if err := m.rc().ZRem(ctx, m.timeoutKey, encodedKey).Err(); err != nil {
			return nil, err
		}
	}
	return old, nil
}

// Values returns values across all live keys.
func (m *rMultimapCache) Values(ctx context.Context) ([]any, error) {
	if err := m.cleanupExpired(ctx); err != nil {
		return nil, err
	}
	return m.multimap.Values(ctx)
}

// Entries returns every live key/value association.
func (m *rMultimapCache) Entries(ctx context.Context) ([]MultimapEntry, error) {
	if err := m.cleanupExpired(ctx); err != nil {
		return nil, err
	}
	return m.multimap.Entries(ctx)
}

// KeySet returns every live key.
func (m *rMultimapCache) KeySet(ctx context.Context) ([]any, error) {
	if err := m.cleanupExpired(ctx); err != nil {
		return nil, err
	}
	return m.multimap.KeySet(ctx)
}

// ReadAllKeySet is the eager Redisson-style alias of KeySet.
func (m *rMultimapCache) ReadAllKeySet(ctx context.Context) ([]any, error) {
	return m.KeySet(ctx)
}

// KeySize returns the number of live keys.
func (m *rMultimapCache) KeySize(ctx context.Context) (int64, error) {
	if err := m.cleanupExpired(ctx); err != nil {
		return 0, err
	}
	return m.multimap.KeySize(ctx)
}

// Size returns the number of live key/value associations.
func (m *rMultimapCache) Size(ctx context.Context) (int64, error) {
	if err := m.cleanupExpired(ctx); err != nil {
		return 0, err
	}
	return m.multimap.Size(ctx)
}

// IsEmpty reports whether the cache has no live associations.
func (m *rMultimapCache) IsEmpty(ctx context.Context) (bool, error) {
	size, err := m.Size(ctx)
	return size == 0, err
}

// RemoveAll removes key, its values and expiration metadata.
func (m *rMultimapCache) RemoveAll(ctx context.Context, key any) (bool, error) {
	encodedKey, err := m.c.codec.Encode(key)
	if err != nil {
		return false, err
	}
	id := m.multimap.internalID(encodedKey)
	n, err := multimapCacheRemoveAllScript.Run(ctx, m.rc(),
		[]string{m.name, m.multimap.collectionKey(id), m.timeoutKey},
		encodedKey).Int()
	return n == 1, err
}

// Expire applies ttl to the index, timeout metadata and every per-key
// collection, matching RedissonMultimapCache's whole-object expiration.
func (m *rMultimapCache) Expire(ctx context.Context, ttl time.Duration) (bool, error) {
	n, err := multimapCacheExpireScript.Run(ctx, m.rc(),
		[]string{m.name, m.timeoutKey},
		ttl.Milliseconds(), m.multimap.collectionKey("")).Int()
	return n == 1, err
}

// Clear removes the index, timeout metadata and every per-key collection.
func (m *rMultimapCache) Clear(ctx context.Context) error {
	if err := m.multimap.Clear(ctx); err != nil {
		return err
	}
	return m.rc().Del(ctx, m.timeoutKey).Err()
}

// Delete removes the whole multimap cache.
func (m *rMultimapCache) Delete(ctx context.Context) error {
	return m.Clear(ctx)
}
