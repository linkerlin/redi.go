package redi

import (
	"context"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// RSetCache is an RSet with per-element TTL and maxIdle, wire-compatible
// with Redisson's RedissonSetCache:
//
//	ZSET {name}                        member = codec value,
//	                                   score = absolute expiry epoch ms
//	                                   (math.MaxInt64 for no TTL)
//	ZSET redisson__idle__set:{name}    idle *duration* per member;
//	                                   contains() refreshes the deadline.
type RSetCache struct {
	rObject
	idleKey string
}

func newRSetCache(c *Client, name string) *RSetCache {
	return &RSetCache{
		rObject: rObject{c: c, name: name},
		idleKey: prefixName("redisson__idle__set", name),
	}
}

const maxScore = int64(math.MaxInt64)

var setCacheAddScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[1], 0, tonumber(ARGV[1]))
for i = 1, #expired do
    redis.call('zrem', KEYS[1], expired[i])
    redis.call('zrem', KEYS[2], expired[i])
end
local exists = redis.call('zscore', KEYS[1], ARGV[3])
redis.call('zadd', KEYS[1], ARGV[2], ARGV[3])
if tonumber(ARGV[4]) > 0 then
    redis.call('zadd', KEYS[2], ARGV[4], ARGV[3])
else
    redis.call('zrem', KEYS[2], ARGV[3])
end
if exists == false then
    return 1
end
return 0
`)

var setCacheConditionalAddScript = redis.NewScript(`
local score = redis.call('zscore', KEYS[1], ARGV[3])
local live = score ~= false and tonumber(score) > tonumber(ARGV[1])
local mode = ARGV[4]
if mode == 'absent' and live then
    return 0
end
if mode ~= 'absent' and not live then
    return 0
end
if mode == 'less' and tonumber(ARGV[2]) >= tonumber(score) then
    return 0
end
if mode == 'greater' and tonumber(ARGV[2]) <= tonumber(score) then
    return 0
end
redis.call('zadd', KEYS[1], ARGV[2], ARGV[3])
redis.call('zrem', KEYS[2], ARGV[3])
return 1
`)

var setCacheContainsScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[1], 0, tonumber(ARGV[1]))
for i = 1, #expired do
    redis.call('zrem', KEYS[1], expired[i])
    redis.call('zrem', KEYS[2], expired[i])
end
local score = redis.call('zscore', KEYS[1], ARGV[2])
if score == false then
    return 0
end
local idle = redis.call('zscore', KEYS[2], ARGV[2])
if idle ~= false then
    redis.call('zadd', KEYS[1], tonumber(ARGV[1]) + tonumber(idle), ARGV[2])
end
return 1
`)

var setCacheEvictScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[1], 0, tonumber(ARGV[1]))
for i = 1, #expired do
    redis.call('zrem', KEYS[1], expired[i])
    redis.call('zrem', KEYS[2], expired[i])
end
return #expired
`)

func (s *RSetCache) evictExpired(ctx context.Context) error {
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return err
	}
	return setCacheEvictScript.Run(ctx, s.rc(), []string{s.name, s.idleKey}, now).Err()
}

// Add adds element with optional ttl and maxIdle (0 = disabled).
// Returns true when the element was not present before.
func (s *RSetCache) Add(ctx context.Context, element any, ttl, maxIdle time.Duration) (bool, error) {
	enc, err := s.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	expiry := maxScore
	switch {
	case ttl > 0 && maxIdle > 0:
		if ttl < maxIdle {
			expiry = now + ttl.Milliseconds()
		} else {
			expiry = now + maxIdle.Milliseconds()
		}
	case ttl > 0:
		expiry = now + ttl.Milliseconds()
	case maxIdle > 0:
		expiry = now + maxIdle.Milliseconds()
	}
	idleMs := maxIdle.Milliseconds()
	n, err := setCacheAddScript.Run(ctx, s.rc(),
		[]string{s.name, s.idleKey}, now, expiry, enc, idleMs).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *RSetCache) conditionalAdd(
	ctx context.Context, element any, ttl time.Duration, mode string,
) (bool, error) {
	enc, err := s.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	expiry := maxScore
	if ttl > 0 {
		expiry = now + ttl.Milliseconds()
	}
	n, err := setCacheConditionalAddScript.Run(ctx, s.rc(),
		[]string{s.name, s.idleKey}, now, expiry, enc, mode).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// AddIfAbsent adds element only when it has no live TTL deadline.
func (s *RSetCache) AddIfAbsent(ctx context.Context, element any, ttl time.Duration) (bool, error) {
	return s.conditionalAdd(ctx, element, ttl, "absent")
}

// TryAdd is the Redisson-style alias of AddIfAbsent.
func (s *RSetCache) TryAdd(ctx context.Context, element any, ttl time.Duration) (bool, error) {
	return s.AddIfAbsent(ctx, element, ttl)
}

// AddIfExists updates the TTL only when element is live.
func (s *RSetCache) AddIfExists(ctx context.Context, element any, ttl time.Duration) (bool, error) {
	return s.conditionalAdd(ctx, element, ttl, "exists")
}

// AddIfLess shortens the TTL deadline of a live element.
func (s *RSetCache) AddIfLess(ctx context.Context, element any, ttl time.Duration) (bool, error) {
	return s.conditionalAdd(ctx, element, ttl, "less")
}

// AddIfGreater extends the TTL deadline of a live element.
func (s *RSetCache) AddIfGreater(ctx context.Context, element any, ttl time.Duration) (bool, error) {
	return s.conditionalAdd(ctx, element, ttl, "greater")
}

// Contains reports membership; a hit refreshes the maxIdle deadline.
func (s *RSetCache) Contains(ctx context.Context, element any) (bool, error) {
	enc, err := s.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	n, err := setCacheContainsScript.Run(ctx, s.rc(),
		[]string{s.name, s.idleKey}, now, enc).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Remove drops element (and its idle entry).
func (s *RSetCache) Remove(ctx context.Context, element any) (bool, error) {
	enc, err := s.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	n, err := s.rc().ZRem(ctx, s.name, enc).Result()
	if err != nil || n == 0 {
		return false, err
	}
	return true, s.rc().ZRem(ctx, s.idleKey, enc).Err()
}

// Size returns the live element count (after eviction).
func (s *RSetCache) Size(ctx context.Context) (int64, error) {
	if err := s.evictExpired(ctx); err != nil {
		return 0, err
	}
	return s.rc().ZCard(ctx, s.name).Result()
}

// Members returns all live elements (decoded).
func (s *RSetCache) Members(ctx context.Context) ([]any, error) {
	if err := s.evictExpired(ctx); err != nil {
		return nil, err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return nil, err
	}
	vals, err := s.rc().ZRangeByScore(ctx, s.name, &redis.ZRangeBy{
		Min: formatFloat(float64(now + 1)),
		Max: formatFloat(float64(maxScore)),
	}).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

// ReadAll is the Redisson-style alias of Members.
func (s *RSetCache) ReadAll(ctx context.Context) ([]any, error) {
	return s.Members(ctx)
}

// Random returns one random live element, or (nil, nil) when empty.
func (s *RSetCache) Random(ctx context.Context) (any, error) {
	if err := s.evictExpired(ctx); err != nil {
		return nil, err
	}
	vals, err := s.rc().ZRandMember(ctx, s.name, 1).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil
	}
	return s.c.codec.Decode(vals[0])
}

// RandomN returns up to n distinct random live elements.
func (s *RSetCache) RandomN(ctx context.Context, n int64) ([]any, error) {
	if n <= 0 {
		return nil, nil
	}
	if err := s.evictExpired(ctx); err != nil {
		return nil, err
	}
	vals, err := s.rc().ZRandMember(ctx, s.name, int(n)).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

var setCacheRemoveRandomScript = redis.NewScript(`
local member = redis.call('zrandmember', KEYS[1])
if member == false then
    return nil
end
redis.call('zrem', KEYS[1], member)
redis.call('zrem', KEYS[2], member)
return member
`)

// RemoveRandom removes and returns one random live element.
func (s *RSetCache) RemoveRandom(ctx context.Context) (any, error) {
	if err := s.evictExpired(ctx); err != nil {
		return nil, err
	}
	v, err := setCacheRemoveRandomScript.Run(ctx, s.rc(),
		[]string{s.name, s.idleKey}).Result()
	if err == redis.Nil || v == nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	encoded, ok := v.(string)
	if !ok {
		return nil, nil
	}
	return s.c.codec.Decode(encoded)
}

var setCacheRemoveRandomNScript = redis.NewScript(`
local members = redis.call('zrandmember', KEYS[1], ARGV[1])
for i = 1, #members do
    redis.call('zrem', KEYS[1], members[i])
    redis.call('zrem', KEYS[2], members[i])
end
return members
`)

// RemoveRandomN removes and returns up to n distinct random live elements.
func (s *RSetCache) RemoveRandomN(ctx context.Context, n int64) ([]any, error) {
	if n <= 0 {
		return nil, nil
	}
	if err := s.evictExpired(ctx); err != nil {
		return nil, err
	}
	vals, err := setCacheRemoveRandomNScript.Run(ctx, s.rc(),
		[]string{s.name, s.idleKey}, n).StringSlice()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

var setCacheContainsAllScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[1], 0, tonumber(ARGV[1]))
for i = 1, #expired do
    redis.call('zrem', KEYS[1], expired[i])
    redis.call('zrem', KEYS[2], expired[i])
end
for i = 2, #ARGV do
    if redis.call('zscore', KEYS[1], ARGV[i]) == false then
        return 0
    end
    local idle = redis.call('zscore', KEYS[2], ARGV[i])
    if idle ~= false then
        redis.call('zadd', KEYS[1], tonumber(ARGV[1]) + tonumber(idle), ARGV[i])
    end
end
return 1
`)

// ContainsAll reports whether every element is live in the cache.
func (s *RSetCache) ContainsAll(ctx context.Context, elements ...any) (bool, error) {
	if len(elements) == 0 {
		return true, nil
	}
	encoded, err := s.encodeAll(elements)
	if err != nil {
		return false, err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	args := make([]any, 1, len(encoded)+1)
	args[0] = now
	args = append(args, encoded...)
	n, err := setCacheContainsAllScript.Run(ctx, s.rc(),
		[]string{s.name, s.idleKey}, args...).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

var setCacheRemoveAllScript = redis.NewScript(`
local removed = 0
for i = 1, #ARGV do
    removed = removed + redis.call('zrem', KEYS[1], ARGV[i])
    redis.call('zrem', KEYS[2], ARGV[i])
end
return removed
`)

// RemoveAll removes the supplied elements and reports whether the cache changed.
func (s *RSetCache) RemoveAll(ctx context.Context, elements ...any) (bool, error) {
	if len(elements) == 0 {
		return false, nil
	}
	if err := s.evictExpired(ctx); err != nil {
		return false, err
	}
	encoded, err := s.encodeAll(elements)
	if err != nil {
		return false, err
	}
	n, err := setCacheRemoveAllScript.Run(ctx, s.rc(),
		[]string{s.name, s.idleKey}, encoded...).Int64()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

var setCacheRetainAllScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[1], 0, tonumber(ARGV[1]))
for i = 1, #expired do
    redis.call('zrem', KEYS[1], expired[i])
    redis.call('zrem', KEYS[2], expired[i])
end
local keep = {}
for i = 2, #ARGV do
    keep[ARGV[i]] = true
end
local members = redis.call('zrange', KEYS[1], 0, -1)
local removed = 0
for i = 1, #members do
    if keep[members[i]] == nil then
        removed = removed + redis.call('zrem', KEYS[1], members[i])
        redis.call('zrem', KEYS[2], members[i])
    end
end
if removed > 0 or #expired > 0 then
    return 1
end
return 0
`)

// RetainAll keeps only the supplied live elements and reports whether changed.
func (s *RSetCache) RetainAll(ctx context.Context, elements ...any) (bool, error) {
	encoded, err := s.encodeAll(elements)
	if err != nil {
		return false, err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	args := make([]any, 1, len(encoded)+1)
	args[0] = now
	args = append(args, encoded...)
	n, err := setCacheRetainAllScript.Run(ctx, s.rc(),
		[]string{s.name, s.idleKey}, args...).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Clear removes the cache and its idle set.
func (s *RSetCache) Clear(ctx context.Context) error {
	return s.rc().Del(ctx, s.name, s.idleKey).Err()
}

func (s *RSetCache) encodeAll(elements []any) ([]any, error) {
	encoded := make([]any, len(elements))
	for i, element := range elements {
		value, err := s.c.codec.Encode(element)
		if err != nil {
			return nil, err
		}
		encoded[i] = value
	}
	return encoded, nil
}

func (s *RSetCache) decodeAll(values []string) ([]any, error) {
	decoded := make([]any, len(values))
	for i, value := range values {
		element, err := s.c.codec.Decode(value)
		if err != nil {
			return nil, err
		}
		decoded[i] = element
	}
	return decoded, nil
}
