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
redis.call('zremrangebyscore', KEYS[1], 0, tonumber(ARGV[1]))
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

var setCacheContainsScript = redis.NewScript(`
redis.call('zremrangebyscore', KEYS[1], 0, tonumber(ARGV[1]))
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
redis.call('zremrangebyscore', KEYS[1], 0, tonumber(ARGV[1]))
return 1
`)

func (s *RSetCache) evictExpired(ctx context.Context) error {
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return err
	}
	return setCacheEvictScript.Run(ctx, s.rc(), []string{s.name}, now).Err()
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
	out := make([]any, len(vals))
	for i, v := range vals {
		d, err := s.c.codec.Decode(v)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

// Clear removes the cache and its idle set.
func (s *RSetCache) Clear(ctx context.Context) error {
	return s.rc().Del(ctx, s.name, s.idleKey).Err()
}
