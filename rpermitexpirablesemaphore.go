package redi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RPermitExpirableSemaphore is a semaphore whose permits are individually
// leased and expire on their own, wire-compatible with Redisson's
// RedissonPermitExpirableSemaphore:
//
//	STRING {name}             available permit counter
//	ZSET   {name}:timeout     issued permits (member = opaque permit id,
//	                          score = absolute expiry epoch ms)
//	PUBSUB redisson_sc:{name} release wake-up channel
//
// Each acquire returns a unique permit id; only that id can release the
// permit. Expired permits are reclaimed into the pool atomically on the
// next acquire (Lua).
type RPermitExpirableSemaphore struct {
	rObject
	timeoutKey string
	channel    string
}

func newRPermitExpirableSemaphore(c *Client, name string) *RPermitExpirableSemaphore {
	return &RPermitExpirableSemaphore{
		rObject:    rObject{c: c, name: name},
		timeoutKey: suffixName(name, "timeout"),
		channel:    prefixName("redisson_sc", name),
	}
}

var permitAcquireScript = redis.NewScript(`
local key = KEYS[1]
local permits_key = KEYS[2]
local permit_id = ARGV[1]
local lease_ms = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local expired = redis.call('zrangebyscore', permits_key, 0, now)
if #expired > 0 then
    redis.call('zremrangebyscore', permits_key, 0, now)
    redis.call('incrby', key, #expired)
end

local available = tonumber(redis.call('get', key) or '0')
if available <= 0 then
    return nil
end

redis.call('decrby', key, 1)
redis.call('zadd', permits_key, now + lease_ms, permit_id)
return permit_id
`)

var permitReleaseScript = redis.NewScript(`
local removed = redis.call('zrem', KEYS[2], ARGV[1])
if removed == 1 then
    redis.call('incrby', KEYS[1], 1)
    redis.call('publish', KEYS[3], ARGV[1])
    return 1
end
return 0
`)

// ErrNoPermit is returned by Acquire when the context ends first.
var ErrNoPermit = errors.New("redi: no permit acquired")

// TrySetPermits initializes the counter only when it does not exist.
func (s *RPermitExpirableSemaphore) TrySetPermits(ctx context.Context, permits int64) (bool, error) {
	return s.rc().SetNX(ctx, s.name, permits, 0).Result()
}

// TryAcquire leases one permit for lease duration, returning its id
// ("" when none available).
func (s *RPermitExpirableSemaphore) TryAcquire(ctx context.Context, lease time.Duration) (string, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return "", err
	}
	permitID := hex.EncodeToString(id)
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return "", err
	}
	res, err := permitAcquireScript.Run(ctx, s.rc(),
		[]string{s.name, s.timeoutKey}, permitID, lease.Milliseconds(), now).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	pid, _ := res.(string)
	return pid, nil
}

// Acquire blocks until a permit is leased or ctx ends (ErrNoPermit).
// Wakes on release via the redisson_sc channel with a 1s fallback.
func (s *RPermitExpirableSemaphore) Acquire(ctx context.Context, lease time.Duration) (string, error) {
	pid, err := s.TryAcquire(ctx, lease)
	if err != nil || pid != "" {
		return pid, err
	}
	sub := s.subscribe(ctx, s.channel)
	defer sub.Close() //nolint:errcheck // connection teardown
	wake := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return "", ErrNoPermit
		case <-wake:
		case <-time.After(time.Second):
		}
		pid, err := s.TryAcquire(ctx, lease)
		if err != nil {
			return "", err
		}
		if pid != "" {
			return pid, nil
		}
	}
}

// Release returns a leased permit. Returns false for an unknown or expired
// permit id.
func (s *RPermitExpirableSemaphore) Release(ctx context.Context, permitID string) (bool, error) {
	n, err := permitReleaseScript.Run(ctx, s.rc(),
		[]string{s.name, s.timeoutKey, s.channel}, permitID).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

var permitPurgeScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1])
if #expired > 0 then
    redis.call('zremrangebyscore', KEYS[2], 0, ARGV[1])
    redis.call('incrby', KEYS[1], #expired)
end
return #expired
`)

// purgeExpired reclaims expired permits into the pool (lazy eviction; runs
// on every read so counters reflect reality).
func (s *RPermitExpirableSemaphore) purgeExpired(ctx context.Context) error {
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return err
	}
	return permitPurgeScript.Run(ctx, s.rc(), []string{s.name, s.timeoutKey}, now).Err()
}

// AvailablePermits returns the free permit count.
func (s *RPermitExpirableSemaphore) AvailablePermits(ctx context.Context) (int64, error) {
	if err := s.purgeExpired(ctx); err != nil {
		return 0, err
	}
	v, err := s.rc().Get(ctx, s.name).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return parseInt64(v), nil
}

// AcquiredPermits returns the currently leased permit count.
func (s *RPermitExpirableSemaphore) AcquiredPermits(ctx context.Context) (int64, error) {
	if err := s.purgeExpired(ctx); err != nil {
		return 0, err
	}
	return s.rc().ZCard(ctx, s.timeoutKey).Result()
}

// UpdateLeaseTime extends (or shortens) a permit's lease. Returns false
// for an unknown permit.
func (s *RPermitExpirableSemaphore) UpdateLeaseTime(ctx context.Context, permitID string, lease time.Duration) (bool, error) {
	if _, err := s.rc().ZScore(ctx, s.timeoutKey, permitID).Result(); err != nil {
		if err == redis.Nil {
			return false, nil // unknown permit
		}
		return false, err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	// ZAddXX returns the count of ADDED members (0 for existing), so
	// existence is checked above instead.
	return true, s.rc().ZAddXX(ctx, s.timeoutKey, redis.Z{
		Score:  float64(now + lease.Milliseconds()),
		Member: permitID,
	}).Err()
}

// LeaseTime returns the remaining lease of a permit (-1 when unknown).
func (s *RPermitExpirableSemaphore) LeaseTime(ctx context.Context, permitID string) (time.Duration, error) {
	score, err := s.rc().ZScore(ctx, s.timeoutKey, permitID).Result()
	if err == redis.Nil {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	remaining := time.Duration((score - float64(now)) * float64(time.Millisecond))
	if remaining < 0 {
		remaining = 0
	}
	return remaining, nil
}

// Delete removes the counter and the permit zset.
func (s *RPermitExpirableSemaphore) Delete(ctx context.Context) error {
	return s.rc().Del(ctx, s.name, s.timeoutKey).Err()
}
