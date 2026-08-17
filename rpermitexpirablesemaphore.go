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
local permits = tonumber(ARGV[1])
local lease_ms = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local expired = redis.call('zrangebyscore', permits_key, 0, now)
if #expired > 0 then
    redis.call('zremrangebyscore', permits_key, 0, now)
    redis.call('incrby', key, #expired)
end

local available = tonumber(redis.call('get', key) or '0')
if available < permits then
    return 0
end

redis.call('decrby', key, permits)
for i = 4, #ARGV do
    redis.call('zadd', permits_key, now + lease_ms, ARGV[i])
end
return 1
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
	ids, err := s.TryAcquireN(ctx, 1, lease)
	if err != nil || len(ids) == 0 {
		return "", err
	}
	return ids[0], nil
}

// TryAcquireN atomically leases permits, returning an empty slice when the
// requested number isn't available.
func (s *RPermitExpirableSemaphore) TryAcquireN(
	ctx context.Context,
	permits int,
	lease time.Duration,
) ([]string, error) {
	if permits < 0 {
		return nil, errors.New("redi: permits must not be negative")
	}
	if permits == 0 {
		return []string{}, nil
	}
	ids, err := generatePermitIDs(permits)
	if err != nil {
		return nil, err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return nil, err
	}
	args := make([]any, 0, len(ids)+3)
	args = append(args, permits, lease.Milliseconds(), now)
	for _, id := range ids {
		args = append(args, id)
	}
	acquired, err := permitAcquireScript.Run(ctx, s.rc(),
		[]string{s.name, s.timeoutKey}, args...).Int()
	if err != nil {
		return nil, err
	}
	if acquired == 0 {
		return []string{}, nil
	}
	return ids, nil
}

// Acquire blocks until a permit is leased or ctx ends (ErrNoPermit).
// Wakes on release via the redisson_sc channel with a 1s fallback.
func (s *RPermitExpirableSemaphore) Acquire(ctx context.Context, lease time.Duration) (string, error) {
	ids, err := s.acquireN(ctx, 1, lease, 0, false)
	if err != nil || len(ids) == 0 {
		return "", err
	}
	return ids[0], nil
}

// TryAcquireWait waits up to wait for one permit, returning an empty id on
// timeout.
func (s *RPermitExpirableSemaphore) TryAcquireWait(
	ctx context.Context,
	lease, wait time.Duration,
) (string, error) {
	ids, err := s.acquireN(ctx, 1, lease, wait, true)
	if err != nil || len(ids) == 0 {
		return "", err
	}
	return ids[0], nil
}

// AcquireN blocks until all requested permits can be leased atomically.
func (s *RPermitExpirableSemaphore) AcquireN(
	ctx context.Context,
	permits int,
	lease time.Duration,
) ([]string, error) {
	return s.acquireN(ctx, permits, lease, 0, false)
}

func (s *RPermitExpirableSemaphore) acquireN(
	ctx context.Context,
	permits int,
	lease, wait time.Duration,
	bounded bool,
) ([]string, error) {
	var deadline time.Time
	if bounded {
		deadline = time.Now().Add(wait)
	}
	ids, err := s.TryAcquireN(ctx, permits, lease)
	if err != nil || len(ids) > 0 || permits == 0 {
		return ids, err
	}
	if bounded && wait <= 0 {
		return []string{}, nil
	}
	sub := s.subscribe(ctx, s.channel)
	defer sub.Close() //nolint:errcheck // connection teardown
	wake := sub.Channel()

	fallback := time.NewTicker(time.Second)
	defer fallback.Stop()
	var timeout <-chan time.Time
	var timer *time.Timer
	if bounded {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return []string{}, nil
		}
		timer = time.NewTimer(remaining)
		defer timer.Stop()
		timeout = timer.C
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ErrNoPermit
		case <-timeout:
			return []string{}, nil
		case <-wake:
		case <-fallback.C:
		}
		ids, err := s.TryAcquireN(ctx, permits, lease)
		if err != nil {
			return nil, err
		}
		if len(ids) > 0 {
			return ids, nil
		}
	}
}

func generatePermitIDs(count int) ([]string, error) {
	ids := make([]string, count)
	for i := range ids {
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return nil, err
		}
		ids[i] = hex.EncodeToString(id)
	}
	return ids, nil
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

var permitReleaseAllScript = redis.NewScript(`
local removed = 0
for i = 1, #ARGV do
    removed = removed + redis.call('zrem', KEYS[2], ARGV[i])
end
if removed > 0 then
    redis.call('incrby', KEYS[1], removed)
    redis.call('publish', KEYS[3], removed)
end
return removed
`)

// ReleaseAll returns all known permit ids and reports how many were released.
func (s *RPermitExpirableSemaphore) ReleaseAll(ctx context.Context, permitIDs ...string) (int64, error) {
	if len(permitIDs) == 0 {
		return 0, nil
	}
	args := make([]any, len(permitIDs))
	for i, permitID := range permitIDs {
		args[i] = permitID
	}
	return permitReleaseAllScript.Run(ctx, s.rc(),
		[]string{s.name, s.timeoutKey, s.channel}, args...).Int64()
}

var permitPurgeScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1])
if #expired > 0 then
    redis.call('zremrangebyscore', KEYS[2], 0, ARGV[1])
    redis.call('incrby', KEYS[1], #expired)
end
return #expired
`)

var permitSetPermitsScript = redis.NewScript(`
local available = redis.call('get', KEYS[1])
if available == false then
    redis.call('set', KEYS[1], ARGV[1])
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
end
local acquired = redis.call('zcard', KEYS[3])
local total = tonumber(available) + acquired
if total ~= tonumber(ARGV[1]) then
    redis.call('incrby', KEYS[1], tonumber(ARGV[1]) - total)
    redis.call('publish', KEYS[2], ARGV[1])
end
return 1
`)

var permitAddPermitsScript = redis.NewScript(`
local available = tonumber(redis.call('get', KEYS[1]) or '0')
redis.call('set', KEYS[1], available + tonumber(ARGV[1]))
if tonumber(ARGV[1]) > 0 then
    redis.call('publish', KEYS[2], ARGV[1])
end
return 1
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

// GetPermits returns the configured pool size (available plus acquired).
func (s *RPermitExpirableSemaphore) GetPermits(ctx context.Context) (int64, error) {
	if err := s.purgeExpired(ctx); err != nil {
		return 0, err
	}
	available, err := s.AvailablePermits(ctx)
	if err != nil {
		return 0, err
	}
	acquired, err := s.rc().ZCard(ctx, s.timeoutKey).Result()
	if err != nil {
		return 0, err
	}
	return available + acquired, nil
}

// SetPermits changes the configured pool size while preserving acquisitions.
func (s *RPermitExpirableSemaphore) SetPermits(ctx context.Context, permits int64) error {
	if err := s.purgeExpired(ctx); err != nil {
		return err
	}
	return permitSetPermitsScript.Run(ctx, s.rc(),
		[]string{s.name, s.channel, s.timeoutKey}, permits).Err()
}

// AddPermits adjusts the available counter by permits.
func (s *RPermitExpirableSemaphore) AddPermits(ctx context.Context, permits int64) error {
	return permitAddPermitsScript.Run(ctx, s.rc(),
		[]string{s.name, s.channel}, permits).Err()
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
