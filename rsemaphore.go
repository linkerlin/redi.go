package redi

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RSemaphore is a distributed counting semaphore backed by a Redis String
// (permit counter), Redisson wire-compatible: channel redisson_sc:{name}
// wakes blocked acquirers on release.
type RSemaphore struct {
	rObject
	channel string
}

func newRSemaphore(c *Client, name string) *RSemaphore {
	return &RSemaphore{
		rObject: rObject{c: c, name: name},
		channel: prefixName("redisson_sc", name),
	}
}

var semAcquireScript = redis.NewScript(`
local value = redis.call('get', KEYS[1])
if (value ~= false and tonumber(value) >= tonumber(ARGV[1])) then
    redis.call('decrby', KEYS[1], ARGV[1])
    return 1
end
return 0
`)

var semReleaseScript = redis.NewScript(`
local value = redis.call('incrby', KEYS[1], ARGV[1])
redis.call('publish', KEYS[2], ARGV[1])
return value
`)

var semDrainScript = redis.NewScript(`
local value = redis.call('get', KEYS[1])
if value == false then
    return 0
end
redis.call('set', KEYS[1], 0)
return value
`)

var semReleaseIfExistsScript = redis.NewScript(`
if redis.call('exists', KEYS[1]) == 0 then
    return 0
end
local value = redis.call('incrby', KEYS[1], ARGV[1])
redis.call('publish', KEYS[2], value)
return 1
`)

// TrySetPermits initializes the semaphore only when it does not exist.
func (s *RSemaphore) TrySetPermits(ctx context.Context, permits int64) (bool, error) {
	return s.rc().SetNX(ctx, s.name, permits, 0).Result()
}

// TryAcquire takes permits when available, returning false otherwise.
func (s *RSemaphore) TryAcquire(ctx context.Context, permits int64) (bool, error) {
	n, err := semAcquireScript.Run(ctx, s.rc(), []string{s.name}, permits).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// TryAcquireWait tries to acquire until wait elapses or ctx is cancelled.
// Wakes on release via the redisson_sc channel, with a 1s fallback poll.
func (s *RSemaphore) TryAcquireWait(ctx context.Context, permits int64, wait time.Duration) (bool, error) {
	if wait <= 0 {
		return s.TryAcquire(ctx, permits)
	}
	ok, err := s.TryAcquire(ctx, permits)
	if err != nil || ok {
		return ok, err
	}
	deadline := time.Now().Add(wait)
	sub := s.subscribe(ctx, s.channel)
	defer sub.Close() //nolint:errcheck // connection teardown
	wake := sub.Channel()
	for {
		remain := time.Until(deadline)
		if remain <= 0 {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-wake:
		case <-time.After(minDuration(remain, time.Second)):
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		ok, err = s.TryAcquire(ctx, permits)
		if err != nil || ok {
			return ok, err
		}
	}
}

// Acquire blocks until permits are available or ctx is cancelled.
// Wakes on release via the redisson_sc channel, with a 1s fallback poll.
func (s *RSemaphore) Acquire(ctx context.Context, permits int64) error {
	ok, err := s.TryAcquire(ctx, permits)
	if err != nil || ok {
		return err
	}
	sub := s.subscribe(ctx, s.channel)
	defer sub.Close() //nolint:errcheck // connection teardown
	wake := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-time.After(time.Second):
		}
		ok, err := s.TryAcquire(ctx, permits)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
}

// Release returns permits and wakes blocked acquirers.
func (s *RSemaphore) Release(ctx context.Context, permits int64) error {
	_, err := semReleaseScript.Run(ctx, s.rc(), []string{s.name, s.channel}, permits).Int()
	return err
}

// ReleaseIfExists returns permits only when the semaphore is initialized.
func (s *RSemaphore) ReleaseIfExists(
	ctx context.Context, permits int64,
) (bool, error) {
	if permits <= 0 {
		return false, nil
	}
	n, err := semReleaseIfExistsScript.Run(ctx, s.rc(),
		[]string{s.name, s.channel}, permits).Int()
	return n == 1, err
}

// AvailablePermits returns the current permit count (0 when uninitialized).
func (s *RSemaphore) AvailablePermits(ctx context.Context) (int64, error) {
	v, err := s.rc().Get(ctx, s.name).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return parseInt64(v), nil
}

// DrainPermits takes all available permits and returns how many were taken.
func (s *RSemaphore) DrainPermits(ctx context.Context) (int64, error) {
	n, err := semDrainScript.Run(ctx, s.rc(), []string{s.name}).Int()
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

// AddPermits increases (or, when negative, decreases) the permit counter.
func (s *RSemaphore) AddPermits(ctx context.Context, permits int64) error {
	return s.rc().IncrBy(ctx, s.name, permits).Err()
}

func parseInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
