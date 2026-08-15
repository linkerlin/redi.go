package redi

import (
	"context"
	"fmt"
	"sync"
	"time"

	goexecutors "github.com/linkerlin/GoExecutors"
	"github.com/linkerlin/gotrycatch"
	"github.com/redis/go-redis/v9"
)

const (
	// defaultLockTTL is the default time-to-live for a distributed lock.
	defaultLockTTL = 30 * time.Second
	// watchdogInterval is how often the watchdog refreshes the lock TTL.
	watchdogInterval = 10 * time.Second
)

// RLock is a distributed, re-entrant lock backed by a Redis key.
// It uses a Lua script for atomic lock acquisition/release (identical to the
// Redisson approach) and a background watchdog goroutine managed by GoExecutors
// to renew the lease while the caller holds the lock.
type RLock struct {
	rc      *redis.Client
	name    string
	lockKey string
	mu sync.Mutex
	// cancelWatchdog cancels the watchdog goroutine when Unlock is called.
	cancelWatchdog context.CancelFunc
}

func newRLock(rc *redis.Client, name string) *RLock {
	return &RLock{
		rc:      rc,
		name:    name,
		lockKey: "redi:lock:" + name,
	}
}

// lockScript acquires the lock atomically.
// KEYS[1] = lock key, ARGV[1] = clientID, ARGV[2] = TTL in milliseconds.
var lockScript = redis.NewScript(`
if redis.call("exists", KEYS[1]) == 0 then
    redis.call("hset", KEYS[1], ARGV[1], 1)
    redis.call("pexpire", KEYS[1], ARGV[2])
    return 1
end
if redis.call("hexists", KEYS[1], ARGV[1]) == 1 then
    redis.call("hincrby", KEYS[1], ARGV[1], 1)
    redis.call("pexpire", KEYS[1], ARGV[2])
    return 1
end
return 0
`)

// unlockScript releases the lock atomically.
// KEYS[1] = lock key, ARGV[1] = clientID.
var unlockScript = redis.NewScript(`
if redis.call("hexists", KEYS[1], ARGV[1]) == 0 then
    return 0
end
local cnt = redis.call("hincrby", KEYS[1], ARGV[1], -1)
if cnt <= 0 then
    redis.call("del", KEYS[1])
end
return 1
`)

// renewScript extends the lock TTL if the caller still owns it.
// KEYS[1] = lock key, ARGV[1] = clientID, ARGV[2] = TTL in milliseconds.
var renewScript = redis.NewScript(`
if redis.call("hexists", KEYS[1], ARGV[1]) == 1 then
    redis.call("pexpire", KEYS[1], ARGV[2])
    return 1
end
return 0
`)

// Lock attempts to acquire the distributed lock, blocking until it succeeds
// or the context is cancelled. ttl controls how long the lock is held before
// the watchdog renews it.
func (l *RLock) Lock(ctx context.Context, clientID string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = defaultLockTTL
	}
	ttlMs := ttl.Milliseconds()

	var acquireErr error
	tb := gotrycatch.Try(func() {
		for {
			res, err := lockScript.Run(ctx, l.rc, []string{l.lockKey}, clientID, ttlMs).Int()
			if err != nil {
				panic(err)
			}
			if res == 1 {
				// Lock acquired – start watchdog.
				wdCtx, cancel := context.WithCancel(context.Background())
				l.mu.Lock()
				if l.cancelWatchdog != nil {
					l.cancelWatchdog()
				}
				l.cancelWatchdog = cancel
				l.mu.Unlock()
				l.startWatchdog(wdCtx, clientID, ttl)
				return
			}
			// Wait before retrying.
			select {
			case <-ctx.Done():
				panic(ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) {
		acquireErr = err
	})
	tb.Finally(func() {})
	return acquireErr
}

// TryLock attempts to acquire the lock once. Returns false immediately if the
// lock is already held by another client.
func (l *RLock) TryLock(ctx context.Context, clientID string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = defaultLockTTL
	}
	var acquired bool
	var tryErr error
	tb := gotrycatch.Try(func() {
		res, err := lockScript.Run(ctx, l.rc, []string{l.lockKey}, clientID, ttl.Milliseconds()).Int()
		if err != nil {
			panic(err)
		}
		if res == 1 {
			acquired = true
			wdCtx, cancel := context.WithCancel(context.Background())
			l.mu.Lock()
			if l.cancelWatchdog != nil {
				l.cancelWatchdog()
			}
			l.cancelWatchdog = cancel
			l.mu.Unlock()
			l.startWatchdog(wdCtx, clientID, ttl)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) {
		tryErr = err
	})
	tb.Finally(func() {})
	return acquired, tryErr
}

// Unlock releases the distributed lock held by clientID.
func (l *RLock) Unlock(ctx context.Context, clientID string) error {
	l.mu.Lock()
	cancel := l.cancelWatchdog
	l.cancelWatchdog = nil
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	var unlockErr error
	tb := gotrycatch.Try(func() {
		res, err := unlockScript.Run(ctx, l.rc, []string{l.lockKey}, clientID).Int()
		if err != nil {
			panic(err)
		}
		if res == 0 {
			panic(fmt.Errorf("redi: unlock attempt by non-owner clientID %q on lock %q", clientID, l.name))
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) {
		unlockErr = err
	})
	tb.Finally(func() {})
	return unlockErr
}

// startWatchdog runs a background task (via GoExecutors) that periodically
// renews the lock TTL to prevent expiry while the holder is still active.
func (l *RLock) startWatchdog(ctx context.Context, clientID string, ttl time.Duration) {
	pool, err := goexecutors.New[struct{}](
		goexecutors.WithCoreSize(1),
		goexecutors.WithMaxSize(1),
		goexecutors.WithQueueSize(1),
	)
	if err != nil {
		// Fall back: no watchdog — lock will expire on its own.
		return
	}

	_, _ = pool.Submit(ctx, func(ctx context.Context) (struct{}, error) {
		ticker := time.NewTicker(watchdogInterval)
		defer ticker.Stop()
		defer pool.Shutdown(context.Background()) //nolint:errcheck
		for {
			select {
			case <-ctx.Done():
				return struct{}{}, nil
			case <-ticker.C:
				tb := gotrycatch.Try(func() {
					_, err := renewScript.Run(ctx, l.rc,
						[]string{l.lockKey}, clientID, ttl.Milliseconds()).Int()
					if err != nil {
						panic(err)
					}
				})
				_ = gotrycatch.Catch[error](tb, func(_ error) {})
			}
		}
	})
}
