package redi

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrLockNotHeld is returned by Unlock when the caller does not own the lock.
var ErrLockNotHeld = errors.New("redi: lock not held")

// unlockMsg is Redisson's LockPubSub.UNLOCK_MESSAGE (0).
const unlockMsg = "0"

// lockAcquireScript atomically acquires the lock or returns the remaining
// PTTTL. KEYS[1]=lock name, ARGV[1]=lease ms, ARGV[2]=field (holder id).
// Returns nil when acquired (Redisson semantics).
var lockAcquireScript = redis.NewScript(`
if (redis.call('exists', KEYS[1]) == 0) or (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    redis.call('hincrby', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
return redis.call('pttl', KEYS[1])
`)

// lockRenewScript extends the lease when the holder still owns the lock.
var lockRenewScript = redis.NewScript(`
if (redis.call('hexists', KEYS[1], ARGV[1]) == 1) then
    redis.call('pexpire', KEYS[1], ARGV[2])
    return 1
end
return 0
`)

// lockUnlockScript decrements the hold count, publishing unlockMsg when the
// lock is fully released. Returns -1 (not owner), remaining count, or 0.
// KEYS[1]=lock name, KEYS[2]=channel, ARGV[1]=unlock msg, ARGV[2]=lease ms,
// ARGV[3]=field.
var lockUnlockScript = redis.NewScript(`
if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then
    return -1
end
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1)
if (counter > 0) then
    redis.call('pexpire', KEYS[1], ARGV[2])
    return counter
end
redis.call('del', KEYS[1])
redis.call('publish', KEYS[2], ARGV[1])
return 0
`)

var lockForceUnlockScript = redis.NewScript(`
if (redis.call('del', KEYS[1]) == 1) then
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
end
return 0
`)

// RLock is a distributed re-entrant lock, wire-compatible with Redisson's
// RLock: a Redis HASH at the raw name whose fields are holder ids with
// re-entrancy counts, and a pub/sub channel redisson_lock__channel:{name}
// used to wake up waiting acquirers on release (~0ms wakeup vs 50-100ms
// polling).
//
// The caller-supplied clientID is used as the HASH field directly; to
// interoperate with Java Redisson use "uuid:threadId" shaped ids.
//
// Lock semantics (matching Redisson):
//   - ttl <= 0: watchdog mode – lease = Config.LockWatchdogTimeout (30s
//     default), renewed every lease/3 until Unlock.
//   - ttl > 0: fixed lease, no renewal, lock expires on its own.
type RLock struct {
	rObject
	channel string

	mu       sync.Mutex
	renewers map[string]context.CancelFunc // field -> watchdog cancel
}

func newRLock(c *Client, name string) *RLock {
	return &RLock{
		rObject:  rObject{c: c, name: name},
		channel:  prefixName("redisson_lock__channel", name),
		renewers: make(map[string]context.CancelFunc),
	}
}

// Lock acquires the lock, blocking until success or ctx cancellation.
// See the RLock doc for ttl semantics.
func (l *RLock) Lock(ctx context.Context, clientID string, ttl time.Duration) error {
	lease := l.c.cfg.LockWatchdogTimeout
	watchdog := ttl <= 0
	if ttl > 0 {
		lease = ttl
	}

	res, err := l.runAcquire(ctx, clientID, lease)
	if err != nil {
		return err
	}
	if res {
		if watchdog {
			l.startRenewer(clientID)
		}
		return nil
	}

	// Contended: subscribe for unlock wake-ups, retry with 1s fallback.
	sub := l.subscribe(ctx, l.channel)
	defer sub.Close()
	wake := sub.Channel()
	for {
		res, err = l.runAcquire(ctx, clientID, lease)
		if err != nil {
			return err
		}
		if res {
			if watchdog {
				l.startRenewer(clientID)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-time.After(time.Second):
		}
	}
}

// TryLock attempts to acquire the lock once, returning false immediately
// when held by someone else.
func (l *RLock) TryLock(ctx context.Context, clientID string, ttl time.Duration) (bool, error) {
	lease := l.c.cfg.LockWatchdogTimeout
	watchdog := ttl <= 0
	if ttl > 0 {
		lease = ttl
	}
	res, err := l.runAcquire(ctx, clientID, lease)
	if err != nil {
		return false, err
	}
	if res {
		if watchdog {
			l.startRenewer(clientID)
		}
		return true, nil
	}
	return false, nil
}

// Unlock releases one hold. When the hold count reaches zero the key is
// deleted and waiting acquirers are woken. The watchdog (if any) is only
// cancelled after ownership is verified, so a stray Unlock by a non-owner
// cannot kill the real holder's renewal.
func (l *RLock) Unlock(ctx context.Context, clientID string) error {
	lease := l.c.cfg.LockWatchdogTimeout
	n, err := lockUnlockScript.Run(ctx, l.rc(),
		[]string{l.name, l.channel}, unlockMsg, lease.Milliseconds(), clientID).Int()
	if err != nil {
		return err
	}
	if n < 0 {
		return ErrLockNotHeld
	}
	if n == 0 {
		l.stopRenewer(clientID)
	}
	return nil
}

// ForceUnlock deletes the lock regardless of owner and wakes all waiters.
func (l *RLock) ForceUnlock(ctx context.Context) (bool, error) {
	n, err := lockForceUnlockScript.Run(ctx, l.rc(),
		[]string{l.name, l.channel}, unlockMsg).Int()
	l.stopAllRenewers()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// IsLocked reports whether the lock is held by anyone.
func (l *RLock) IsLocked(ctx context.Context) (bool, error) {
	return l.Exists(ctx)
}

// IsHeldBy reports whether clientID holds the lock.
func (l *RLock) IsHeldBy(ctx context.Context, clientID string) (bool, error) {
	return l.rc().HExists(ctx, l.name, clientID).Result()
}

// HoldCount returns the re-entrancy count held by clientID (0 if none).
func (l *RLock) HoldCount(ctx context.Context, clientID string) (int64, error) {
	n, err := l.rc().HGet(ctx, l.name, clientID).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
}

// runAcquire returns acquired=true when the script returned nil
// (go-redis surfaces a Lua nil reply as the redis.Nil error).
func (l *RLock) runAcquire(ctx context.Context, clientID string, lease time.Duration) (bool, error) {
	res, err := lockAcquireScript.Run(ctx, l.rc(), []string{l.name},
		lease.Milliseconds(), clientID).Result()
	if err == redis.Nil {
		return true, nil // acquired
	}
	if err != nil {
		return false, err
	}
	_ = res // remaining pttl of the current holder
	return false, nil
}

// startRenewer launches (once per field) the watchdog goroutine that renews
// the lease at LockWatchdogTimeout/3 intervals until cancelled, the lock is
// released, or ownership is lost.
func (l *RLock) startRenewer(field string) {
	l.mu.Lock()
	if _, ok := l.renewers[field]; ok {
		l.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(l.c.ctx)
	l.renewers[field] = cancel
	l.mu.Unlock()

	lease := l.c.cfg.LockWatchdogTimeout
	interval := lease / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	go func() {
		defer func() {
			l.mu.Lock()
			delete(l.renewers, field)
			l.mu.Unlock()
		}()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := lockRenewScript.Run(ctx, l.rc(), []string{l.name},
					field, lease.Milliseconds()).Int()
				if err != nil {
					// Transient error – keep trying while the lease holds.
					l.c.logf("lock %q renew: %v", l.name, err)
					continue
				}
				if n == 0 {
					return // ownership lost (expired or force-unlocked)
				}
			}
		}
	}()
}

func (l *RLock) stopRenewer(field string) {
	l.mu.Lock()
	cancel := l.renewers[field]
	delete(l.renewers, field)
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (l *RLock) stopAllRenewers() {
	l.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(l.renewers))
	for _, cancel := range l.renewers {
		cancels = append(cancels, cancel)
	}
	l.renewers = make(map[string]context.CancelFunc)
	l.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
