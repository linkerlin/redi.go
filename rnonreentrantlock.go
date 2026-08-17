package redi

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RNonReentrantLock is a distributed lock that refuses re-entry by the same
// holder (Redisson NonReentrantLock). HASH field still stores the holder id
// with count always 1.
type RNonReentrantLock struct {
	rObject
	channel  string
	mu       sync.Mutex
	renewers map[string]context.CancelFunc
}

func newRNonReentrantLock(c *Client, name string) *RNonReentrantLock {
	return &RNonReentrantLock{
		rObject:  rObject{c: c, name: name},
		channel:  prefixName("redisson_lock__channel", name),
		renewers: make(map[string]context.CancelFunc),
	}
}

var nonReentrantAcquireScript = redis.NewScript(`
if (redis.call('exists', KEYS[1]) == 0) then
    redis.call('hset', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
return redis.call('pttl', KEYS[1])
`)

var nonReentrantUnlockScript = redis.NewScript(`
if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then
    return -1
end
redis.call('del', KEYS[1])
redis.call('publish', KEYS[2], ARGV[1])
return 0
`)

// Lock blocks until acquired or ctx cancelled.
func (l *RNonReentrantLock) Lock(ctx context.Context, clientID string, ttl time.Duration) error {
	ok, err := l.TryLockWait(ctx, clientID, time.Hour*24*365, ttl)
	if err != nil {
		return err
	}
	if !ok {
		return ctx.Err()
	}
	return nil
}

// TryLock attempts one acquisition.
func (l *RNonReentrantLock) TryLock(ctx context.Context, clientID string, ttl time.Duration) (bool, error) {
	lease := l.c.cfg.LockWatchdogTimeout
	watchdog := ttl <= 0
	if ttl > 0 {
		lease = ttl
	}
	_, err := nonReentrantAcquireScript.Run(ctx, l.rc(), []string{l.name},
		lease.Milliseconds(), clientID).Result()
	if err == redis.Nil {
		if watchdog {
			l.startRenewer(clientID, lease)
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

// TryLockWait waits up to wait for the lock.
func (l *RNonReentrantLock) TryLockWait(ctx context.Context, clientID string, wait, ttl time.Duration) (bool, error) {
	if wait <= 0 {
		return l.TryLock(ctx, clientID, ttl)
	}
	deadline := time.Now().Add(wait)
	lease := l.c.cfg.LockWatchdogTimeout
	watchdog := ttl <= 0
	if ttl > 0 {
		lease = ttl
	}
	for {
		_, err := nonReentrantAcquireScript.Run(ctx, l.rc(), []string{l.name},
			lease.Milliseconds(), clientID).Result()
		if err == redis.Nil {
			if watchdog {
				l.startRenewer(clientID, lease)
			}
			return true, nil
		}
		if err != nil {
			return false, err
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return false, nil
		}
		sub := l.subscribe(ctx, l.channel)
		wake := sub.Channel()
		select {
		case <-ctx.Done():
			_ = sub.Close()
			return false, ctx.Err()
		case <-wake:
		case <-time.After(minDuration(remain, time.Second)):
		}
		_ = sub.Close()
	}
}

// Unlock releases the lock when held by clientID.
func (l *RNonReentrantLock) Unlock(ctx context.Context, clientID string) error {
	lease := l.c.cfg.LockWatchdogTimeout
	n, err := nonReentrantUnlockScript.Run(ctx, l.rc(),
		[]string{l.name, l.channel}, unlockMsg, lease.Milliseconds(), clientID).Int()
	if err != nil {
		return err
	}
	if n < 0 {
		return ErrLockNotHeld
	}
	l.stopRenewer(clientID)
	return nil
}

// ForceUnlock deletes the lock regardless of owner and wakes waiters.
func (l *RNonReentrantLock) ForceUnlock(ctx context.Context) (bool, error) {
	n, err := lockForceUnlockScript.Run(ctx, l.rc(),
		[]string{l.name, l.channel}, unlockMsg).Int()
	l.stopAllRenewers()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// IsLocked reports whether anyone holds the lock.
func (l *RNonReentrantLock) IsLocked(ctx context.Context) (bool, error) {
	return l.Exists(ctx)
}

// IsHeldBy reports whether clientID holds the lock.
func (l *RNonReentrantLock) IsHeldBy(ctx context.Context, clientID string) (bool, error) {
	return l.rc().HExists(ctx, l.name, clientID).Result()
}

// RemainTimeToLive returns the remaining lock lease.
func (l *RNonReentrantLock) RemainTimeToLive(ctx context.Context) (time.Duration, error) {
	return l.rc().PTTL(ctx, l.name).Result()
}

func (l *RNonReentrantLock) startRenewer(field string, lease time.Duration) {
	l.mu.Lock()
	if _, ok := l.renewers[field]; ok {
		l.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(l.c.ctx)
	l.renewers[field] = cancel
	l.mu.Unlock()
	go func() {
		ticker := time.NewTicker(lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := lockRenewScript.Run(ctx, l.rc(), []string{l.name},
					field, lease.Milliseconds()).Int()
				if err != nil || n == 0 {
					l.stopRenewer(field)
					return
				}
			}
		}
	}()
}

func (l *RNonReentrantLock) stopRenewer(field string) {
	l.mu.Lock()
	if c, ok := l.renewers[field]; ok {
		c()
		delete(l.renewers, field)
	}
	l.mu.Unlock()
}

func (l *RNonReentrantLock) stopAllRenewers() {
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
