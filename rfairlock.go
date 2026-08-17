package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const fairLockWaitTime = 5 * time.Minute
const nonReentrantFairMarker = "redi_non_reentrant"

// fairLockTryAcquireScript is RedissonFairLock.tryLockInnerAsync's
// EVAL_NULL_BOOLEAN variant. It never adds a new waiter to the queue.
var fairLockTryAcquireScript = redis.NewScript(`
while true do
    local firstThreadId2 = redis.call('lindex', KEYS[2], 0)
    if firstThreadId2 == false then
        break
    end
    local timeout = redis.call('zscore', KEYS[3], firstThreadId2)
    if timeout ~= false and tonumber(timeout) <= tonumber(ARGV[3]) then
        redis.call('zrem', KEYS[3], firstThreadId2)
        redis.call('lpop', KEYS[2])
    else
        break
    end
end
if (redis.call('exists', KEYS[1]) == 0)
        and ((redis.call('exists', KEYS[2]) == 0)
        or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then
    redis.call('lpop', KEYS[2])
    redis.call('zrem', KEYS[3], ARGV[2])
    local keys = redis.call('zrange', KEYS[3], 0, -1)
    for i = 1, #keys, 1 do
        redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i])
    end
    redis.call('hset', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    if ARGV[5] == '0' then
        return ARGV[6]
    end
    redis.call('hincrby', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
return 1
`)

// fairLockAcquireScript is RedissonFairLock.tryLockInnerAsync's EVAL_LONG
// variant. Contended callers are appended once to the FIFO queue.
var fairLockAcquireScript = redis.NewScript(`
while true do
    local firstThreadId2 = redis.call('lindex', KEYS[2], 0)
    if firstThreadId2 == false then
        break
    end
    local timeout = redis.call('zscore', KEYS[3], firstThreadId2)
    if timeout ~= false and tonumber(timeout) <= tonumber(ARGV[4]) then
        redis.call('zrem', KEYS[3], firstThreadId2)
        redis.call('lpop', KEYS[2])
    else
        break
    end
end
if (redis.call('exists', KEYS[1]) == 0)
        and ((redis.call('exists', KEYS[2]) == 0)
        or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then
    redis.call('lpop', KEYS[2])
    redis.call('zrem', KEYS[3], ARGV[2])
    local keys = redis.call('zrange', KEYS[3], 0, -1)
    for i = 1, #keys, 1 do
        redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i])
    end
    redis.call('hset', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    if ARGV[5] == '0' then
        return ARGV[6]
    end
    redis.call('hincrby', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
local timeout = redis.call('zscore', KEYS[3], ARGV[2])
if timeout ~= false then
    local ttl = redis.call('pttl', KEYS[1])
    return math.max(0, ttl)
end
local lastThreadId = redis.call('lindex', KEYS[2], -1)
local ttl
if lastThreadId ~= false and lastThreadId ~= ARGV[2]
        and redis.call('zscore', KEYS[3], lastThreadId) ~= false then
    ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4])
else
    ttl = redis.call('pttl', KEYS[1])
end
local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4])
if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then
    redis.call('rpush', KEYS[2], ARGV[2])
end
return ttl
`)

var fairLockAcquireFailedScript = redis.NewScript(`
local queue = redis.call('lrange', KEYS[1], 0, -1)
local i = 1
while i <= #queue and queue[i] ~= ARGV[1] do
    i = i + 1
end
i = i + 1
while i <= #queue do
    redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), queue[i])
    i = i + 1
end
redis.call('zrem', KEYS[2], ARGV[1])
redis.call('lrem', KEYS[1], 0, ARGV[1])
`)

var fairLockUnlockScript = redis.NewScript(`
while true do
    local firstThreadId2 = redis.call('lindex', KEYS[2], 0)
    if firstThreadId2 == false then
        break
    end
    local timeout = redis.call('zscore', KEYS[3], firstThreadId2)
    if timeout ~= false and tonumber(timeout) <= tonumber(ARGV[4]) then
        redis.call('zrem', KEYS[3], firstThreadId2)
        redis.call('lpop', KEYS[2])
    else
        break
    end
end
if (redis.call('exists', KEYS[1]) == 0) then
    local nextThreadId = redis.call('lindex', KEYS[2], 0)
    if nextThreadId ~= false then
        redis.call(ARGV[5], KEYS[4] .. ':' .. nextThreadId, ARGV[1])
    end
    return 1
end
if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then
    return nil
end
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1)
if (counter > 0) then
    redis.call('pexpire', KEYS[1], ARGV[2])
    return 0
end
redis.call('del', KEYS[1])
local nextThreadId = redis.call('lindex', KEYS[2], 0)
if nextThreadId ~= false then
    redis.call(ARGV[5], KEYS[4] .. ':' .. nextThreadId, ARGV[1])
end
return 1
`)

var fairLockForceUnlockScript = redis.NewScript(`
while true do
    local firstThreadId2 = redis.call('lindex', KEYS[2], 0)
    if firstThreadId2 == false then
        break
    end
    local timeout = redis.call('zscore', KEYS[3], firstThreadId2)
    if timeout ~= false and tonumber(timeout) <= tonumber(ARGV[2]) then
        redis.call('zrem', KEYS[3], firstThreadId2)
        redis.call('lpop', KEYS[2])
    else
        break
    end
end
if (redis.call('del', KEYS[1]) == 1) then
    local nextThreadId = redis.call('lindex', KEYS[2], 0)
    if nextThreadId ~= false then
        redis.call(ARGV[3], KEYS[4] .. ':' .. nextThreadId, ARGV[1])
    end
    return 1
end
return 0
`)

// RFairLock is Redisson's FIFO, re-entrant distributed lock. The lock HASH
// uses the raw name; waiters use redisson_lock_queue:{name} (LIST) and
// redisson_lock_timeout:{name} (ZSET).
type RFairLock struct {
	*RLock
	queueKey   string
	timeoutKey string
	reentrant  bool
}

func newRFairLock(c *Client, name string) *RFairLock {
	return &RFairLock{
		RLock:      newRLock(c, name),
		queueKey:   prefixName("redisson_lock_queue", name),
		timeoutKey: prefixName("redisson_lock_timeout", name),
		reentrant:  true,
	}
}

// Lock acquires the lock in FIFO order, blocking until success or ctx
// cancellation.
func (l *RFairLock) Lock(ctx context.Context, clientID string, ttl time.Duration) error {
	lease, watchdog := l.lease(ttl)
	acquired, err := l.runFairAcquire(ctx, clientID, lease, false)
	if err != nil {
		return err
	}
	if acquired {
		if watchdog {
			l.startRenewer(clientID)
		}
		return nil
	}

	sub := l.subscribe(ctx, l.channel+":"+clientID)
	defer sub.Close() //nolint:errcheck // connection teardown
	if _, err := sub.Receive(ctx); err != nil {
		l.removeWaiter(context.Background(), clientID)
		return err
	}
	wake := sub.Channel()
	for {
		acquired, err = l.runFairAcquire(ctx, clientID, lease, false)
		if err != nil {
			l.removeWaiter(context.Background(), clientID)
			return err
		}
		if acquired {
			if watchdog {
				l.startRenewer(clientID)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			l.removeWaiter(context.Background(), clientID)
			return ctx.Err()
		case <-wake:
		case <-time.After(time.Second):
		}
	}
}

// TryLock attempts one acquisition without joining the wait queue.
func (l *RFairLock) TryLock(ctx context.Context, clientID string, ttl time.Duration) (bool, error) {
	lease, watchdog := l.lease(ttl)
	acquired, err := l.runFairAcquire(ctx, clientID, lease, true)
	if err != nil || !acquired {
		return acquired, err
	}
	if watchdog {
		l.startRenewer(clientID)
	}
	return true, nil
}

// TryLockWait joins the FIFO queue and waits up to wait for the lock.
func (l *RFairLock) TryLockWait(
	ctx context.Context, clientID string, wait, ttl time.Duration,
) (bool, error) {
	if wait <= 0 {
		return l.TryLock(ctx, clientID, ttl)
	}
	deadline := time.Now().Add(wait)
	lease, watchdog := l.lease(ttl)
	acquired, err := l.runFairAcquire(ctx, clientID, lease, false)
	if err != nil {
		return false, err
	}
	if acquired {
		if watchdog {
			l.startRenewer(clientID)
		}
		return true, nil
	}

	sub := l.subscribe(ctx, l.channel+":"+clientID)
	defer sub.Close() //nolint:errcheck // connection teardown
	if _, err := sub.Receive(ctx); err != nil {
		l.removeWaiter(context.Background(), clientID)
		return false, err
	}
	wake := sub.Channel()
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			l.removeWaiter(context.Background(), clientID)
			return false, nil
		}
		select {
		case <-ctx.Done():
			l.removeWaiter(context.Background(), clientID)
			return false, ctx.Err()
		case <-wake:
		case <-time.After(minDuration(remaining, time.Second)):
		}
		if time.Now().After(deadline) {
			l.removeWaiter(context.Background(), clientID)
			return false, nil
		}
		acquired, err = l.runFairAcquire(ctx, clientID, lease, false)
		if err != nil {
			l.removeWaiter(context.Background(), clientID)
			return false, err
		}
		if acquired {
			if watchdog {
				l.startRenewer(clientID)
			}
			return true, nil
		}
	}
}

// Unlock releases one hold and wakes only the current queue head.
func (l *RFairLock) Unlock(ctx context.Context, clientID string) error {
	n, err := fairLockUnlockScript.Run(ctx, l.rc(),
		[]string{l.name, l.queueKey, l.timeoutKey, l.channel},
		unlockMsg, l.c.cfg.LockWatchdogTimeout.Milliseconds(), clientID,
		time.Now().UnixMilli(), "publish").Int()
	if err == redis.Nil {
		return ErrLockNotHeld
	}
	if err != nil {
		return err
	}
	if n == 1 {
		l.stopRenewer(clientID)
	}
	return nil
}

// ForceUnlock deletes the lock regardless of owner and wakes the queue head.
func (l *RFairLock) ForceUnlock(ctx context.Context) (bool, error) {
	n, err := fairLockForceUnlockScript.Run(ctx, l.rc(),
		[]string{l.name, l.queueKey, l.timeoutKey, l.channel},
		unlockMsg, time.Now().UnixMilli(), "publish").Int()
	l.stopAllRenewers()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// IsLocked reports whether anyone holds the lock.
func (l *RFairLock) IsLocked(ctx context.Context) (bool, error) {
	return l.RLock.IsLocked(ctx)
}

// IsHeldBy reports whether clientID holds the lock.
func (l *RFairLock) IsHeldBy(ctx context.Context, clientID string) (bool, error) {
	return l.RLock.IsHeldBy(ctx, clientID)
}

// HoldCount returns the re-entrancy count held by clientID.
func (l *RFairLock) HoldCount(ctx context.Context, clientID string) (int64, error) {
	return l.RLock.HoldCount(ctx, clientID)
}

// RemainTimeToLive returns the remaining lock lease.
func (l *RFairLock) RemainTimeToLive(ctx context.Context) (time.Duration, error) {
	return l.RLock.RemainTimeToLive(ctx)
}

func (l *RFairLock) lease(ttl time.Duration) (time.Duration, bool) {
	if ttl > 0 {
		return ttl, false
	}
	return l.c.cfg.LockWatchdogTimeout, true
}

func (l *RFairLock) runFairAcquire(
	ctx context.Context,
	clientID string,
	lease time.Duration,
	tryOnly bool,
) (bool, error) {
	script := fairLockAcquireScript
	reentrant := 0
	if l.reentrant {
		reentrant = 1
	}
	args := []any{
		lease.Milliseconds(), clientID, fairLockWaitTime.Milliseconds(),
		time.Now().UnixMilli(), reentrant, nonReentrantFairMarker,
	}
	if tryOnly {
		script = fairLockTryAcquireScript
		args[2], args[3] = args[3], args[2]
	}
	result, err := script.Run(ctx, l.rc(), []string{l.name, l.queueKey, l.timeoutKey},
		args...).Result()
	if err == redis.Nil {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if result == nonReentrantFairMarker {
		return false, ErrLockReentrant
	}
	return false, nil
}

func (l *RFairLock) removeWaiter(ctx context.Context, clientID string) {
	cleanupCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := fairLockAcquireFailedScript.Run(cleanupCtx, l.rc(),
		[]string{l.queueKey, l.timeoutKey},
		clientID, fairLockWaitTime.Milliseconds()).Err()
	if err != nil && err != redis.Nil {
		l.c.logf("fair lock %q waiter cleanup: %v", l.name, err)
	}
}
