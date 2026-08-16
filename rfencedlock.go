package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RFencedLock is a re-entrant distributed lock whose every acquisition
// increments a fencing token, wire-compatible with Redisson's
// RedissonFencedLock:
//
//	HASH {name}                       holder counts (same as RLock)
//	STRING redisson_lock_token:{name} monotonic fencing token (INCR per
//	                                 acquisition, INCLUDING re-entries)
//
// The guarded service should reject operations carrying a token smaller
// than the last seen one (the classic fencing answer to lock expiry).
type RFencedLock struct {
	*RLock
	tokenKey string
}

func newRFencedLock(c *Client, name string) *RFencedLock {
	return &RFencedLock{
		RLock:    newRLock(c, name),
		tokenKey: prefixName("redisson_lock_token", name),
	}
}

// fencedAcquireScript is Redisson's RedissonFencedLock.tryLockInnerAsync:
// success returns {-1, token}; contention returns {pttl, -1}.
var fencedAcquireScript = redis.NewScript(`
if (redis.call('exists', KEYS[1]) == 0) or (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    local token = redis.call('incr', KEYS[2]);
    redis.call('hincrby', KEYS[1], ARGV[2], 1);
    redis.call('pexpire', KEYS[1], ARGV[1]);
    return {-1, token};
end
return {redis.call('pttl', KEYS[1]), -1};
`)

// runFencedAcquire returns (token>0, nil) on success; (0, nil) when held
// by someone else.
func (l *RFencedLock) runFencedAcquire(ctx context.Context, clientID string, lease time.Duration) (int64, error) {
	res, err := fencedAcquireScript.Run(ctx, l.rc(), []string{l.name, l.tokenKey},
		lease.Milliseconds(), clientID).Slice()
	if err != nil {
		return 0, err
	}
	ttl, _ := res[0].(int64)
	token, _ := res[1].(int64)
	if ttl == -1 {
		return token, nil // acquired
	}
	return 0, nil
}

// TryLockAndGetToken attempts one acquisition, returning the new fencing
// token (0 when the lock is held by someone else).
func (l *RFencedLock) TryLockAndGetToken(ctx context.Context, clientID string, ttl time.Duration) (int64, error) {
	lease := l.c.cfg.LockWatchdogTimeout
	watchdog := ttl <= 0
	if ttl > 0 {
		lease = ttl
	}
	token, err := l.runFencedAcquire(ctx, clientID, lease)
	if err != nil || token == 0 {
		return 0, err
	}
	if watchdog {
		l.startRenewer(clientID)
	}
	return token, nil
}

// LockAndGetToken blocks until acquired, returning the fencing token.
func (l *RFencedLock) LockAndGetToken(ctx context.Context, clientID string, ttl time.Duration) (int64, error) {
	lease := l.c.cfg.LockWatchdogTimeout
	watchdog := ttl <= 0
	if ttl > 0 {
		lease = ttl
	}
	token, err := l.runFencedAcquire(ctx, clientID, lease)
	if err != nil {
		return 0, err
	}
	if token > 0 {
		if watchdog {
			l.startRenewer(clientID)
		}
		return token, nil
	}

	sub := l.subscribe(ctx, l.channel)
	defer sub.Close() //nolint:errcheck // connection teardown
	wake := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-wake:
		case <-time.After(time.Second):
		}
		token, err := l.runFencedAcquire(ctx, clientID, lease)
		if err != nil {
			return 0, err
		}
		if token > 0 {
			if watchdog {
				l.startRenewer(clientID)
			}
			return token, nil
		}
	}
}

// GetToken returns the current fencing token (0 when never acquired).
func (l *RFencedLock) GetToken(ctx context.Context) (int64, error) {
	v, err := l.rc().Get(ctx, l.tokenKey).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return parseInt64(v), nil
}
