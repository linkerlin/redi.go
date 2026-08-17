package redi

import (
	"context"
	"time"
)

// RSpinLock is a re-entrant lock that busy-waits with short sleeps instead of
// pub/sub wakeups (Redisson SpinLock). Wire layout matches RLock (HASH +
// redisson_lock__channel:{name} unused for acquire).
type RSpinLock struct {
	*RLock
}

func newRSpinLock(c *Client, name string) *RSpinLock {
	return &RSpinLock{RLock: newRLock(c, name)}
}

// Lock spins until acquired or ctx cancelled. ttl semantics match RLock.
func (l *RSpinLock) Lock(ctx context.Context, clientID string, ttl time.Duration) error {
	for {
		ok, err := l.TryLock(ctx, clientID, ttl)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TryLockWait spins until wait elapses (no pub/sub).
func (l *RSpinLock) TryLockWait(ctx context.Context, clientID string, wait, ttl time.Duration) (bool, error) {
	if wait <= 0 {
		return l.TryLock(ctx, clientID, ttl)
	}
	deadline := time.Now().Add(wait)
	for {
		ok, err := l.TryLock(ctx, clientID, ttl)
		if err != nil || ok {
			return ok, err
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(minDuration(remain, 10*time.Millisecond)):
		}
	}
}
