package redi

import (
	"context"
	"errors"
	"time"
)

// ErrMultiLockFailed is returned by Lock when the context ends before all
// member locks are acquired (acquired members are rolled back).
var ErrMultiLockFailed = errors.New("redi: multilock acquisition failed")

// RMultiLock groups RLocks into one all-or-nothing lock, mirroring
// Redisson's RedissonMultiLock orchestration: acquire members in order
// within the wait budget; on any failure roll back every lock this call
// acquired. Wire-compatible by construction (plain RLock members).
type RMultiLock struct {
	locks []*RLock
}

// TryLock attempts to acquire every member once (no waiting). All
// acquired members are rolled back on the first failure.
func (m *RMultiLock) TryLock(ctx context.Context, clientID string, ttl time.Duration) (bool, error) {
	for _, l := range m.locks {
		ok, err := l.TryLock(ctx, clientID, ttl)
		if err != nil {
			m.rollback(ctx, clientID)
			return false, err
		}
		if !ok {
			m.rollback(ctx, clientID)
			return false, nil
		}
	}
	return true, nil
}

// Lock blocks until every member is acquired, the wait budget expires
// (ErrMultiLockFailed) or ctx ends. Reuses the Redisson lease-time
// convention: the effective per-member wait is bounded by waitTime.
func (m *RMultiLock) Lock(ctx context.Context, clientID string, ttl, waitTime time.Duration) error {
	if waitTime <= 0 {
		waitTime = time.Minute
	}
	deadline := time.Now().Add(waitTime)
	for _, l := range m.locks {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			m.rollback(ctx, clientID)
			return ErrMultiLockFailed
		}
		lctx, cancel := context.WithTimeout(ctx, remaining)
		err := l.Lock(lctx, clientID, ttl)
		cancel()
		if err != nil {
			m.rollback(ctx, clientID)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrMultiLockFailed
		}
	}
	return nil
}

// Unlock releases every member.
func (m *RMultiLock) Unlock(ctx context.Context, clientID string) error {
	var firstErr error
	for _, l := range m.locks {
		if err := l.Unlock(ctx, clientID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// rollback releases only the members this process could have locked.
func (m *RMultiLock) rollback(ctx context.Context, clientID string) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, l := range m.locks {
		_ = l.Unlock(rctx, clientID) //nolint:errcheck // best-effort rollback
	}
}
