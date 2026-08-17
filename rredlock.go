package redi

import (
	"context"
	"errors"
	"time"
)

// ErrRedLockFailed is returned when the majority cannot be acquired within
// the requested wait budget.
var ErrRedLockFailed = errors.New("redi: redlock majority acquisition failed")

// RRedLock implements RedissonRedLock's majority policy over independent
// RLock members. Members may belong to different Client instances.
type RRedLock struct {
	locks []*RLock
}

// TryLock attempts every member once and succeeds when a strict majority is
// acquired. A failed minority is tolerated; failure rolls back acquired locks.
func (r *RRedLock) TryLock(
	ctx context.Context, clientID string, ttl time.Duration,
) (bool, error) {
	return r.tryLock(ctx, clientID, ttl, 0)
}

// TryLockWait attempts to acquire a strict majority within wait. Like
// RedissonRedLock, each member receives at most remain/len(locks) wait time.
func (r *RRedLock) TryLockWait(
	ctx context.Context, clientID string, wait, ttl time.Duration,
) (bool, error) {
	if wait <= 0 {
		return r.TryLock(ctx, clientID, ttl)
	}
	return r.tryLock(ctx, clientID, ttl, wait)
}

// Lock acquires a strict majority or returns ErrRedLockFailed when wait
// elapses. Context cancellation is returned unchanged.
func (r *RRedLock) Lock(
	ctx context.Context, clientID string, ttl, wait time.Duration,
) error {
	ok, err := r.TryLockWait(ctx, clientID, wait, ttl)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRedLockFailed
	}
	return nil
}

// Unlock best-effort unlocks every member, matching RedissonRedLock. Members
// held by another owner are ignored.
func (r *RRedLock) Unlock(ctx context.Context, clientID string) error {
	var firstErr error
	for _, lock := range r.locks {
		err := lock.Unlock(ctx, clientID)
		if err != nil && !errors.Is(err, ErrLockNotHeld) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *RRedLock) tryLock(
	ctx context.Context, clientID string, ttl, wait time.Duration,
) (bool, error) {
	if len(r.locks) == 0 {
		return false, ErrRedLockFailed
	}
	required := len(r.locks)/2 + 1
	acquired := make([]*RLock, 0, len(r.locks))
	var firstErr error
	var deadline time.Time
	if wait > 0 {
		deadline = time.Now().Add(wait)
	}

	attemptTTL := ttl
	if ttl > 0 && wait > 0 {
		attemptTTL = 2 * wait
	}
	for index, lock := range r.locks {
		var (
			ok  bool
			err error
		)
		if wait > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				r.rollback(ctx, clientID, acquired)
				return false, firstErr
			}
			memberWait := remaining / time.Duration(len(r.locks))
			if memberWait < time.Millisecond {
				memberWait = time.Millisecond
			}
			ok, err = lock.TryLockWait(ctx, clientID, memberWait, attemptTTL)
		} else {
			ok, err = lock.TryLock(ctx, clientID, attemptTTL)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if ok {
			acquired = append(acquired, lock)
		}
		remainingMembers := len(r.locks) - index - 1
		if len(acquired)+remainingMembers < required {
			r.rollback(ctx, clientID, acquired)
			return false, firstErr
		}
	}
	if len(acquired) < required {
		r.rollback(ctx, clientID, acquired)
		return false, firstErr
	}
	if ttl > 0 && attemptTTL != ttl {
		for _, lock := range acquired {
			if _, err := lock.Expire(ctx, ttl); err != nil {
				r.rollback(ctx, clientID, acquired)
				return false, err
			}
		}
	}
	return true, nil
}

func (r *RRedLock) rollback(
	ctx context.Context, clientID string, acquired []*RLock,
) {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, lock := range acquired {
		_ = lock.Unlock(rollbackCtx, clientID) //nolint:errcheck // best effort
	}
}
