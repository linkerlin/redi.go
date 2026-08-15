package redi

import (
	"context"

	"github.com/linkerlin/gotrycatch"
	"github.com/redis/go-redis/v9"
)

// RList is a distributed list backed by a Redis List (RPUSH / LPOP).
type RList struct {
	rc   *redis.Client
	name string
	key  string
}

func newRList(rc *redis.Client, name string) *RList {
	return &RList{rc: rc, name: name, key: "redi:list:" + name}
}

// Add appends values to the tail of the list.
func (l *RList) Add(ctx context.Context, values ...any) error {
	var addErr error
	tb := gotrycatch.Try(func() {
		if err := l.rc.RPush(ctx, l.key, values...).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { addErr = err })
	tb.Finally(func() {})
	return addErr
}

// Get returns the element at the given index (0-based).
func (l *RList) Get(ctx context.Context, index int64) (string, error) {
	var val string
	var getErr error
	tb := gotrycatch.Try(func() {
		v, err := l.rc.LIndex(ctx, l.key, index).Result()
		if err != nil {
			panic(err)
		}
		val = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { getErr = err })
	tb.Finally(func() {})
	return val, getErr
}

// Set replaces the element at index with value.
func (l *RList) Set(ctx context.Context, index int64, value any) error {
	var setErr error
	tb := gotrycatch.Try(func() {
		if err := l.rc.LSet(ctx, l.key, index, value).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { setErr = err })
	tb.Finally(func() {})
	return setErr
}

// Remove removes the first count occurrences of value.
func (l *RList) Remove(ctx context.Context, count int64, value any) error {
	var rmErr error
	tb := gotrycatch.Try(func() {
		if err := l.rc.LRem(ctx, l.key, count, value).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { rmErr = err })
	tb.Finally(func() {})
	return rmErr
}

// Size returns the number of elements in the list.
func (l *RList) Size(ctx context.Context) (int64, error) {
	var sz int64
	var szErr error
	tb := gotrycatch.Try(func() {
		v, err := l.rc.LLen(ctx, l.key).Result()
		if err != nil {
			panic(err)
		}
		sz = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { szErr = err })
	tb.Finally(func() {})
	return sz, szErr
}

// Range returns all elements in [start, stop].
func (l *RList) Range(ctx context.Context, start, stop int64) ([]string, error) {
	var vals []string
	var rangeErr error
	tb := gotrycatch.Try(func() {
		v, err := l.rc.LRange(ctx, l.key, start, stop).Result()
		if err != nil {
			panic(err)
		}
		vals = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { rangeErr = err })
	tb.Finally(func() {})
	return vals, rangeErr
}

// Clear removes the entire list.
func (l *RList) Clear(ctx context.Context) error {
	var clearErr error
	tb := gotrycatch.Try(func() {
		if err := l.rc.Del(ctx, l.key).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { clearErr = err })
	tb.Finally(func() {})
	return clearErr
}
