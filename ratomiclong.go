package redi

import (
	"context"

	"github.com/linkerlin/gotrycatch"
	"github.com/redis/go-redis/v9"
)

// RAtomicLong is a distributed atomic 64-bit integer backed by a Redis String.
type RAtomicLong struct {
	rc   *redis.Client
	name string
	key  string
}

func newRAtomicLong(rc *redis.Client, name string) *RAtomicLong {
	return &RAtomicLong{rc: rc, name: name, key: "redi:atomic:" + name}
}

// Get returns the current value.
func (a *RAtomicLong) Get(ctx context.Context) (int64, error) {
	var val int64
	var getErr error
	tb := gotrycatch.Try(func() {
		v, err := a.rc.Get(ctx, a.key).Int64()
		if err != nil {
			panic(err)
		}
		val = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { getErr = err })
	tb.Finally(func() {})
	return val, getErr
}

// Set replaces the value.
func (a *RAtomicLong) Set(ctx context.Context, value int64) error {
	var setErr error
	tb := gotrycatch.Try(func() {
		if err := a.rc.Set(ctx, a.key, value, 0).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { setErr = err })
	tb.Finally(func() {})
	return setErr
}

// IncrementAndGet atomically increments the value by 1 and returns the new value.
func (a *RAtomicLong) IncrementAndGet(ctx context.Context) (int64, error) {
	var val int64
	var incErr error
	tb := gotrycatch.Try(func() {
		v, err := a.rc.Incr(ctx, a.key).Result()
		if err != nil {
			panic(err)
		}
		val = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { incErr = err })
	tb.Finally(func() {})
	return val, incErr
}

// DecrementAndGet atomically decrements the value by 1 and returns the new value.
func (a *RAtomicLong) DecrementAndGet(ctx context.Context) (int64, error) {
	var val int64
	var decErr error
	tb := gotrycatch.Try(func() {
		v, err := a.rc.Decr(ctx, a.key).Result()
		if err != nil {
			panic(err)
		}
		val = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { decErr = err })
	tb.Finally(func() {})
	return val, decErr
}

// AddAndGet atomically adds delta to the value and returns the new value.
func (a *RAtomicLong) AddAndGet(ctx context.Context, delta int64) (int64, error) {
	var val int64
	var addErr error
	tb := gotrycatch.Try(func() {
		v, err := a.rc.IncrBy(ctx, a.key, delta).Result()
		if err != nil {
			panic(err)
		}
		val = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { addErr = err })
	tb.Finally(func() {})
	return val, addErr
}

// CompareAndSet atomically sets the value to newVal only if the current value
// equals expect. Returns true when the update took place.
var casScript = redis.NewScript(`
local cur = redis.call("get", KEYS[1])
if cur == ARGV[1] then
    redis.call("set", KEYS[1], ARGV[2])
    return 1
end
return 0
`)

func (a *RAtomicLong) CompareAndSet(ctx context.Context, expect, newVal int64) (bool, error) {
	var swapped bool
	var casErr error
	tb := gotrycatch.Try(func() {
		res, err := casScript.Run(ctx, a.rc, []string{a.key}, expect, newVal).Int()
		if err != nil {
			panic(err)
		}
		swapped = res == 1
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { casErr = err })
	tb.Finally(func() {})
	return swapped, casErr
}
