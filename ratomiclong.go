package redi

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// RAtomicLong is a distributed atomic 64-bit integer backed by a Redis String
// holding a plain decimal number (Redisson StringCodec format, interoperable
// without any codec configuration).
type RAtomicLong struct {
	rObject
}

func newRAtomicLong(c *Client, name string) *RAtomicLong {
	return &RAtomicLong{rObject{c: c, name: name}}
}

// Get returns the current value; 0 when the key does not exist
// (Redisson semantics).
func (a *RAtomicLong) Get(ctx context.Context) (int64, error) {
	v, err := a.rc().Get(ctx, a.name).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

// Set replaces the value and returns the previous one.
func (a *RAtomicLong) Set(ctx context.Context, value int64) (int64, error) {
	v, err := a.rc().GetSet(ctx, a.name, strconv.FormatInt(value, 10)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

// IncrementAndGet atomically increments the value by 1 and returns the new value.
func (a *RAtomicLong) IncrementAndGet(ctx context.Context) (int64, error) {
	return a.rc().Incr(ctx, a.name).Result()
}

// DecrementAndGet atomically decrements the value by 1 and returns the new value.
func (a *RAtomicLong) DecrementAndGet(ctx context.Context) (int64, error) {
	return a.rc().Decr(ctx, a.name).Result()
}

// AddAndGet atomically adds delta to the value and returns the new value.
func (a *RAtomicLong) AddAndGet(ctx context.Context, delta int64) (int64, error) {
	return a.rc().IncrBy(ctx, a.name, delta).Result()
}

// GetAndAdd atomically adds delta and returns the previous value.
func (a *RAtomicLong) GetAndAdd(ctx context.Context, delta int64) (int64, error) {
	n, err := a.rc().IncrBy(ctx, a.name, delta).Result()
	if err != nil {
		return 0, err
	}
	return n - delta, nil
}

// CompareAndSet atomically sets the value to newVal only when the current
// value equals expect. A missing key counts as 0.
func (a *RAtomicLong) CompareAndSet(ctx context.Context, expect, newVal int64) (bool, error) {
	res, err := atomicLongCASScript.Run(ctx, a.rc(), []string{a.name},
		strconv.FormatInt(expect, 10), strconv.FormatInt(newVal, 10)).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

var atomicLongCASScript = redis.NewScript(`
local cur = redis.call('get', KEYS[1])
if not cur then cur = '0' end
if cur == ARGV[1] then
    redis.call('set', KEYS[1], ARGV[2])
    return 1
end
return 0
`)
