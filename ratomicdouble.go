package redi

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// RAtomicDouble is a distributed atomic double backed by a Redis String
// holding a plain decimal number (Redisson StringCodec-compatible).
type RAtomicDouble struct {
	rObject
}

func newRAtomicDouble(c *Client, name string) *RAtomicDouble {
	return &RAtomicDouble{rObject{c: c, name: name}}
}

// Get returns the current value; 0 when the key does not exist.
func (a *RAtomicDouble) Get(ctx context.Context) (float64, error) {
	v, err := a.rc().Get(ctx, a.name).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(v, 64)
}

// Set replaces the value and returns the previous one.
func (a *RAtomicDouble) Set(ctx context.Context, value float64) (float64, error) {
	v, err := a.rc().GetSet(ctx, a.name, strconv.FormatFloat(value, 'f', -1, 64)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(v, 64)
}

// AddAndGet atomically adds delta (INCRBYFLOAT) and returns the new value.
func (a *RAtomicDouble) AddAndGet(ctx context.Context, delta float64) (float64, error) {
	return a.rc().IncrByFloat(ctx, a.name, delta).Result()
}

// IncrementAndGet adds 1 atomically.
func (a *RAtomicDouble) IncrementAndGet(ctx context.Context) (float64, error) {
	return a.AddAndGet(ctx, 1)
}

// DecrementAndGet subtracts 1 atomically.
func (a *RAtomicDouble) DecrementAndGet(ctx context.Context) (float64, error) {
	return a.AddAndGet(ctx, -1)
}

// CompareAndSet sets newVal only when the current value equals expect
// (numeric comparison; missing key counts as 0).
func (a *RAtomicDouble) CompareAndSet(ctx context.Context, expect, newVal float64) (bool, error) {
	n, err := atomicDoubleCASScript.Run(ctx, a.rc(), []string{a.name},
		strconv.FormatFloat(expect, 'f', -1, 64),
		strconv.FormatFloat(newVal, 'f', -1, 64)).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

var atomicDoubleCASScript = redis.NewScript(`
local cur = redis.call('get', KEYS[1])
if not cur then cur = '0' end
if tonumber(cur) == tonumber(ARGV[1]) then
    redis.call('set', KEYS[1], ARGV[2])
    return 1
end
return 0
`)
