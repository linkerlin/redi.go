package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RBucket is a distributed object holder backed by a Redis String.
// The whole value is codec (JSON) encoded, Redisson-compatible.
type RBucket struct {
	rObject
}

func newRBucket(c *Client, name string) *RBucket {
	return &RBucket{rObject{c: c, name: name}}
}

// Get returns the value, or (nil, nil) when absent.
func (b *RBucket) Get(ctx context.Context) (any, error) {
	v, err := b.rc().Get(ctx, b.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b.c.codec.Decode(v)
}

// GetInto decodes the value into the pointer target. Returns false when absent.
func (b *RBucket) GetInto(ctx context.Context, target any) (bool, error) {
	v, err := b.rc().Get(ctx, b.name).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, decodeInto(b.c.codec, v, target)
}

// Set replaces the value.
func (b *RBucket) Set(ctx context.Context, value any) error {
	enc, err := b.c.codec.Encode(value)
	if err != nil {
		return err
	}
	return b.rc().Set(ctx, b.name, enc, 0).Err()
}

// SetWithTTL replaces the value with millisecond-precision TTL.
func (b *RBucket) SetWithTTL(ctx context.Context, value any, ttl time.Duration) error {
	enc, err := b.c.codec.Encode(value)
	if err != nil {
		return err
	}
	return b.rc().Set(ctx, b.name, enc, ttl).Err()
}

// SetAndKeepTTL replaces the value without changing its existing TTL.
func (b *RBucket) SetAndKeepTTL(ctx context.Context, value any) error {
	enc, err := b.c.codec.Encode(value)
	if err != nil {
		return err
	}
	return b.rc().SetArgs(ctx, b.name, enc, redis.SetArgs{KeepTTL: true}).Err()
}

// GetAndSet replaces the value and returns the previous one.
func (b *RBucket) GetAndSet(ctx context.Context, value any) (any, error) {
	enc, err := b.c.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	v, err := b.rc().GetSet(ctx, b.name, enc).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b.c.codec.Decode(v)
}

// GetAndSetWithTTL replaces the value with a TTL and returns the previous one.
func (b *RBucket) GetAndSetWithTTL(ctx context.Context, value any, ttl time.Duration) (any, error) {
	enc, err := b.c.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	v, err := bucketGetAndSetTTLScript.Run(
		ctx, b.rc(), []string{b.name}, enc, ttl.Milliseconds(),
	).Text()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b.c.codec.Decode(v)
}

// GetAndDelete removes and returns the value.
func (b *RBucket) GetAndDelete(ctx context.Context) (any, error) {
	v, err := b.rc().GetDel(ctx, b.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b.c.codec.Decode(v)
}

// TrySet sets the value only when absent (SET NX).
func (b *RBucket) TrySet(ctx context.Context, value any) (bool, error) {
	enc, err := b.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	return b.rc().SetNX(ctx, b.name, enc, 0).Result()
}

// TrySetWithTTL sets the value with a TTL only when absent (SET NX).
func (b *RBucket) TrySetWithTTL(ctx context.Context, value any, ttl time.Duration) (bool, error) {
	enc, err := b.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	return b.rc().SetNX(ctx, b.name, enc, ttl).Result()
}

// SetIfExists sets the value only when the key exists (SET XX).
func (b *RBucket) SetIfExists(ctx context.Context, value any) (bool, error) {
	enc, err := b.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	return b.rc().SetXX(ctx, b.name, enc, 0).Result()
}

// SetIfExistsWithTTL sets the value with a TTL only when the key exists.
func (b *RBucket) SetIfExistsWithTTL(ctx context.Context, value any, ttl time.Duration) (bool, error) {
	enc, err := b.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	return b.rc().SetXX(ctx, b.name, enc, ttl).Result()
}

// Size returns the encoded value length in bytes (STRLEN).
func (b *RBucket) Size(ctx context.Context) (int64, error) {
	return b.rc().StrLen(ctx, b.name).Result()
}

// CompareAndSet atomically replaces the value when it equals expect.
// expect == nil means "key must be absent".
func (b *RBucket) CompareAndSet(ctx context.Context, expect, update any) (bool, error) {
	encUpdate, err := b.c.codec.Encode(update)
	if err != nil {
		return false, err
	}
	if expect == nil {
		n, err := bucketCASAbsentScript.Run(ctx, b.rc(), []string{b.name}, encUpdate).Int()
		return n == 1, err
	}
	encExpect, err := b.c.codec.Encode(expect)
	if err != nil {
		return false, err
	}
	n, err := bucketCASScript.Run(ctx, b.rc(), []string{b.name}, encExpect, encUpdate).Int()
	return n == 1, err
}

// GetAndExpire returns the value and sets a relative TTL (GETEX PX).
func (b *RBucket) GetAndExpire(ctx context.Context, ttl time.Duration) (any, error) {
	v, err := b.rc().GetEx(ctx, b.name, ttl).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b.c.codec.Decode(v)
}

// GetAndExpireAt returns the value and sets an absolute expiry (GETEX PXAT).
func (b *RBucket) GetAndExpireAt(ctx context.Context, at time.Time) (any, error) {
	v, err := bucketGetExPxatScript.Run(ctx, b.rc(),
		[]string{b.name}, at.UnixMilli()).Text()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b.c.codec.Decode(v)
}

// GetAndClearExpire returns the value and removes its TTL (GETEX PERSIST).
func (b *RBucket) GetAndClearExpire(ctx context.Context) (any, error) {
	v, err := b.rc().GetEx(ctx, b.name, 0).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b.c.codec.Decode(v)
}

// CompareAndDelete atomically deletes the value when it equals expect.
func (b *RBucket) CompareAndDelete(ctx context.Context, expect any) (bool, error) {
	encExpect, err := b.c.codec.Encode(expect)
	if err != nil {
		return false, err
	}
	n, err := bucketCompareDeleteScript.Run(ctx, b.rc(), []string{b.name}, encExpect).Int()
	return n == 1, err
}

var bucketGetAndSetTTLScript = redis.NewScript(`
local old = redis.call('get', KEYS[1])
redis.call('set', KEYS[1], ARGV[1], 'px', ARGV[2])
return old
`)

var bucketCASScript = redis.NewScript(`
local cur = redis.call('get', KEYS[1])
if cur == ARGV[1] then
    redis.call('set', KEYS[1], ARGV[2])
    return 1
end
return 0
`)

var bucketCompareDeleteScript = redis.NewScript(`
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
`)

var bucketCASAbsentScript = redis.NewScript(`
if redis.call('exists', KEYS[1]) == 0 then
    redis.call('set', KEYS[1], ARGV[1])
    return 1
end
return 0
`)

var bucketGetExPxatScript = redis.NewScript(`
return redis.call('getex', KEYS[1], 'pxat', ARGV[1])
`)
