package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RBuckets performs batch operations across multiple bucket keys
// (MGET / MSET / MSETNX), values codec-encoded like RBucket.
type RBuckets struct {
	c *Client
}

func newRBuckets(c *Client) *RBuckets { return &RBuckets{c: c} }

var bucketsSetIfScript = redis.NewScript(`
for i = 1, #KEYS do
    local exists = redis.call('exists', KEYS[i])
    if (ARGV[1] == 'XX' and exists == 0)
            or (ARGV[1] == 'NX' and exists == 1) then
        return 0
    end
end
for i = 1, #KEYS do
    redis.call('set', KEYS[i], ARGV[i + 1])
end
return 1
`)

// Get returns the existing values for the given keys (missing keys omitted).
func (b *RBuckets) Get(ctx context.Context, keys ...string) (map[string]any, error) {
	out := make(map[string]any, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	vals, err := b.c.rc.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		decoded, err := b.c.codec.Decode(s)
		if err != nil {
			return nil, err
		}
		out[keys[i]] = decoded
	}
	return out, nil
}

// Set stores the mapping (MSET), optionally with a millisecond TTL applied
// to every key (pipelined PSETEX when ttl > 0).
func (b *RBuckets) Set(ctx context.Context, mapping map[string]any, ttl time.Duration) error {
	if len(mapping) == 0 {
		return nil
	}
	pipe := b.c.rc.Pipeline()
	for k, v := range mapping {
		enc, err := b.c.codec.Encode(v)
		if err != nil {
			return err
		}
		pipe.Set(ctx, k, enc, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// TrySet stores the mapping only when NONE of the keys exist (MSETNX).
// Returns false when any key is already present.
func (b *RBuckets) TrySet(ctx context.Context, mapping map[string]any) (bool, error) {
	if len(mapping) == 0 {
		return true, nil
	}
	encoded := make(map[string]any, len(mapping))
	for k, v := range mapping {
		enc, err := b.c.codec.Encode(v)
		if err != nil {
			return false, err
		}
		encoded[k] = enc
	}
	return b.c.rc.MSetNX(ctx, encoded).Result()
}

func (b *RBuckets) setIf(ctx context.Context, mapping map[string]any, mode string) (bool, error) {
	if len(mapping) == 0 {
		return false, nil
	}
	keys := make([]string, 0, len(mapping))
	args := make([]any, 1, len(mapping)+1)
	args[0] = mode
	for key, value := range mapping {
		encoded, err := b.c.codec.Encode(value)
		if err != nil {
			return false, err
		}
		keys = append(keys, key)
		args = append(args, encoded)
	}
	n, err := bucketsSetIfScript.Run(ctx, b.c.rc, keys, args...).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// SetIfAllKeysExist stores all values only when every key already exists.
func (b *RBuckets) SetIfAllKeysExist(ctx context.Context, mapping map[string]any) (bool, error) {
	return b.setIf(ctx, mapping, "XX")
}

// SetIfAllKeysAbsent stores all values only when no key exists.
func (b *RBuckets) SetIfAllKeysAbsent(ctx context.Context, mapping map[string]any) (bool, error) {
	return b.setIf(ctx, mapping, "NX")
}
