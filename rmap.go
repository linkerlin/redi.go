package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RMap is a distributed map backed by a Redis Hash, wire-compatible with
// Redisson's RMap using JsonJacksonCodec: both fields and values are JSON
// encoded by the client Codec.
type RMap struct {
	rObject
}

func newRMap(c *Client, name string) *RMap {
	return &RMap{rObject{c: c, name: name}}
}

// Put sets field in the map to value.
func (m *RMap) Put(ctx context.Context, field string, value any) error {
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return err
	}
	return m.rc().HSet(ctx, m.name, encodeKey(m.c.codec, field), enc).Err()
}

// Get retrieves the value for field. Returns (nil, nil) when absent.
func (m *RMap) Get(ctx context.Context, field string) (any, error) {
	v, err := m.rc().HGet(ctx, m.name, encodeKey(m.c.codec, field)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return m.c.codec.Decode(v)
}

// GetAll returns all field-value pairs (values decoded).
func (m *RMap) GetAll(ctx context.Context) (map[string]any, error) {
	raw, err := m.rc().HGetAll(ctx, m.name).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		key, _ := m.c.codec.Decode(k)
		val, err := m.c.codec.Decode(v)
		if err != nil {
			return nil, err
		}
		if ks, ok := key.(string); ok {
			out[ks] = val
		}
	}
	return out, nil
}

// PutIfAbsent sets field only when absent. Returns true when set.
func (m *RMap) PutIfAbsent(ctx context.Context, field string, value any) (bool, error) {
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	return m.rc().HSetNX(ctx, m.name, encodeKey(m.c.codec, field), enc).Result()
}

// Delete removes one or more fields from the map.
func (m *RMap) Delete(ctx context.Context, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}
	enc := make([]string, len(fields))
	for i, f := range fields {
		enc[i] = encodeKey(m.c.codec, f)
	}
	return m.rc().HDel(ctx, m.name, enc...).Err()
}

// ContainsKey reports whether the map contains the given field.
func (m *RMap) ContainsKey(ctx context.Context, field string) (bool, error) {
	return m.rc().HExists(ctx, m.name, encodeKey(m.c.codec, field)).Result()
}

// Size returns the number of fields in the map.
func (m *RMap) Size(ctx context.Context) (int64, error) {
	return m.rc().HLen(ctx, m.name).Result()
}

// Clear removes the entire map.
func (m *RMap) Clear(ctx context.Context) error { return m.Delete(ctx) }

// AddAndGet atomically adds delta to the numeric value of field and returns
// the new value. The stored value must be a bare JSON number.
func (m *RMap) AddAndGet(ctx context.Context, field string, delta int64) (int64, error) {
	return mapAddAndGetScript.Run(ctx, m.rc(),
		[]string{m.name}, encodeKey(m.c.codec, field), delta).Int64()
}

var mapAddAndGetScript = redis.NewScript(`
local v = redis.call('hget', KEYS[1], ARGV[1])
if v == false then
    redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
    return tonumber(ARGV[2])
end
local n = redis.call('hincrby', KEYS[1], ARGV[1], ARGV[2])
return n
`)
