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

// PutAll writes every entry in a single HSET (empty map is a no-op).
func (m *RMap) PutAll(ctx context.Context, entries map[string]any) error {
	if len(entries) == 0 {
		return nil
	}
	args := make([]any, 0, len(entries)*2)
	for field, value := range entries {
		enc, err := m.c.codec.Encode(value)
		if err != nil {
			return err
		}
		args = append(args, encodeKey(m.c.codec, field), enc)
	}
	return m.rc().HSet(ctx, m.name, args...).Err()
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

// GetInto decodes the value for field into target. Returns false when absent.
func (m *RMap) GetInto(ctx context.Context, field string, target any) (bool, error) {
	v, err := m.rc().HGet(ctx, m.name, encodeKey(m.c.codec, field)).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, decodeInto(m.c.codec, v, target)
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

// GetAllKeys returns values for the given fields (missing fields omitted).
func (m *RMap) GetAllKeys(ctx context.Context, fields ...string) (map[string]any, error) {
	if len(fields) == 0 {
		return map[string]any{}, nil
	}
	enc := make([]string, len(fields))
	for i, f := range fields {
		enc[i] = encodeKey(m.c.codec, f)
	}
	vals, err := m.rc().HMGet(ctx, m.name, enc...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(fields))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		decoded, err := m.c.codec.Decode(s)
		if err != nil {
			return nil, err
		}
		out[fields[i]] = decoded
	}
	return out, nil
}

// ContainsValue reports whether any field stores value (codec equality).
func (m *RMap) ContainsValue(ctx context.Context, value any) (bool, error) {
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	vals, err := m.rc().HVals(ctx, m.name).Result()
	if err != nil {
		return false, err
	}
	for _, v := range vals {
		if v == enc {
			return true, nil
		}
	}
	return false, nil
}

// PutIfAbsent sets field only when absent. Returns true when set.
func (m *RMap) PutIfAbsent(ctx context.Context, field string, value any) (bool, error) {
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	return m.rc().HSetNX(ctx, m.name, encodeKey(m.c.codec, field), enc).Result()
}

// FastPutIfAbsent is the Redisson-style alias of PutIfAbsent.
func (m *RMap) FastPutIfAbsent(ctx context.Context, field string, value any) (bool, error) {
	return m.PutIfAbsent(ctx, field, value)
}

// PutIfExists sets field only when it already exists. Returns true when written.
func (m *RMap) PutIfExists(ctx context.Context, field string, value any) (bool, error) {
	return m.FastPutIfExists(ctx, field, value)
}

// FastPut sets field and reports whether it was a new field (true) or an
// update of an existing one (false) — Redisson fastPut semantics (HSET).
func (m *RMap) FastPut(ctx context.Context, field string, value any) (bool, error) {
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	n, err := m.rc().HSet(ctx, m.name, encodeKey(m.c.codec, field), enc).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Replace sets field only when it already exists. Returns the previous
// value, or (nil, nil) when the field was absent (unchanged).
func (m *RMap) Replace(ctx context.Context, field string, value any) (any, error) {
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return nil, err
	}
	res, err := mapReplaceScript.Run(ctx, m.rc(),
		[]string{m.name}, encodeKey(m.c.codec, field), enc).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s, ok := res.(string)
	if !ok {
		return nil, nil
	}
	return m.c.codec.Decode(s)
}

// ReplaceIf sets field to newValue only when the current value equals
// oldValue (codec-encoded equality). Returns true when replaced.
func (m *RMap) ReplaceIf(ctx context.Context, field string, oldValue, newValue any) (bool, error) {
	oldEnc, err := m.c.codec.Encode(oldValue)
	if err != nil {
		return false, err
	}
	newEnc, err := m.c.codec.Encode(newValue)
	if err != nil {
		return false, err
	}
	n, err := mapReplaceIfScript.Run(ctx, m.rc(),
		[]string{m.name}, encodeKey(m.c.codec, field), oldEnc, newEnc).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Keys returns all map fields (decoded). Order is undefined.
func (m *RMap) Keys(ctx context.Context) ([]string, error) {
	raw, err := m.rc().HKeys(ctx, m.name).Result()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(raw))
	for _, k := range raw {
		decoded, err := m.c.codec.Decode(k)
		if err != nil {
			return nil, err
		}
		if ks, ok := decoded.(string); ok {
			out = append(out, ks)
		}
	}
	return out, nil
}

// Values returns all map values (decoded). Order is undefined.
func (m *RMap) Values(ctx context.Context) ([]any, error) {
	raw, err := m.rc().HVals(ctx, m.name).Result()
	if err != nil {
		return nil, err
	}
	out := make([]any, len(raw))
	for i, v := range raw {
		d, err := m.c.codec.Decode(v)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

// RandomKeys returns up to count random distinct fields.
func (m *RMap) RandomKeys(ctx context.Context, count int64) ([]string, error) {
	if count <= 0 {
		return []string{}, nil
	}
	raw, err := m.rc().HRandField(ctx, m.name, int(count)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(raw))
	for _, field := range raw {
		decoded, err := m.c.codec.Decode(field)
		if err != nil {
			return nil, err
		}
		if value, ok := decoded.(string); ok {
			out = append(out, value)
		}
	}
	return out, nil
}

// RandomEntries returns up to count random distinct field-value pairs.
func (m *RMap) RandomEntries(
	ctx context.Context, count int64,
) (map[string]any, error) {
	if count <= 0 {
		return map[string]any{}, nil
	}
	raw, err := m.rc().HRandFieldWithValues(ctx, m.name, int(count)).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(raw))
	for _, entry := range raw {
		field, err := m.c.codec.Decode(entry.Key)
		if err != nil {
			return nil, err
		}
		value, err := m.c.codec.Decode(entry.Value)
		if err != nil {
			return nil, err
		}
		if key, ok := field.(string); ok {
			out[key] = value
		}
	}
	return out, nil
}

// ValueSize returns the encoded value size in bytes (HSTRLEN).
func (m *RMap) ValueSize(ctx context.Context, field string) (int64, error) {
	return m.rc().HStrLen(ctx, m.name, encodeKey(m.c.codec, field)).Result()
}

// FastPutIfExists sets field only when it already exists. Returns true when
// written — Redisson fastPutIfExists.
func (m *RMap) FastPutIfExists(ctx context.Context, field string, value any) (bool, error) {
	enc, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	n, err := mapFastReplaceScript.Run(ctx, m.rc(),
		[]string{m.name}, encodeKey(m.c.codec, field), enc).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// FastReplace is an alias of FastPutIfExists (Redisson fastReplace).
func (m *RMap) FastReplace(ctx context.Context, field string, value any) (bool, error) {
	return m.FastPutIfExists(ctx, field, value)
}

// FastRemove deletes fields and returns how many were actually removed
// (Redisson fastRemove / HDEL).
func (m *RMap) FastRemove(ctx context.Context, fields ...string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	enc := make([]string, len(fields))
	for i, f := range fields {
		enc[i] = encodeKey(m.c.codec, f)
	}
	return m.rc().HDel(ctx, m.name, enc...).Result()
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

// Clear removes the entire map key (DEL), not individual fields.
// Must not call Delete with no fields — that path is a no-op by design.
func (m *RMap) Clear(ctx context.Context) error {
	return m.rObject.Delete(ctx)
}

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

// Redisson replace(K,V): return previous value or nil when absent.
var mapReplaceScript = redis.NewScript(`
if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
    local v = redis.call('hget', KEYS[1], ARGV[1])
    redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
    return v
end
return nil
`)

// Redisson replace(K, old, new): compare encoded values.
var mapReplaceIfScript = redis.NewScript(`
if redis.call('hget', KEYS[1], ARGV[1]) == ARGV[2] then
    redis.call('hset', KEYS[1], ARGV[1], ARGV[3])
    return 1
end
return 0
`)

// Redisson fastReplace / fastPutIfExists.
var mapFastReplaceScript = redis.NewScript(`
if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
    redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
    return 1
end
return 0
`)
