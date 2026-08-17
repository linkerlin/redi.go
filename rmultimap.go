package redi

import (
	"context"
	"encoding/base64"
	"encoding/binary"

	"github.com/redis/go-redis/v9"
)

// RMultimap is the shared base for RSetMultimap / RListMultimap,
// wire-compatible with Redisson 4.6.x:
//
//	HASH {name}           field = codec(JSON) key, value = internal id
//	SET/LIST {name}:{id}  per-key collection
//
// The internal id is deterministic: HighwayHash-128 (Redisson fixed key)
// over the codec-encoded key, packed big-endian, unpadded base64 —
// matching Java's Hash.hash128toBase64 byte-for-byte (redi.py-verified).
type RMultimap struct {
	rObject
	isSet bool // true: SET collection; false: LIST collection
}

// MultimapEntry is one key/value association returned by Entries.
type MultimapEntry struct {
	Key   any
	Value any
}

// internalID computes the Redisson Hash.hash128toBase64 for an encoded key.
// Note the big-endian packing (the bloom filter uses the raw little-endian
// halves — both match their respective Java call sites).
func (m *RMultimap) internalID(encodedKey string) string {
	return redissonHash128Base64(encodedKey)
}

func redissonHash128Base64(encoded string) string {
	sum := highwayhashSum128([]byte(encoded))
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], binary.LittleEndian.Uint64(sum[0:8]))
	binary.BigEndian.PutUint64(buf[8:16], binary.LittleEndian.Uint64(sum[8:16]))
	return base64.RawStdEncoding.EncodeToString(buf)
}

func (m *RMultimap) collectionKey(id string) string {
	return suffixName(m.name, id)
}

// collectionAdd inserts an encoded value into the per-key collection.
func (m *RMultimap) collectionAdd(ctx context.Context, collKey, encValue string) error {
	if m.isSet {
		_, err := m.rc().SAdd(ctx, collKey, encValue).Result()
		return err
	}
	return m.rc().RPush(ctx, collKey, encValue).Err()
}

// collectionRemove removes count occurrences of an encoded value.
func (m *RMultimap) collectionRemove(ctx context.Context, collKey, encValue string) (int64, error) {
	if m.isSet {
		return m.rc().SRem(ctx, collKey, encValue).Result()
	}
	return m.rc().LRem(ctx, collKey, 1, encValue).Result()
}

// collectionRead returns all encoded members of the per-key collection.
func (m *RMultimap) collectionRead(ctx context.Context, collKey string) ([]string, error) {
	if m.isSet {
		return m.rc().SMembers(ctx, collKey).Result()
	}
	return m.rc().LRange(ctx, collKey, 0, -1).Result()
}

func (m *RMultimap) collectionSize(ctx context.Context, collKey string) (int64, error) {
	if m.isSet {
		return m.rc().SCard(ctx, collKey).Result()
	}
	return m.rc().LLen(ctx, collKey).Result()
}

func (m *RMultimap) collectionMode() int {
	if m.isSet {
		return 1
	}
	return 0
}

var multimapReplaceScript = redis.NewScript(`
local members;
if ARGV[3] == '1' then
    members = redis.call('smembers', KEYS[2]);
else
    members = redis.call('lrange', KEYS[2], 0, -1);
end;
redis.call('del', KEYS[2]);
if #ARGV > 3 then
    redis.call('hset', KEYS[1], ARGV[1], ARGV[2]);
    for i = 4, #ARGV do
        if ARGV[3] == '1' then
            redis.call('sadd', KEYS[2], ARGV[i]);
        else
            redis.call('rpush', KEYS[2], ARGV[i]);
        end;
    end;
else
    redis.call('hdel', KEYS[1], ARGV[1]);
end;
return members;
`)

var multimapFastReplaceScript = redis.NewScript(`
redis.call('del', KEYS[2]);
if #ARGV > 3 then
    redis.call('hset', KEYS[1], ARGV[1], ARGV[2]);
    for i = 4, #ARGV do
        if ARGV[3] == '1' then
            redis.call('sadd', KEYS[2], ARGV[i]);
        else
            redis.call('rpush', KEYS[2], ARGV[i]);
        end;
    end;
else
    redis.call('hdel', KEYS[1], ARGV[1]);
end;
`)

var multimapContainsValueScript = redis.NewScript(`
local ids = redis.call('hvals', KEYS[1]);
for i = 1, #ids do
    local name = ARGV[3] .. ids[i];
    if ARGV[2] == '1' then
        if redis.call('sismember', name, ARGV[1]) == 1 then
            return 1;
        end;
    else
        local values = redis.call('lrange', name, 0, -1);
        for j = 1, #values do
            if values[j] == ARGV[1] then
                return 1;
            end;
        end;
    end;
end;
return 0;
`)

var multimapValuesScript = redis.NewScript(`
local ids = redis.call('hvals', KEYS[1]);
local result = {};
for i = 1, #ids do
    local name = ARGV[2] .. ids[i];
    local values;
    if ARGV[1] == '1' then
        values = redis.call('smembers', name);
    else
        values = redis.call('lrange', name, 0, -1);
    end;
    for j = 1, #values do
        table.insert(result, values[j]);
    end;
end;
return result;
`)

var multimapFastRemoveScript = redis.NewScript(`
local removed = redis.call('hdel', KEYS[1], unpack(ARGV))
if removed > 0 and #KEYS > 1 then
    redis.call('del', unpack(KEYS, 2, #KEYS))
end
return removed
`)

var multimapFastRemoveValueScript = redis.NewScript(`
local entries = redis.call('hgetall', KEYS[1])
local removed = 0
for i = 1, #entries, 2 do
    local name = ARGV[2] .. entries[i + 1]
    for j = 3, #ARGV do
        if ARGV[1] == '1' then
            removed = removed + redis.call('srem', name, ARGV[j])
        else
            removed = removed + redis.call('lrem', name, 1, ARGV[j])
        end
    end
    if redis.call('exists', name) == 0 then
        redis.call('hdel', KEYS[1], entries[i])
    end
end
return removed
`)

var multimapEntriesScript = redis.NewScript(`
local entries = redis.call('hgetall', KEYS[1])
local result = {}
for i = 1, #entries, 2 do
    local name = ARGV[2] .. entries[i + 1]
    local values
    if ARGV[1] == '1' then
        values = redis.call('smembers', name)
    else
        values = redis.call('lrange', name, 0, -1)
    end
    for j = 1, #values do
        result[#result + 1] = entries[i]
        result[#result + 1] = values[j]
    end
end
return result
`)

// Put associates value with key. Returns true when a new association was
// created.
func (m *RMultimap) Put(ctx context.Context, key, value any) (bool, error) {
	ek, err := m.c.codec.Encode(key)
	if err != nil {
		return false, err
	}
	ev, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	id := m.internalID(ek)
	collKey := m.collectionKey(id)
	exists, err := m.rc().HExists(ctx, m.name, ek).Result()
	if err != nil {
		return false, err
	}
	if exists {
		n, err := m.collectionAddCount(ctx, collKey, ev)
		if err != nil {
			return false, err
		}
		return n > 0, nil
	}
	if err := m.rc().HSet(ctx, m.name, ek, id).Err(); err != nil {
		return false, err
	}
	return true, m.collectionAdd(ctx, collKey, ev)
}

// collectionAddCount adds and reports whether it was new (SADD count / RPUSH
// always new for lists).
func (m *RMultimap) collectionAddCount(ctx context.Context, collKey, ev string) (int64, error) {
	if m.isSet {
		return m.rc().SAdd(ctx, collKey, ev).Result()
	}
	n, err := m.rc().RPush(ctx, collKey, ev).Result()
	return n, err
}

// PutAll associates multiple values with key.
func (m *RMultimap) PutAll(ctx context.Context, key any, values ...any) (bool, error) {
	changed := false
	for _, v := range values {
		ok, err := m.Put(ctx, key, v)
		if err != nil {
			return false, err
		}
		changed = changed || ok
	}
	return changed, nil
}

// ReplaceValues replaces all values associated with key and returns the old values.
func (m *RMultimap) ReplaceValues(ctx context.Context, key any, values ...any) ([]any, error) {
	ek, err := m.c.codec.Encode(key)
	if err != nil {
		return nil, err
	}
	args := []any{ek, m.internalID(ek), m.collectionMode()}
	for _, value := range values {
		ev, err := m.c.codec.Encode(value)
		if err != nil {
			return nil, err
		}
		args = append(args, ev)
	}
	res, err := multimapReplaceScript.Run(ctx, m.rc(),
		[]string{m.name, m.collectionKey(m.internalID(ek))}, args...).Slice()
	if err == redis.Nil {
		return []any{}, nil
	}
	if err != nil {
		return nil, err
	}
	return m.decodeValues(res)
}

// FastReplaceValues replaces all values associated with key without returning the old values.
func (m *RMultimap) FastReplaceValues(ctx context.Context, key any, values ...any) error {
	ek, err := m.c.codec.Encode(key)
	if err != nil {
		return err
	}
	args := []any{ek, m.internalID(ek), m.collectionMode()}
	for _, value := range values {
		ev, err := m.c.codec.Encode(value)
		if err != nil {
			return err
		}
		args = append(args, ev)
	}
	err = multimapFastReplaceScript.Run(ctx, m.rc(),
		[]string{m.name, m.collectionKey(m.internalID(ek))}, args...).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}

// RemoveEntry removes one association (key, value). When the collection
// becomes empty it is deleted along with the hash entry.
func (m *RMultimap) RemoveEntry(ctx context.Context, key, value any) (bool, error) {
	ek, err := m.c.codec.Encode(key)
	if err != nil {
		return false, err
	}
	ev, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	id := m.internalID(ek)
	if exists, err := m.rc().HExists(ctx, m.name, ek).Result(); err != nil || !exists {
		return false, err
	}
	collKey := m.collectionKey(id)
	removed, err := m.collectionRemove(ctx, collKey, ev)
	if err != nil {
		return false, err
	}
	if n, serr := m.collectionSize(ctx, collKey); serr == nil && n == 0 {
		_ = m.rc().Del(ctx, collKey).Err()
		_ = m.rc().HDel(ctx, m.name, ek).Err()
	}
	return removed > 0, nil
}

// RemoveAll drops every association of key.
func (m *RMultimap) RemoveAll(ctx context.Context, key any) (bool, error) {
	ek, err := m.c.codec.Encode(key)
	if err != nil {
		return false, err
	}
	exists, err := m.rc().HExists(ctx, m.name, ek).Result()
	if err != nil || !exists {
		return false, err
	}
	_ = m.rc().Del(ctx, m.collectionKey(m.internalID(ek))).Err()
	return true, m.rc().HDel(ctx, m.name, ek).Err()
}

// FastRemove drops keys and their value collections in one operation.
func (m *RMultimap) FastRemove(ctx context.Context, keys ...any) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	encoded := make([]any, len(keys))
	redisKeys := make([]string, 1, len(keys)+1)
	redisKeys[0] = m.name
	for i, key := range keys {
		ek, err := m.c.codec.Encode(key)
		if err != nil {
			return 0, err
		}
		encoded[i] = ek
		redisKeys = append(redisKeys, m.collectionKey(m.internalID(ek)))
	}
	return multimapFastRemoveScript.Run(ctx, m.rc(), redisKeys, encoded...).Int64()
}

// FastRemoveValue removes one occurrence of each value from every key.
func (m *RMultimap) FastRemoveValue(ctx context.Context, values ...any) (int64, error) {
	if len(values) == 0 {
		return 0, nil
	}
	args := []any{m.collectionMode(), m.collectionKey("")}
	for _, value := range values {
		encoded, err := m.c.codec.Encode(value)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	return multimapFastRemoveValueScript.Run(ctx, m.rc(),
		[]string{m.name}, args...).Int64()
}

// ContainsKey reports whether key has any association.
func (m *RMultimap) ContainsKey(ctx context.Context, key any) (bool, error) {
	ek, err := m.c.codec.Encode(key)
	if err != nil {
		return false, err
	}
	return m.rc().HExists(ctx, m.name, ek).Result()
}

// ContainsEntry reports whether (key, value) is associated.
func (m *RMultimap) ContainsEntry(ctx context.Context, key, value any) (bool, error) {
	ek, err := m.c.codec.Encode(key)
	if err != nil {
		return false, err
	}
	ev, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	members, err := m.collectionRead(ctx, m.collectionKey(m.internalID(ek)))
	if err != nil {
		return false, err
	}
	for _, mm := range members {
		if mm == ev {
			return true, nil
		}
	}
	return false, nil
}

// ContainsValue reports whether any key is associated with value.
func (m *RMultimap) ContainsValue(ctx context.Context, value any) (bool, error) {
	ev, err := m.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	n, err := multimapContainsValueScript.Run(ctx, m.rc(), []string{m.name},
		ev, m.collectionMode(), m.collectionKey("")).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Get returns all values associated with key (decoded).
func (m *RMultimap) Get(ctx context.Context, key any) ([]any, error) {
	ek, err := m.c.codec.Encode(key)
	if err != nil {
		return nil, err
	}
	members, err := m.collectionRead(ctx, m.collectionKey(m.internalID(ek)))
	if err != nil {
		return nil, err
	}
	raw := make([]any, len(members))
	for i, member := range members {
		raw[i] = member
	}
	return m.decodeValues(raw)
}

// KeySet returns all keys (decoded).
func (m *RMultimap) KeySet(ctx context.Context) ([]any, error) {
	keys, err := m.rc().HKeys(ctx, m.name).Result()
	if err != nil {
		return nil, err
	}
	out := make([]any, len(keys))
	for i, k := range keys {
		d, err := m.c.codec.Decode(k)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

// ReadAllKeySet is the eager KeySet alias used by Redisson.
func (m *RMultimap) ReadAllKeySet(ctx context.Context) ([]any, error) {
	return m.KeySet(ctx)
}

// Values returns all values across all keys.
func (m *RMultimap) Values(ctx context.Context) ([]any, error) {
	res, err := multimapValuesScript.Run(ctx, m.rc(), []string{m.name},
		m.collectionMode(), m.collectionKey("")).Slice()
	if err == redis.Nil {
		return []any{}, nil
	}
	if err != nil {
		return nil, err
	}
	return m.decodeValues(res)
}

// Entries returns every key/value association.
func (m *RMultimap) Entries(ctx context.Context) ([]MultimapEntry, error) {
	res, err := multimapEntriesScript.Run(ctx, m.rc(), []string{m.name},
		m.collectionMode(), m.collectionKey("")).Slice()
	if err == redis.Nil {
		return []MultimapEntry{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]MultimapEntry, 0, len(res)/2)
	for i := 0; i+1 < len(res); i += 2 {
		encodedKey, _ := res[i].(string)
		encodedValue, _ := res[i+1].(string)
		key, err := m.c.codec.Decode(encodedKey)
		if err != nil {
			return nil, err
		}
		value, err := m.c.codec.Decode(encodedValue)
		if err != nil {
			return nil, err
		}
		out = append(out, MultimapEntry{Key: key, Value: value})
	}
	return out, nil
}

// KeySize returns the number of keys in the multimap index.
func (m *RMultimap) KeySize(ctx context.Context) (int64, error) {
	return m.rc().HLen(ctx, m.name).Result()
}

// IsEmpty reports whether the multimap has no associations.
func (m *RMultimap) IsEmpty(ctx context.Context) (bool, error) {
	size, err := m.Size(ctx)
	if err != nil {
		return false, err
	}
	return size == 0, nil
}

// Size returns the total number of associations across all keys.
func (m *RMultimap) Size(ctx context.Context) (int64, error) {
	ids, err := m.rc().HVals(ctx, m.name).Result()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, id := range ids {
		n, err := m.collectionSize(ctx, m.collectionKey(id))
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// Clear removes the multimap (hash + every collection).
func (m *RMultimap) Clear(ctx context.Context) error {
	ids, err := m.rc().HVals(ctx, m.name).Result()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := m.rc().Del(ctx, m.collectionKey(id)).Err(); err != nil {
			return err
		}
	}
	return m.rc().Del(ctx, m.name).Err()
}

func (m *RMultimap) decodeValues(values []any) ([]any, error) {
	out := make([]any, len(values))
	for i, value := range values {
		encoded, _ := value.(string)
		decoded, err := m.c.codec.Decode(encoded)
		if err != nil {
			return nil, err
		}
		out[i] = decoded
	}
	return out, nil
}

// RSetMultimap stores an unordered value set per key.
type RSetMultimap struct{ RMultimap }

// RListMultimap stores an ordered value list per key.
type RListMultimap struct{ RMultimap }

func newRSetMultimap(c *Client, name string) *RSetMultimap {
	return &RSetMultimap{RMultimap{rObject: rObject{c: c, name: name}, isSet: true}}
}

func newRListMultimap(c *Client, name string) *RListMultimap {
	return &RListMultimap{RMultimap{rObject: rObject{c: c, name: name}, isSet: false}}
}
