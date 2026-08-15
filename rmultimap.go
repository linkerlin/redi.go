package redi

import (
	"context"
	"encoding/base64"
	"encoding/binary"
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

func newRMultimap(c *Client, name string, isSet bool) *RMultimap {
	return &RMultimap{rObject: rObject{c: c, name: name}, isSet: isSet}
}

// internalID computes the Redisson Hash.hash128toBase64 for an encoded key.
// Note the big-endian packing (the bloom filter uses the raw little-endian
// halves — both match their respective Java call sites).
func (m *RMultimap) internalID(encodedKey string) string {
	sum := highwayhashSum128([]byte(encodedKey))
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

// collectionAddCount adds and reports whether it was new (SADD count / LPUSH
// always new for lists).
func (m *RMultimap) collectionAddCount(ctx context.Context, collKey, ev string) (int64, error) {
	if m.isSet {
		return m.rc().SAdd(ctx, collKey, ev).Result()
	}
	n, err := m.rc().LPush(ctx, collKey, ev).Result()
	return n, err
}

// PutAll associates multiple values with key.
func (m *RMultimap) PutAll(ctx context.Context, key string, values ...any) (bool, error) {
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
	out := make([]any, len(members))
	for i, mm := range members {
		d, err := m.c.codec.Decode(mm)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
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
