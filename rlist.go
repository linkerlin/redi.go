package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RList is a distributed list backed by a Redis List (RPUSH / LPOP).
// Elements are codec (JSON) encoded, Redisson-compatible.
type RList struct {
	rObject
}

func newRList(c *Client, name string) *RList {
	return &RList{rObject{c: c, name: name}}
}

// listDeleteSentinel matches Redisson's raw LSET/LREM tombstone for
// index-based removes (not codec-encoded).
const listDeleteSentinel = "DELETED_BY_REDISSON"

// Add appends values to the tail of the list.
func (l *RList) Add(ctx context.Context, values ...any) error {
	_, err := l.AddCounted(ctx, values...)
	return err
}

// AddCounted appends values and returns the list length after the push
// (Redis RPUSH).
func (l *RList) AddCounted(ctx context.Context, values ...any) (int64, error) {
	if len(values) == 0 {
		return l.Size(ctx)
	}
	enc, err := l.encodeAll(values)
	if err != nil {
		return 0, err
	}
	return l.rc().RPush(ctx, l.name, enc...).Result()
}

// Get returns the element at the given index (0-based).
// Returns (nil, nil) when the index is out of range.
func (l *RList) Get(ctx context.Context, index int64) (any, error) {
	v, err := l.rc().LIndex(ctx, l.name, index).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return l.c.codec.Decode(v)
}

// GetInto decodes the element at index into target. Returns false when
// the index is out of range.
func (l *RList) GetInto(ctx context.Context, index int64, target any) (bool, error) {
	v, err := l.rc().LIndex(ctx, l.name, index).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, decodeInto(l.c.codec, v, target)
}

// Set replaces the element at index with value (LSET).
func (l *RList) Set(ctx context.Context, index int64, value any) error {
	enc, err := l.c.codec.Encode(value)
	if err != nil {
		return err
	}
	return l.rc().LSet(ctx, l.name, index, enc).Err()
}

// FastSet is the Redisson-style alias of Set.
func (l *RList) FastSet(ctx context.Context, index int64, value any) error {
	return l.Set(ctx, index, value)
}

// Remove removes up to count occurrences of value (count semantics match
// Redis LREM). Prefer RemoveCounted when you need the removed tally.
func (l *RList) Remove(ctx context.Context, count int64, value any) error {
	_, err := l.RemoveCounted(ctx, count, value)
	return err
}

// RemoveCounted is LREM and returns how many elements were removed.
func (l *RList) RemoveCounted(ctx context.Context, count int64, value any) (int64, error) {
	enc, err := l.c.codec.Encode(value)
	if err != nil {
		return 0, err
	}
	return l.rc().LRem(ctx, l.name, count, enc).Result()
}

// RemoveByIndex removes and returns the element at index (Redisson remove(int)
// / fastRemove tombstone path). Returns (nil, nil) when out of range.
func (l *RList) RemoveByIndex(ctx context.Context, index int64) (any, error) {
	if index == 0 {
		v, err := l.rc().LPop(ctx, l.name).Result()
		if err == redis.Nil {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return l.c.codec.Decode(v)
	}
	res, err := listRemoveByIndexScript.Run(ctx, l.rc(),
		[]string{l.name}, index, listDeleteSentinel).Result()
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
	return l.c.codec.Decode(s)
}

// FastRemoveByIndex removes the element at index without returning it
// (Redisson fastRemove).
func (l *RList) FastRemoveByIndex(ctx context.Context, index int64) error {
	err := listFastRemoveByIndexScript.Run(ctx, l.rc(),
		[]string{l.name}, index, listDeleteSentinel).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}

// IndexOf returns the first index of value, or -1 when absent.
func (l *RList) IndexOf(ctx context.Context, value any) (int64, error) {
	enc, err := l.c.codec.Encode(value)
	if err != nil {
		return -1, err
	}
	return listIndexOfScript.Run(ctx, l.rc(), []string{l.name}, enc).Int64()
}

// LastIndexOf returns the last index of value, or -1 when absent.
func (l *RList) LastIndexOf(ctx context.Context, value any) (int64, error) {
	enc, err := l.c.codec.Encode(value)
	if err != nil {
		return -1, err
	}
	return listLastIndexOfScript.Run(ctx, l.rc(), []string{l.name}, enc).Int64()
}

// Contains reports whether value appears in the list.
func (l *RList) Contains(ctx context.Context, value any) (bool, error) {
	idx, err := l.IndexOf(ctx, value)
	if err != nil {
		return false, err
	}
	return idx >= 0, nil
}

// AddBefore inserts element before pivot (LINSERT BEFORE).
// Returns the list length after insert, or -1 when pivot is missing.
func (l *RList) AddBefore(ctx context.Context, pivot, element any) (int64, error) {
	return l.linsert(ctx, "BEFORE", pivot, element)
}

// AddAfter inserts element after pivot (LINSERT AFTER).
// Returns the list length after insert, or -1 when pivot is missing.
func (l *RList) AddAfter(ctx context.Context, pivot, element any) (int64, error) {
	return l.linsert(ctx, "AFTER", pivot, element)
}

func (l *RList) linsert(ctx context.Context, where string, pivot, element any) (int64, error) {
	p, err := l.c.codec.Encode(pivot)
	if err != nil {
		return 0, err
	}
	e, err := l.c.codec.Encode(element)
	if err != nil {
		return 0, err
	}
	return l.rc().LInsert(ctx, l.name, where, p, e).Result()
}

// Trim keeps only elements in [start, stop] (LTRIM, inclusive).
func (l *RList) Trim(ctx context.Context, start, stop int64) error {
	return l.rc().LTrim(ctx, l.name, start, stop).Err()
}

// Size returns the number of elements in the list.
func (l *RList) Size(ctx context.Context) (int64, error) {
	return l.rc().LLen(ctx, l.name).Result()
}

// Range returns all elements in [start, stop] (decoded).
func (l *RList) Range(ctx context.Context, start, stop int64) ([]any, error) {
	vals, err := l.rc().LRange(ctx, l.name, start, stop).Result()
	if err != nil {
		return nil, err
	}
	return l.decodeAll(vals)
}

// ReadAll returns the entire list (Redisson readAll).
func (l *RList) ReadAll(ctx context.Context) ([]any, error) {
	return l.Range(ctx, 0, -1)
}

// Clear removes the entire list.
func (l *RList) Clear(ctx context.Context) error { return l.Delete(ctx) }

func (l *RList) encodeAll(values []any) ([]any, error) {
	enc := make([]any, len(values))
	for i, v := range values {
		s, err := l.c.codec.Encode(v)
		if err != nil {
			return nil, err
		}
		enc[i] = s
	}
	return enc, nil
}

func (l *RList) decodeAll(vals []string) ([]any, error) {
	out := make([]any, len(vals))
	for i, v := range vals {
		d, err := l.c.codec.Decode(v)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

var listIndexOfScript = redis.NewScript(`
local items = redis.call('lrange', KEYS[1], 0, -1)
for i = 1, #items do
    if items[i] == ARGV[1] then
        return i - 1
    end
end
return -1
`)

var listLastIndexOfScript = redis.NewScript(`
local items = redis.call('lrange', KEYS[1], 0, -1)
for i = #items, 1, -1 do
    if items[i] == ARGV[1] then
        return i - 1
    end
end
return -1
`)

var listRemoveByIndexScript = redis.NewScript(`
local v = redis.call('lindex', KEYS[1], ARGV[1])
if v == false then
    return nil
end
redis.call('lset', KEYS[1], ARGV[1], ARGV[2])
redis.call('lrem', KEYS[1], 1, ARGV[2])
return v
`)

var listFastRemoveByIndexScript = redis.NewScript(`
if redis.call('lindex', KEYS[1], ARGV[1]) == false then
    return nil
end
redis.call('lset', KEYS[1], ARGV[1], ARGV[2])
redis.call('lrem', KEYS[1], 1, ARGV[2])
`)
