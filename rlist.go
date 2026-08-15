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

// Add appends values to the tail of the list.
func (l *RList) Add(ctx context.Context, values ...any) error {
	enc, err := l.encodeAll(values)
	if err != nil {
		return err
	}
	return l.rc().RPush(ctx, l.name, enc...).Err()
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

// Set replaces the element at index with value.
func (l *RList) Set(ctx context.Context, index int64, value any) error {
	enc, err := l.c.codec.Encode(value)
	if err != nil {
		return err
	}
	return l.rc().LSet(ctx, l.name, index, enc).Err()
}

// Remove removes the first count occurrences of value.
func (l *RList) Remove(ctx context.Context, count int64, value any) error {
	enc, err := l.c.codec.Encode(value)
	if err != nil {
		return err
	}
	return l.rc().LRem(ctx, l.name, count, enc).Err()
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
