package redi

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RArray is a Redis 8.8+ ARRAY type (ARSET/ARGET/…). Values are codec-encoded
// like other Redisson structures. Commands missing on older Redis return the
// server error; tests skip when the ARRAY command family is unavailable.
type RArray struct {
	rObject
}

func newRArray(c *Client, name string) *RArray {
	return &RArray{rObject{c: c, name: name}}
}

// Set stores value at index (ARSET). Returns the number of newly filled slots.
func (a *RArray) Set(ctx context.Context, index int64, value any) (int64, error) {
	if index < 0 {
		return 0, errors.New("redi: array index must be non-negative")
	}
	enc, err := a.c.codec.Encode(value)
	if err != nil {
		return 0, err
	}
	return a.c.rc.Do(ctx, "ARSET", a.name, index, enc).Int64()
}

// SetAll stores contiguous values starting at index.
func (a *RArray) SetAll(ctx context.Context, index int64, values ...any) (int64, error) {
	if index < 0 {
		return 0, errors.New("redi: array index must be non-negative")
	}
	if len(values) == 0 {
		return 0, nil
	}
	args := make([]any, 0, 3+len(values))
	args = append(args, "ARSET", a.name, index)
	for _, v := range values {
		enc, err := a.c.codec.Encode(v)
		if err != nil {
			return 0, err
		}
		args = append(args, enc)
	}
	return a.c.rc.Do(ctx, args...).Int64()
}

// SetEntries stores non-contiguous index→value pairs (ARMSET).
func (a *RArray) SetEntries(ctx context.Context, entries map[int64]any) (int64, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	args := make([]any, 0, 1+2*len(entries))
	args = append(args, "ARMSET", a.name)
	for idx, v := range entries {
		if idx < 0 {
			return 0, errors.New("redi: array index must be non-negative")
		}
		enc, err := a.c.codec.Encode(v)
		if err != nil {
			return 0, err
		}
		args = append(args, idx, enc)
	}
	return a.c.rc.Do(ctx, args...).Int64()
}

// Get returns the value at index, or (nil, nil) when unset.
func (a *RArray) Get(ctx context.Context, index int64) (any, error) {
	if index < 0 {
		return nil, errors.New("redi: array index must be non-negative")
	}
	s, err := a.c.rc.Do(ctx, "ARGET", a.name, index).Text()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return a.c.codec.Decode(s)
}

// GetMulti returns values at the given indexes (ARMGET). Missing slots are nil.
func (a *RArray) GetMulti(ctx context.Context, indexes ...int64) ([]any, error) {
	if len(indexes) == 0 {
		return nil, nil
	}
	args := make([]any, 0, 2+len(indexes))
	args = append(args, "ARMGET", a.name)
	for _, idx := range indexes {
		if idx < 0 {
			return nil, errors.New("redi: array index must be non-negative")
		}
		args = append(args, idx)
	}
	raw, err := a.c.rc.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}
	return a.decodeSlotSlice(raw)
}

// Delete removes values at the given indexes (ARDEL).
func (a *RArray) DeleteIndexes(ctx context.Context, indexes ...int64) (int64, error) {
	if len(indexes) == 0 {
		return 0, nil
	}
	args := make([]any, 0, 2+len(indexes))
	args = append(args, "ARDEL", a.name)
	for _, idx := range indexes {
		if idx < 0 {
			return 0, errors.New("redi: array index must be non-negative")
		}
		args = append(args, idx)
	}
	return a.c.rc.Do(ctx, args...).Int64()
}

// DeleteRange removes indexes in [start, end] inclusive (ARDELRANGE).
func (a *RArray) DeleteRange(ctx context.Context, start, end int64) (int64, error) {
	if start < 0 || end < 0 {
		return 0, errors.New("redi: array index must be non-negative")
	}
	return a.c.rc.Do(ctx, "ARDELRANGE", a.name, start, end).Int64()
}

// Count returns the number of set (non-empty) slots (ARCOUNT).
func (a *RArray) Count(ctx context.Context) (int64, error) {
	return a.c.rc.Do(ctx, "ARCOUNT", a.name).Int64()
}

// Length returns max index + 1 (ARLEN), or 0 when the key is absent.
func (a *RArray) Length(ctx context.Context) (int64, error) {
	return a.c.rc.Do(ctx, "ARLEN", a.name).Int64()
}

// Range returns values in [start, end] inclusive (ARGETRANGE). Unset slots are nil.
func (a *RArray) Range(ctx context.Context, start, end int64) ([]any, error) {
	if start < 0 || end < 0 {
		return nil, errors.New("redi: array index must be non-negative")
	}
	raw, err := a.c.rc.Do(ctx, "ARGETRANGE", a.name, start, end).Slice()
	if err != nil {
		return nil, err
	}
	return a.decodeSlotSlice(raw)
}

// Insert appends values at the internal cursor (ARINSERT). Returns the last
// inserted index.
func (a *RArray) Insert(ctx context.Context, values ...any) (int64, error) {
	if len(values) == 0 {
		return 0, errors.New("redi: array Insert requires at least one value")
	}
	args := make([]any, 0, 2+len(values))
	args = append(args, "ARINSERT", a.name)
	for _, v := range values {
		enc, err := a.c.codec.Encode(v)
		if err != nil {
			return 0, err
		}
		args = append(args, enc)
	}
	return a.c.rc.Do(ctx, args...).Int64()
}

// Next returns the next index ARINSERT would use (ARNEXT).
func (a *RArray) Next(ctx context.Context) (int64, error) {
	return a.c.rc.Do(ctx, "ARNEXT", a.name).Int64()
}

// Seek sets the insert cursor (ARSEEK).
func (a *RArray) Seek(ctx context.Context, index int64) (bool, error) {
	if index < 0 {
		return false, errors.New("redi: array index must be non-negative")
	}
	n, err := a.c.rc.Do(ctx, "ARSEEK", a.name, index).Bool()
	return n, err
}

// Clear deletes the array key.
func (a *RArray) Clear(ctx context.Context) error { return a.Delete(ctx) }

func (a *RArray) decodeSlotSlice(raw []any) ([]any, error) {
	out := make([]any, len(raw))
	for i, item := range raw {
		if item == nil {
			continue
		}
		s, ok := item.(string)
		if !ok {
			switch v := item.(type) {
			case []byte:
				s = string(v)
			default:
				return nil, fmt.Errorf("redi: unexpected array slot type %T", item)
			}
		}
		dec, err := a.c.codec.Decode(s)
		if err != nil {
			return nil, err
		}
		out[i] = dec
	}
	return out, nil
}
