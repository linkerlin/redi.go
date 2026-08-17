package redi

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RCircularBuffer is a Redis 8.8+ ARRAY ring with stable slot indexes
// (RedissonCircularBuffer). Distinct from LIST-backed RRingBuffer.
type RCircularBuffer struct {
	rObject
	settingsKey string
}

func newRCircularBuffer(c *Client, name string) *RCircularBuffer {
	return &RCircularBuffer{
		rObject:     rObject{c: c, name: name},
		settingsKey: prefixName("redisson_acb", name),
	}
}

// TrySetCapacity sets capacity only when unset.
func (b *RCircularBuffer) TrySetCapacity(ctx context.Context, capacity int) (bool, error) {
	if capacity <= 0 {
		return false, errors.New("redi: circular buffer capacity must be positive")
	}
	ok, err := b.rc().SetNX(ctx, b.settingsKey, strconv.Itoa(capacity), 0).Result()
	return ok, err
}

// SetCapacity overwrites capacity metadata (applied on next write).
func (b *RCircularBuffer) SetCapacity(ctx context.Context, capacity int) error {
	if capacity <= 0 {
		return errors.New("redi: circular buffer capacity must be positive")
	}
	return b.rc().Set(ctx, b.settingsKey, strconv.Itoa(capacity), 0).Err()
}

// Capacity returns configured capacity (0 when unset).
func (b *RCircularBuffer) Capacity(ctx context.Context) (int, error) {
	s, err := b.rc().Get(ctx, b.settingsKey).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, _ := strconv.Atoi(s)
	return n, nil
}

func (b *RCircularBuffer) requireCapacity(ctx context.Context) (int, error) {
	cap, err := b.Capacity(ctx)
	if err != nil {
		return 0, err
	}
	if cap <= 0 {
		return 0, errors.New("redi: circular buffer capacity not set")
	}
	return cap, nil
}

// Add appends value (ARRING).
func (b *RCircularBuffer) Add(ctx context.Context, value any) error {
	return b.AddAll(ctx, value)
}

// AddAll appends values.
func (b *RCircularBuffer) AddAll(ctx context.Context, values ...any) error {
	cap, err := b.requireCapacity(ctx)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return nil
	}
	args := []any{"ARRING", b.name, cap}
	for _, v := range values {
		enc, err := b.c.codec.Encode(v)
		if err != nil {
			return err
		}
		args = append(args, enc)
	}
	return b.c.rc.Do(ctx, args...).Err()
}

// Get returns the value at absolute slot index.
func (b *RCircularBuffer) Get(ctx context.Context, index int64) (any, error) {
	s, err := b.c.rc.Do(ctx, "ARGET", b.name, index).Text()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return b.c.codec.Decode(s)
}

// Size returns ARLEN.
func (b *RCircularBuffer) Size(ctx context.Context) (int64, error) {
	return b.c.rc.Do(ctx, "ARLEN", b.name).Int64()
}

// ReadAll returns values in temporal order via ARLASTITEMS when available,
// else falls back to scanning slots 0..capacity-1.
func (b *RCircularBuffer) ReadAll(ctx context.Context) ([]any, error) {
	cap, err := b.Capacity(ctx)
	if err != nil {
		return nil, err
	}
	if cap <= 0 {
		return []any{}, nil
	}
	res, err := b.c.rc.Do(ctx, "ARLASTITEMS", b.name, cap).Slice()
	if err != nil {
		// fallback: sequential ARGET
		out := make([]any, 0, cap)
		for i := 0; i < cap; i++ {
			v, gerr := b.Get(ctx, int64(i))
			if gerr != nil {
				return nil, gerr
			}
			if v != nil {
				out = append(out, v)
			}
		}
		return out, nil
	}
	out := make([]any, 0, len(res))
	for _, item := range res {
		s := fmtMember(item)
		if s == "" {
			continue
		}
		v, derr := b.c.codec.Decode(s)
		if derr != nil {
			return nil, derr
		}
		out = append(out, v)
	}
	return out, nil
}

// Clear deletes data and capacity metadata.
func (b *RCircularBuffer) Clear(ctx context.Context) error {
	return b.rc().Del(ctx, b.name, b.settingsKey).Err()
}

func skipCircularBuffer(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unknown command") &&
		(strings.Contains(s, "arring") || strings.Contains(s, "arlen") || strings.Contains(s, "arget"))
}
