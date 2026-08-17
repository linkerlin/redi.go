package redi

import (
	"context"
)

// RMaps performs mass HASH replacements (RedissonMaps set semantics via DEL+HSET).
type RMaps struct{ c *Client }

func newRMaps(c *Client) *RMaps { return &RMaps{c: c} }

// Set replaces each named map entirely.
func (m *RMaps) Set(ctx context.Context, maps map[string]map[string]any) error {
	return m.SetBatch(ctx, maps, 0)
}

// SetBatch is Set with an optional batchSize (keys per pipeline flush; 0 = all).
func (m *RMaps) SetBatch(ctx context.Context, maps map[string]map[string]any, batchSize int) error {
	if len(maps) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = len(maps)
	}
	pipe := m.c.rc.Pipeline()
	n := 0
	flush := func() error {
		if pipe.Len() == 0 {
			return nil
		}
		_, err := pipe.Exec(ctx)
		pipe = m.c.rc.Pipeline()
		return err
	}
	for name, entries := range maps {
		_ = pipe.Del(ctx, name)
		if len(entries) > 0 {
			fields := make([]any, 0, 2*len(entries))
			for k, v := range entries {
				enc, err := m.c.codec.Encode(v)
				if err != nil {
					return err
				}
				fields = append(fields, encodeKey(m.c.codec, k), enc)
			}
			_ = pipe.HSet(ctx, name, fields...)
		}
		n++
		if n%batchSize == 0 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}
