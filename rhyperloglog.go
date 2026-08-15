package redi

import (
	"context"
)

// RHyperLogLog estimates cardinality (Redis PFADD/PFCOUNT/PFMERGE).
// Members are codec-encoded; the sketch itself is Redis-native so
// cross-language interop is automatic.
type RHyperLogLog struct {
	rObject
}

func newRHyperLogLog(c *Client, name string) *RHyperLogLog {
	return &RHyperLogLog{rObject{c: c, name: name}}
}

// Add adds an element. Returns true when the register was updated.
func (h *RHyperLogLog) Add(ctx context.Context, element any) (bool, error) {
	enc, err := h.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	n, err := h.rc().PFAdd(ctx, h.name, enc).Result()
	return n == 1, err
}

// AddAll adds multiple elements. Returns true when any register changed.
func (h *RHyperLogLog) AddAll(ctx context.Context, elements ...any) (bool, error) {
	if len(elements) == 0 {
		return false, nil
	}
	enc := make([]any, len(elements))
	for i, e := range elements {
		s, err := h.c.codec.Encode(e)
		if err != nil {
			return false, err
		}
		enc[i] = s
	}
	n, err := h.rc().PFAdd(ctx, h.name, enc...).Result()
	return n == 1, err
}

// Count returns the estimated cardinality.
func (h *RHyperLogLog) Count(ctx context.Context) (int64, error) {
	return h.rc().PFCount(ctx, h.name).Result()
}

// CountWith returns the union cardinality with other HLLs.
func (h *RHyperLogLog) CountWith(ctx context.Context, others ...string) (int64, error) {
	keys := append([]string{h.name}, others...)
	return h.rc().PFCount(ctx, keys...).Result()
}

// MergeWith merges the other HLLs into this one.
func (h *RHyperLogLog) MergeWith(ctx context.Context, others ...string) error {
	return h.rc().PFMerge(ctx, h.name, others...).Err()
}

// MergeInto merges this HLL (and others) into a destination.
func (h *RHyperLogLog) MergeInto(ctx context.Context, dest string, others ...string) error {
	srcs := append([]string{h.name}, others...)
	return h.rc().PFMerge(ctx, dest, srcs...).Err()
}
