package redi

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// RBloomFilterNative wraps Redis BF.* commands (RedissonBloomFilterNative).
// Elements are codec-encoded before insertion so JsonJacksonCodec clients match.
type RBloomFilterNative struct{ rObject }

func newRBloomFilterNative(c *Client, name string) *RBloomFilterNative {
	return &RBloomFilterNative{rObject{c: c, name: name}}
}

// TryInit reserves the filter (BF.RESERVE). Returns false when the key already exists.
func (f *RBloomFilterNative) TryInit(ctx context.Context, errorRate float64, capacity int64) (bool, error) {
	err := f.rc().BFReserve(ctx, f.name, errorRate, capacity).Err()
	if err != nil {
		if isBusyKeyErr(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Add inserts element; true when newly set.
func (f *RBloomFilterNative) Add(ctx context.Context, element any) (bool, error) {
	enc, err := f.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	return f.rc().BFAdd(ctx, f.name, enc).Result()
}

// Contains reports whether element may be present.
func (f *RBloomFilterNative) Contains(ctx context.Context, element any) (bool, error) {
	enc, err := f.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	return f.rc().BFExists(ctx, f.name, enc).Result()
}

// AddAll inserts many elements; returns per-element "newly set" flags.
func (f *RBloomFilterNative) AddAll(ctx context.Context, elements ...any) ([]bool, error) {
	args, err := encodeAnySlice(f.c.codec, elements)
	if err != nil {
		return nil, err
	}
	return f.rc().BFMAdd(ctx, f.name, args...).Result()
}

// ContainsAll returns per-element membership guesses.
func (f *RBloomFilterNative) ContainsAll(ctx context.Context, elements ...any) ([]bool, error) {
	args, err := encodeAnySlice(f.c.codec, elements)
	if err != nil {
		return nil, err
	}
	return f.rc().BFMExists(ctx, f.name, args...).Result()
}

// Card returns BF.CARD (approximate item count).
func (f *RBloomFilterNative) Card(ctx context.Context) (int64, error) {
	return f.rc().BFCard(ctx, f.name).Result()
}

// RCuckooFilter wraps Redis CF.* commands.
type RCuckooFilter struct{ rObject }

func newRCuckooFilter(c *Client, name string) *RCuckooFilter {
	return &RCuckooFilter{rObject{c: c, name: name}}
}

// TryInit reserves capacity (CF.RESERVE).
func (f *RCuckooFilter) TryInit(ctx context.Context, capacity int64) (bool, error) {
	err := f.rc().CFReserve(ctx, f.name, capacity).Err()
	if err != nil {
		if isBusyKeyErr(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Add inserts element.
func (f *RCuckooFilter) Add(ctx context.Context, element any) (bool, error) {
	enc, err := f.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	return f.rc().CFAdd(ctx, f.name, enc).Result()
}

// AddIfAbsent inserts only when absent (CF.ADDNX).
func (f *RCuckooFilter) AddIfAbsent(ctx context.Context, element any) (bool, error) {
	enc, err := f.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	return f.rc().CFAddNX(ctx, f.name, enc).Result()
}

// Contains reports possible membership.
func (f *RCuckooFilter) Contains(ctx context.Context, element any) (bool, error) {
	enc, err := f.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	return f.rc().CFExists(ctx, f.name, enc).Result()
}

// Count returns CF.COUNT for element.
func (f *RCuckooFilter) Count(ctx context.Context, element any) (int64, error) {
	enc, err := f.c.codec.Encode(element)
	if err != nil {
		return 0, err
	}
	return f.rc().CFCount(ctx, f.name, enc).Result()
}

// Delete removes one occurrence of element.
func (f *RCuckooFilter) Delete(ctx context.Context, element any) (bool, error) {
	enc, err := f.c.codec.Encode(element)
	if err != nil {
		return false, err
	}
	return f.rc().CFDel(ctx, f.name, enc).Result()
}

// RTopK wraps Redis TOPK.* commands.
type RTopK struct{ rObject }

func newRTopK(c *Client, name string) *RTopK {
	return &RTopK{rObject{c: c, name: name}}
}

// TryInit reserves a top-k sketch of size k.
func (t *RTopK) TryInit(ctx context.Context, k int64) (bool, error) {
	err := t.rc().TopKReserve(ctx, t.name, k).Err()
	if err != nil {
		if isBusyKeyErr(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Add increments items; returns items dropped from the top-k (possibly empty strings).
func (t *RTopK) Add(ctx context.Context, elements ...any) ([]string, error) {
	args, err := encodeAnySlice(t.c.codec, elements)
	if err != nil {
		return nil, err
	}
	return t.rc().TopKAdd(ctx, t.name, args...).Result()
}

// Query reports whether each element is currently in the top-k.
func (t *RTopK) Query(ctx context.Context, elements ...any) ([]bool, error) {
	args, err := encodeAnySlice(t.c.codec, elements)
	if err != nil {
		return nil, err
	}
	return t.rc().TopKQuery(ctx, t.name, args...).Result()
}

// List returns the current top-k items (encoded strings).
func (t *RTopK) List(ctx context.Context) ([]string, error) {
	return t.rc().TopKList(ctx, t.name).Result()
}

// ListWithCount returns item -> count.
func (t *RTopK) ListWithCount(ctx context.Context) (map[string]int64, error) {
	return t.rc().TopKListWithCount(ctx, t.name).Result()
}

// RTDigest wraps Redis TDIGEST.* commands (numeric sketches; no codec wrap).
type RTDigest struct{ rObject }

func newRTDigest(c *Client, name string) *RTDigest {
	return &RTDigest{rObject{c: c, name: name}}
}

// TryCreate initializes the sketch (TDIGEST.CREATE).
func (t *RTDigest) TryCreate(ctx context.Context) (bool, error) {
	err := t.rc().TDigestCreate(ctx, t.name).Err()
	if err != nil {
		if isBusyKeyErr(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// TryCreateWithCompression initializes with compression.
func (t *RTDigest) TryCreateWithCompression(ctx context.Context, compression int64) (bool, error) {
	err := t.rc().TDigestCreateWithCompression(ctx, t.name, compression).Err()
	if err != nil {
		if isBusyKeyErr(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Add observes values.
func (t *RTDigest) Add(ctx context.Context, values ...float64) error {
	return t.rc().TDigestAdd(ctx, t.name, values...).Err()
}

// Quantile estimates values at the given quantiles (0..1).
func (t *RTDigest) Quantile(ctx context.Context, quantiles ...float64) ([]float64, error) {
	return t.rc().TDigestQuantile(ctx, t.name, quantiles...).Result()
}

// CDF estimates cumulative distribution at the given values.
func (t *RTDigest) CDF(ctx context.Context, values ...float64) ([]float64, error) {
	return t.rc().TDigestCDF(ctx, t.name, values...).Result()
}

// Min / Max / Rank helpers.
func (t *RTDigest) Min(ctx context.Context) (float64, error) {
	return t.rc().TDigestMin(ctx, t.name).Result()
}

func (t *RTDigest) Max(ctx context.Context) (float64, error) {
	return t.rc().TDigestMax(ctx, t.name).Result()
}

// RGcra is Redis GCRA rate limiting (RedissonGcra). Uses the GCRA command
// when available (Redis 8+); tests skip when the server lacks it.
type RGcra struct{ rObject }

func newRGcra(c *Client, name string) *RGcra {
	return &RGcra{rObject{c: c, name: name}}
}

// GcraResult mirrors Redisson's GcraResult.
type GcraResult struct {
	Allowed       bool
	Remaining     int64
	RetryAfterMs  int64
	ResetAfterMs  int64
}

// TryAcquire attempts to take tokens under GCRA.
func (g *RGcra) TryAcquire(ctx context.Context, maxBurst, tokensPerPeriod int64, period time.Duration, tokens int64) (*GcraResult, error) {
	if maxBurst < 0 {
		return nil, fmt.Errorf("redi: maxBurst can't be negative")
	}
	if tokensPerPeriod <= 0 {
		return nil, fmt.Errorf("redi: tokensPerPeriod must be positive")
	}
	if period <= 0 {
		return nil, fmt.Errorf("redi: period must be positive")
	}
	if tokens <= 0 {
		return nil, fmt.Errorf("redi: tokens must be positive")
	}
	secs := period.Seconds()
	args := []any{"GCRA", g.name, maxBurst, tokensPerPeriod, secs}
	if tokens != 1 {
		args = append(args, "TOKENS", tokens)
	}
	res, err := g.c.rc.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}
	out := &GcraResult{}
	if len(res) > 0 {
		out.Allowed = toInt64(res[0]) == 1
	}
	if len(res) > 1 {
		out.Remaining = toInt64(res[1])
	}
	if len(res) > 2 {
		out.RetryAfterMs = toInt64(res[2])
	}
	if len(res) > 3 {
		out.ResetAfterMs = toInt64(res[3])
	}
	return out, nil
}

func encodeAnySlice(c Codec, elements []any) ([]any, error) {
	out := make([]any, len(elements))
	for i, el := range elements {
		enc, err := c.Encode(el)
		if err != nil {
			return nil, err
		}
		out[i] = enc
	}
	return out, nil
}

func isBusyKeyErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "item exists") ||
		strings.Contains(s, "busykey") ||
		strings.Contains(s, "already exists")
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case string:
		var x int64
		_, _ = fmt.Sscan(n, &x)
		return x
	default:
		return 0
	}
}
