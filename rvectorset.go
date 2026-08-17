package redi

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RVectorSet wraps Redis 8+ Vector Set commands (VADD/VSIM/…).
type RVectorSet struct{ rObject }

func newRVectorSet(c *Client, name string) *RVectorSet {
	return &RVectorSet{rObject{c: c, name: name}}
}

// Add inserts or updates element with vector coordinates.
func (v *RVectorSet) Add(ctx context.Context, element string, vector []float64) (bool, error) {
	args := []any{"VADD", v.name, "VALUES", len(vector)}
	for _, x := range vector {
		args = append(args, x)
	}
	args = append(args, element)
	res, err := v.c.rc.Do(ctx, args...).Result()
	if err != nil {
		return false, err
	}
	switch n := res.(type) {
	case bool:
		return n, nil
	case int64:
		return n == 1, nil
	default:
		return firstInt64(res) == 1, nil
	}
}

// SimilarByElement returns nearest neighbour names for element.
func (v *RVectorSet) SimilarByElement(ctx context.Context, element string, count int) ([]string, error) {
	if count <= 0 {
		count = 10
	}
	res, err := v.c.rc.Do(ctx, "VSIM", v.name, "ELE", element, "COUNT", count).Slice()
	if err != nil {
		return nil, err
	}
	return toStringSlice(res), nil
}

// SimilarByVector returns nearest neighbours for a query vector.
func (v *RVectorSet) SimilarByVector(ctx context.Context, vector []float64, count int) ([]string, error) {
	if count <= 0 {
		count = 10
	}
	args := []any{"VSIM", v.name, "VALUES", len(vector)}
	for _, x := range vector {
		args = append(args, x)
	}
	args = append(args, "COUNT", count)
	res, err := v.c.rc.Do(ctx, args...).Slice()
	if err != nil {
		return nil, err
	}
	return toStringSlice(res), nil
}

// GetVector returns approximate coordinates for element.
func (v *RVectorSet) GetVector(ctx context.Context, element string) ([]float64, error) {
	res, err := v.c.rc.Do(ctx, "VEMB", v.name, element).Slice()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]float64, 0, len(res))
	for _, item := range res {
		switch n := item.(type) {
		case float64:
			out = append(out, n)
		case string:
			var f float64
			_, _ = fmt.Sscan(n, &f)
			out = append(out, f)
		case int64:
			out = append(out, float64(n))
		}
	}
	return out, nil
}

// Size returns VCARD.
func (v *RVectorSet) Size(ctx context.Context) (int64, error) {
	res, err := v.c.rc.Do(ctx, "VCARD", v.name).Result()
	return firstInt64(res), err
}

// Dimensions returns VDIM.
func (v *RVectorSet) Dimensions(ctx context.Context) (int64, error) {
	res, err := v.c.rc.Do(ctx, "VDIM", v.name).Result()
	return firstInt64(res), err
}

// Contains reports membership.
func (v *RVectorSet) Contains(ctx context.Context, element string) (bool, error) {
	res, err := v.c.rc.Do(ctx, "VISMEMBER", v.name, element).Result()
	if err != nil {
		return false, err
	}
	switch n := res.(type) {
	case bool:
		return n, nil
	default:
		return firstInt64(res) == 1, nil
	}
}

// Remove deletes element.
func (v *RVectorSet) Remove(ctx context.Context, element string) (bool, error) {
	res, err := v.c.rc.Do(ctx, "VREM", v.name, element).Result()
	if err != nil {
		return false, err
	}
	switch n := res.(type) {
	case bool:
		return n, nil
	default:
		return firstInt64(res) == 1, nil
	}
}

func toStringSlice(res []any) []string {
	out := make([]string, 0, len(res))
	for _, item := range res {
		switch s := item.(type) {
		case string:
			out = append(out, s)
		case []byte:
			out = append(out, string(s))
		default:
			out = append(out, fmt.Sprint(s))
		}
	}
	return out
}

func skipVectorSet(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unknown command") &&
		(strings.Contains(s, "vadd") || strings.Contains(s, "vcard") || strings.Contains(s, "vsim"))
}
