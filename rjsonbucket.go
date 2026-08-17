package redi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RJsonBucket stores a JSON document via RedisJSON (JSON.SET/GET/…).
// Requires the RedisJSON module; tests skip when commands are unavailable.
type RJsonBucket struct{ rObject }

func newRJsonBucket(c *Client, name string) *RJsonBucket {
	return &RJsonBucket{rObject{c: c, name: name}}
}

func jsonBytes(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Set stores value at the document root ($).
func (b *RJsonBucket) Set(ctx context.Context, value any) error {
	raw, err := jsonBytes(value)
	if err != nil {
		return err
	}
	return b.c.rc.Do(ctx, "JSON.SET", b.name, "$", raw).Err()
}

// Get returns the document root.
func (b *RJsonBucket) Get(ctx context.Context) (any, error) {
	return b.GetPath(ctx, "$")
}

// GetPath returns the value at path (JSONPath).
func (b *RJsonBucket) GetPath(ctx context.Context, path string) (any, error) {
	if path == "" {
		path = "$"
	}
	s, err := b.c.rc.Do(ctx, "JSON.GET", b.name, path).Text()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return s, nil
	}
	return out, nil
}

// SetPath writes value at path.
func (b *RJsonBucket) SetPath(ctx context.Context, path string, value any) error {
	if path == "" {
		path = "$"
	}
	raw, err := jsonBytes(value)
	if err != nil {
		return err
	}
	return b.c.rc.Do(ctx, "JSON.SET", b.name, path, raw).Err()
}

// SetPathNX sets path only when absent.
func (b *RJsonBucket) SetPathNX(ctx context.Context, path string, value any) (bool, error) {
	if path == "" {
		path = "$"
	}
	raw, err := jsonBytes(value)
	if err != nil {
		return false, err
	}
	res, err := b.c.rc.Do(ctx, "JSON.SET", b.name, path, raw, "NX").Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return res != nil, nil
}

// DeletePath deletes path (or the whole key when path is empty/$ with JSON.DEL).
func (b *RJsonBucket) DeletePath(ctx context.Context, path string) (int64, error) {
	if path == "" || path == "$" {
		return b.c.rc.Do(ctx, "JSON.DEL", b.name).Int64()
	}
	return b.c.rc.Do(ctx, "JSON.DEL", b.name, path).Int64()
}

// ArrayAppend appends values to the array at path.
func (b *RJsonBucket) ArrayAppend(ctx context.Context, path string, values ...any) (int64, error) {
	args := make([]any, 0, 3+len(values))
	args = append(args, "JSON.ARRAPPEND", b.name, path)
	for _, v := range values {
		raw, err := jsonBytes(v)
		if err != nil {
			return 0, err
		}
		args = append(args, raw)
	}
	res, err := b.c.rc.Do(ctx, args...).Result()
	if err != nil {
		return 0, err
	}
	return firstInt64(res), nil
}

// ArrayLen returns the array length at path.
func (b *RJsonBucket) ArrayLen(ctx context.Context, path string) (int64, error) {
	res, err := b.c.rc.Do(ctx, "JSON.ARRLEN", b.name, path).Result()
	if err != nil {
		return 0, err
	}
	return firstInt64(res), nil
}

// ArrayPop pops an element from the array at path.
func (b *RJsonBucket) ArrayPop(ctx context.Context, path string, index int64) (any, error) {
	s, err := b.c.rc.Do(ctx, "JSON.ARRPOP", b.name, path, index).Text()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return s, nil
	}
	return out, nil
}

// RJsonBuckets provides mass get/set for JSON buckets.
type RJsonBuckets struct{ c *Client }

func newRJsonBuckets(c *Client) *RJsonBuckets { return &RJsonBuckets{c: c} }

// Set stores each name→value document at root.
func (b *RJsonBuckets) Set(ctx context.Context, values map[string]any) error {
	pipe := b.c.rc.Pipeline()
	for name, v := range values {
		raw, err := jsonBytes(v)
		if err != nil {
			return err
		}
		_ = pipe.Do(ctx, "JSON.SET", name, "$", raw)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Get loads documents by name; missing keys are omitted.
func (b *RJsonBuckets) Get(ctx context.Context, names ...string) (map[string]any, error) {
	out := make(map[string]any, len(names))
	for _, name := range names {
		s, err := b.c.rc.Do(ctx, "JSON.GET", name, "$").Text()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		var v any
		if err := json.Unmarshal([]byte(s), &v); err != nil {
			out[name] = s
			continue
		}
		out[name] = v
	}
	return out, nil
}

func skipJSONModule(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unknown command") && strings.Contains(s, "json.")
}

func firstInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case []any:
		if len(n) == 0 {
			return 0
		}
		return firstInt64(n[0])
	case string:
		var x int64
		_, _ = fmt.Sscan(n, &x)
		return x
	default:
		return 0
	}
}
