package redi

import (
	"context"

	"github.com/linkerlin/gotrycatch"
	"github.com/redis/go-redis/v9"
)

// RMap is a distributed map backed by a Redis Hash.
type RMap struct {
	rc   *redis.Client
	name string
	key  string
}

func newRMap(rc *redis.Client, name string) *RMap {
	return &RMap{rc: rc, name: name, key: "redi:map:" + name}
}

// Put sets field in the map to value.
func (m *RMap) Put(ctx context.Context, field string, value any) error {
	var putErr error
	tb := gotrycatch.Try(func() {
		if err := m.rc.HSet(ctx, m.key, field, value).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { putErr = err })
	tb.Finally(func() {})
	return putErr
}

// Get retrieves the value for field. Returns ("", redis.Nil) if not found.
func (m *RMap) Get(ctx context.Context, field string) (string, error) {
	var val string
	var getErr error
	tb := gotrycatch.Try(func() {
		v, err := m.rc.HGet(ctx, m.key, field).Result()
		if err != nil {
			panic(err)
		}
		val = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { getErr = err })
	tb.Finally(func() {})
	return val, getErr
}

// Delete removes one or more fields from the map.
func (m *RMap) Delete(ctx context.Context, fields ...string) error {
	var delErr error
	tb := gotrycatch.Try(func() {
		if err := m.rc.HDel(ctx, m.key, fields...).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { delErr = err })
	tb.Finally(func() {})
	return delErr
}

// GetAll returns all field-value pairs in the map.
func (m *RMap) GetAll(ctx context.Context) (map[string]string, error) {
	var result map[string]string
	var getErr error
	tb := gotrycatch.Try(func() {
		v, err := m.rc.HGetAll(ctx, m.key).Result()
		if err != nil {
			panic(err)
		}
		result = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { getErr = err })
	tb.Finally(func() {})
	return result, getErr
}

// ContainsKey reports whether the map contains the given field.
func (m *RMap) ContainsKey(ctx context.Context, field string) (bool, error) {
	var exists bool
	var existsErr error
	tb := gotrycatch.Try(func() {
		v, err := m.rc.HExists(ctx, m.key, field).Result()
		if err != nil {
			panic(err)
		}
		exists = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { existsErr = err })
	tb.Finally(func() {})
	return exists, existsErr
}

// Size returns the number of fields in the map.
func (m *RMap) Size(ctx context.Context) (int64, error) {
	var sz int64
	var szErr error
	tb := gotrycatch.Try(func() {
		v, err := m.rc.HLen(ctx, m.key).Result()
		if err != nil {
			panic(err)
		}
		sz = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { szErr = err })
	tb.Finally(func() {})
	return sz, szErr
}

// Clear removes all entries from the map.
func (m *RMap) Clear(ctx context.Context) error {
	var clearErr error
	tb := gotrycatch.Try(func() {
		if err := m.rc.Del(ctx, m.key).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { clearErr = err })
	tb.Finally(func() {})
	return clearErr
}
