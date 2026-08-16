package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RFunction manages Redis functions (FUNCTION LOAD / FCALL), mirroring
// Redisson's RFunction. Redis-native - wire-safe across languages.
type RFunction struct {
	c *Client
}

func newRFunction(c *Client) *RFunction { return &RFunction{c: c} }

// Load registers a library (FUNCTION LOAD, replace=true).
func (f *RFunction) Load(ctx context.Context, code string) error {
	return f.c.rc.FunctionLoadReplace(ctx, code).Err()
}

// Delete removes a library (FUNCTION DELETE).
func (f *RFunction) Delete(ctx context.Context, library string) error {
	return f.c.rc.FunctionDelete(ctx, library).Err()
}

// List returns registered libraries (FUNCTION LIST, with code).
func (f *RFunction) List(ctx context.Context, withCode bool) ([]redis.Library, error) {
	return f.c.rc.FunctionList(ctx, redis.FunctionListQuery{WithCode: withCode}).Result()
}

// Flush removes every library (FUNCTION FLUSH).
func (f *RFunction) Flush(ctx context.Context) error {
	return f.c.rc.FunctionFlush(ctx).Err()
}

// Call invokes a registered function (FCALL). keys are the function's
// key arguments, args the remaining arguments.
func (f *RFunction) Call(ctx context.Context, function string, keys []string, args ...any) (any, error) {
	res, err := f.c.rc.FCall(ctx, function, keys, args...).Result()
	if err == redis.Nil {
		return nil, nil
	}
	return res, err
}

// CallReadOnly invokes via FCALL_RO (read-only replica routing).
func (f *RFunction) CallReadOnly(ctx context.Context, function string, keys []string, args ...any) (any, error) {
	res, err := f.c.rc.FCallRo(ctx, function, keys, args...).Result()
	if err == redis.Nil {
		return nil, nil
	}
	return res, err
}
