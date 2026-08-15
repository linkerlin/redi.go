package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// prefixName mirrors Redisson's prefixName helper: "prefix:{name}" with a
// hash-tag so the companion key lands in the same cluster slot as name.
func prefixName(prefix, name string) string {
	if containsByte(name, '{') {
		return prefix + ":" + name
	}
	return prefix + ":{" + name + "}"
}

// suffixName mirrors Redisson's suffixName helper: "{name}:suffix".
func suffixName(name, suffix string) string {
	if containsByte(name, '{') {
		return name + ":" + suffix
	}
	return "{" + name + "}:" + suffix
}

func containsByte(s string, b byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return true
		}
	}
	return false
}

// rObject is embedded by every R* structure. It provides the RObject and
// RExpirable surfaces (delete/exists/touch/rename/expire/...) shared with
// Redisson, and gives all structures access to the client, codec and name.
type rObject struct {
	c    *Client
	name string
	// cmds overrides the command target (a pipeline inside an RBatch);
	// nil means "use the client's connection".
	cmds redis.Cmdable
}

// Name returns the Redis key name of the object.
func (o *rObject) Name() string { return o.name }

func (o *rObject) rc() redis.Cmdable {
	if o.cmds != nil {
		return o.cmds
	}
	return o.c.rc
}

// subscribe always routes to the real client connection: subscriptions are
// long-lived and never batchable, even when the structure is pipeline-bound.
func (o *rObject) subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return o.c.rc.Subscribe(ctx, channels...)
}

func (o *rObject) psubscribe(ctx context.Context, patterns ...string) *redis.PubSub {
	return o.c.rc.PSubscribe(ctx, patterns...)
}

// Delete removes the object's key.
func (o *rObject) Delete(ctx context.Context) error {
	return o.rc().Del(ctx, o.name).Err()
}

// Unlink removes the key asynchronously (non-blocking Redis UNLINK).
func (o *rObject) Unlink(ctx context.Context) error {
	return o.rc().Unlink(ctx, o.name).Err()
}

// Exists reports whether the key exists.
func (o *rObject) Exists(ctx context.Context) (bool, error) {
	n, err := o.rc().Exists(ctx, o.name).Result()
	return n > 0, err
}

// Touch updates the last-access time of the key.
func (o *rObject) Touch(ctx context.Context) (bool, error) {
	n, err := o.rc().Touch(ctx, o.name).Result()
	return n > 0, err
}

// Rename renames the object's key. Note: for structures with companion keys
// (locks, delayed queues, map caches) the companion keys are not moved.
func (o *rObject) Rename(ctx context.Context, newName string) error {
	if err := o.rc().Rename(ctx, o.name, newName).Err(); err != nil {
		return err
	}
	o.name = newName
	return nil
}

// Expire sets a TTL on the key.
func (o *rObject) Expire(ctx context.Context, ttl time.Duration) (bool, error) {
	return o.rc().Expire(ctx, o.name, ttl).Result()
}

// ExpireAt sets an absolute expiry time.
func (o *rObject) ExpireAt(ctx context.Context, ts time.Time) (bool, error) {
	return o.rc().ExpireAt(ctx, o.name, ts).Result()
}

// ClearExpire removes the TTL (key becomes persistent).
func (o *rObject) ClearExpire(ctx context.Context) (bool, error) {
	return o.rc().Persist(ctx, o.name).Result()
}

// RemainTTL returns the remaining time-to-live (-1 no expiry, -2 missing).
func (o *rObject) RemainTTL(ctx context.Context) (time.Duration, error) {
	d, err := o.rc().TTL(ctx, o.name).Result()
	return d, err
}

// serverNowMs returns the Redis server time in milliseconds. Score-based
// structures (delayed queue, map cache, rate limiter) use the server clock
// so multi-host deployments are immune to local clock skew.
func (o *rObject) serverNowMs(ctx context.Context) (int64, error) {
	t, err := o.rc().Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}
