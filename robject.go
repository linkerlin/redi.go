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

// ExpireIfSet is PEXPIRE XX (Redisson expireIfSet(Duration)).
func (o *rObject) ExpireIfSet(ctx context.Context, ttl time.Duration) (bool, error) {
	return o.expireIf(ctx, ttl.Milliseconds(), "XX")
}

// ExpireIfNotSet is PEXPIRE NX (Redisson expireIfNotSet(Duration)).
func (o *rObject) ExpireIfNotSet(ctx context.Context, ttl time.Duration) (bool, error) {
	return o.expireIf(ctx, ttl.Milliseconds(), "NX")
}

// ExpireIfGreater is PEXPIRE GT (Redisson expireIfGreater(Duration)).
func (o *rObject) ExpireIfGreater(ctx context.Context, ttl time.Duration) (bool, error) {
	return o.expireIf(ctx, ttl.Milliseconds(), "GT")
}

// ExpireIfLess is PEXPIRE LT (Redisson expireIfLess(Duration)).
func (o *rObject) ExpireIfLess(ctx context.Context, ttl time.Duration) (bool, error) {
	return o.expireIf(ctx, ttl.Milliseconds(), "LT")
}

// ExpireIfSetAt is PEXPIREAT XX (Redisson expireIfSet(Instant)).
func (o *rObject) ExpireIfSetAt(ctx context.Context, ts time.Time) (bool, error) {
	return o.expireIfAt(ctx, ts.UnixMilli(), "XX")
}

// ExpireIfNotSetAt is PEXPIREAT NX (Redisson expireIfNotSet(Instant)).
func (o *rObject) ExpireIfNotSetAt(ctx context.Context, ts time.Time) (bool, error) {
	return o.expireIfAt(ctx, ts.UnixMilli(), "NX")
}

// ExpireIfGreaterAt is PEXPIREAT GT (Redisson expireIfGreater(Instant)).
func (o *rObject) ExpireIfGreaterAt(ctx context.Context, ts time.Time) (bool, error) {
	return o.expireIfAt(ctx, ts.UnixMilli(), "GT")
}

// ExpireIfLessAt is PEXPIREAT LT (Redisson expireIfLess(Instant)).
func (o *rObject) ExpireIfLessAt(ctx context.Context, ts time.Time) (bool, error) {
	return o.expireIfAt(ctx, ts.UnixMilli(), "LT")
}

func (o *rObject) expireIf(ctx context.Context, ttlMs int64, mode string) (bool, error) {
	n, err := expireIfScript.Run(ctx, o.rc(), []string{o.name}, ttlMs, mode).Int()
	return n == 1, err
}

func (o *rObject) expireIfAt(ctx context.Context, unixMs int64, mode string) (bool, error) {
	n, err := expireIfAtScript.Run(ctx, o.rc(), []string{o.name}, unixMs, mode).Int()
	return n == 1, err
}

// ExpireTime is PEXPIRETIME (unix ms). -1 means no expiry, -2 missing
// (Redisson getExpireTime). go-redis maps Redis -1/-2 to nanosecond
// durations rather than milliseconds, so those sentinels are unwrapped
// before converting a real timestamp.
func (o *rObject) ExpireTime(ctx context.Context) (int64, error) {
	d, err := o.rc().PExpireTime(ctx, o.name).Result()
	if err != nil {
		return 0, err
	}
	if d == -1 || d == -2 {
		return int64(d), nil
	}
	return d.Milliseconds(), nil
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

// Dump serializes the key with Redis DUMP.
func (o *rObject) Dump(ctx context.Context) ([]byte, error) {
	raw, err := o.rc().Dump(ctx, o.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(raw), nil
}

// Restore materializes DUMP bytes onto this key (fails if the key exists).
func (o *rObject) Restore(ctx context.Context, state []byte) error {
	return o.rc().Restore(ctx, o.name, 0, string(state)).Err()
}

// RestoreAndReplace is Restore with REPLACE.
func (o *rObject) RestoreAndReplace(ctx context.Context, state []byte) error {
	return o.rc().RestoreReplace(ctx, o.name, 0, string(state)).Err()
}

// Copy copies this key to dest in the current database (Redis COPY).
func (o *rObject) Copy(ctx context.Context, dest string) (bool, error) {
	n, err := o.rc().Copy(ctx, o.name, dest, o.c.cfg.DB, false).Result()
	return n == 1, err
}

// CopyAndReplace is Copy with REPLACE.
func (o *rObject) CopyAndReplace(ctx context.Context, dest string) (bool, error) {
	n, err := o.rc().Copy(ctx, o.name, dest, o.c.cfg.DB, true).Result()
	return n == 1, err
}

// IdleTime returns seconds since last access (OBJECT IDLETIME).
func (o *rObject) IdleTime(ctx context.Context) (time.Duration, error) {
	d, err := o.rc().ObjectIdleTime(ctx, o.name).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return d, err
}

// SizeInMemory returns MEMORY USAGE bytes (0 when the key is missing).
func (o *rObject) SizeInMemory(ctx context.Context) (int64, error) {
	n, err := o.rc().MemoryUsage(ctx, o.name).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
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

// Redisson expireIf*(Duration): PEXPIRE with NX/XX/GT/LT.
var expireIfScript = redis.NewScript(`
if ARGV[2] ~= '' then
    return redis.call('pexpire', KEYS[1], ARGV[1], ARGV[2])
end
return redis.call('pexpire', KEYS[1], ARGV[1])
`)

// Redisson expireIf*(Instant): PEXPIREAT with NX/XX/GT/LT.
var expireIfAtScript = redis.NewScript(`
if ARGV[2] ~= '' then
    return redis.call('pexpireat', KEYS[1], ARGV[1], ARGV[2])
end
return redis.call('pexpireat', KEYS[1], ARGV[1])
`)
