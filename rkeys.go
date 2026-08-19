package redi

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RKeys provides keyspace management (SCAN iteration, pattern deletion,
// counts), mirroring Redisson's RKeys.
type RKeys struct {
	c *Client
}

func newRKeys(c *Client) *RKeys { return &RKeys{c: c} }

// Count returns the total number of keys in the database (DBSIZE).
func (k *RKeys) Count(ctx context.Context) (int64, error) {
	return k.c.rc.DBSize(ctx).Result()
}

// CountExists returns how many of the given keys exist.
func (k *RKeys) CountExists(ctx context.Context, names ...string) (int64, error) {
	if len(names) == 0 {
		return 0, nil
	}
	return k.c.rc.Exists(ctx, names...).Result()
}

// RandomKey returns a random key ("" when the db is empty).
func (k *RKeys) RandomKey(ctx context.Context) (string, error) {
	return k.c.rc.RandomKey(ctx).Result()
}

// Copy copies a key (Redis COPY).
func (k *RKeys) Copy(ctx context.Context, key, newKey string, replace bool) (bool, error) {
	n, err := k.c.rc.Copy(ctx, key, newKey, 0, replace).Result()
	return n > 0, err
}

// Type returns the type of a key ("none" when missing).
func (k *RKeys) Type(ctx context.Context, key string) (string, error) {
	return k.c.rc.Type(ctx, key).Result()
}

// Expire sets a TTL on a key.
func (k *RKeys) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return k.c.rc.Expire(ctx, key, ttl).Result()
}

// ExpireMany sets the same TTL on many keys and returns how many succeeded
// (Redisson expire(Duration, names...)).
func (k *RKeys) ExpireMany(ctx context.Context, ttl time.Duration, names ...string) (int64, error) {
	if len(names) == 0 {
		return 0, nil
	}
	pipe := k.c.rc.Pipeline()
	cmds := make([]*redis.BoolCmd, len(names))
	for i, name := range names {
		cmds[i] = pipe.Expire(ctx, name, ttl)
	}
	_, err := pipe.Exec(ctx)
	var n int64
	for _, cmd := range cmds {
		if cmd.Err() == nil && cmd.Val() {
			n++
		}
	}
	return n, err
}

// GetSlot returns the Redis Cluster hash slot for key (CRC16 with hash-tag).
func (k *RKeys) GetSlot(key string) int {
	return redisSlot(key)
}

// ExpireAt sets an absolute expiry time on a key.
func (k *RKeys) ExpireAt(ctx context.Context, key string, at time.Time) (bool, error) {
	return k.c.rc.ExpireAt(ctx, key, at).Result()
}

// ClearExpire removes a key's TTL.
func (k *RKeys) ClearExpire(ctx context.Context, key string) (bool, error) {
	return k.c.rc.Persist(ctx, key).Result()
}

// RemainTTL returns the remaining TTL with millisecond precision
// (-1 no expiry, -2 missing).
func (k *RKeys) RemainTTL(ctx context.Context, key string) (time.Duration, error) {
	return k.c.rc.PTTL(ctx, key).Result()
}

// Rename renames a key.
func (k *RKeys) Rename(ctx context.Context, key, newKey string) error {
	return k.c.rc.Rename(ctx, key, newKey).Err()
}

// RenameNX renames a key only when newKey does not exist.
func (k *RKeys) RenameNX(ctx context.Context, key, newKey string) (bool, error) {
	return k.c.rc.RenameNX(ctx, key, newKey).Result()
}

// Touch updates the last-access time of keys.
func (k *RKeys) Touch(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return k.c.rc.Touch(ctx, keys...).Result()
}

// Move moves a key to another Redis database.
func (k *RKeys) Move(ctx context.Context, key string, db int) (bool, error) {
	return k.c.rc.Move(ctx, key, db).Result()
}

// Migrate atomically moves key to another Redis instance.
func (k *RKeys) Migrate(
	ctx context.Context,
	key, host string,
	port, db int,
	timeout time.Duration,
) error {
	return k.c.rc.Migrate(
		ctx, host, strconv.Itoa(port), key, db, timeout,
	).Err()
}

// SwapDB swaps two databases on the connected Redis server.
func (k *RKeys) SwapDB(ctx context.Context, db1, db2 int) error {
	return k.c.rc.Do(ctx, "SWAPDB", db1, db2).Err()
}

// Delete removes keys, returning the count removed.
func (k *RKeys) Delete(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return k.c.rc.Del(ctx, keys...).Result()
}

// Unlink removes keys asynchronously, returning the count unlinked.
func (k *RKeys) Unlink(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return k.c.rc.Unlink(ctx, keys...).Result()
}

// ForEachKey iterates every key (optionally matching a glob pattern) via
// SCAN, invoking fn for each. SCAN is incremental and safe for live use;
// fn returning an error stops the iteration and propagates it.
func (k *RKeys) ForEachKey(ctx context.Context, pattern string, fn func(key string) error) error {
	var cursor uint64
	for {
		keys, next, err := k.c.rc.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := fn(key); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// Keys collects up to the first 10000 keys matching a pattern.
func (k *RKeys) Keys(ctx context.Context, pattern string) ([]string, error) {
	var out []string
	err := k.ForEachKey(ctx, pattern, func(key string) error {
		out = append(out, key)
		return nil
	})
	return out, err
}

// DeleteByPattern removes every key matching a glob pattern (SCAN + DEL
// batches), returning the count removed.
func (k *RKeys) DeleteByPattern(ctx context.Context, pattern string) (int64, error) {
	return k.deletePattern(ctx, pattern, false)
}

// UnlinkByPattern is DeleteByPattern with async UNLINK.
func (k *RKeys) UnlinkByPattern(ctx context.Context, pattern string) (int64, error) {
	return k.deletePattern(ctx, pattern, true)
}

func (k *RKeys) deletePattern(ctx context.Context, pattern string, unlink bool) (int64, error) {
	var total int64
	var batch []string
	remove := func() error {
		if len(batch) == 0 {
			return nil
		}
		var err error
		var n int64
		if unlink {
			n, err = k.c.rc.Unlink(ctx, batch...).Result()
		} else {
			n, err = k.c.rc.Del(ctx, batch...).Result()
		}
		if err == nil {
			total += n
		}
		batch = batch[:0]
		return err
	}
	// ponytail: batch of 100 per round-trip; raise when deleting millions of keys.
	err := k.ForEachKey(ctx, pattern, func(key string) error {
		batch = append(batch, key)
		if len(batch) >= 100 {
			return remove()
		}
		return nil
	})
	if err != nil {
		return total, err
	}
	return total, remove()
}

// FlushDB empties the current database.
func (k *RKeys) FlushDB(ctx context.Context) error {
	return k.c.rc.FlushDB(ctx).Err()
}

// FlushAll empties every database. In cluster mode it runs on every master.
func (k *RKeys) FlushAll(ctx context.Context) error {
	if cluster, ok := k.c.rc.(*redis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushAll(ctx).Err()
		})
	}
	return k.c.rc.FlushAll(ctx).Err()
}
