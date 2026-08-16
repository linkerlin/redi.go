package redi

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RDelayedQueue offers delayed delivery, wire-compatible with Redisson's
// RedissonDelayedQueue (verified against 4.6.1):
//
//	ZSET redisson_delay_queue_timeout:{name}  deadline index:
//	      member = struct.pack('Bc0Lc0', randomIdLen=8, randomId, encLen, encodedValue)
//	LIST redisson_delay_queue:{name}          the same packed members (ordering +
//	      lrem support); expired members are unpacked and RPUSHed onto {name}
//	LIST {name}                                the target queue consumers poll
//	PUB  redisson_delay_queue_channel:{name}   wake-up channel (head startTime)
//
// A background goroutine migrates expired members (Redis TIME clock).
type RDelayedQueue struct {
	rObject
	timeoutSetKey string
	queueName     string
	channel       string

	startOnce sync.Once
}

func newRDelayedQueue(c *Client, name string) *RDelayedQueue {
	dq := &RDelayedQueue{
		rObject:       rObject{c: c, name: name},
		timeoutSetKey: prefixName("redisson_delay_queue_timeout", name),
		queueName:     prefixName("redisson_delay_queue", name),
		channel:       prefixName("redisson_delay_queue_channel", name),
	}
	dq.startWorker()
	return dq
}

// unpackDelayedMember splits a packed member into (randomID, encoded).
// (Packing happens inside the offer Lua script — struct.pack in Redis — so
// only the decode direction exists in Go.)
func unpackDelayedMember(v string) (string, string, bool) {
	b := []byte(v)
	if len(b) < 1 {
		return "", "", false
	}
	idLen := int(b[0])
	if len(b) < 1+idLen+8 {
		return "", "", false
	}
	id := string(b[1 : 1+idLen])
	encLen := int(binary.LittleEndian.Uint64(b[1+idLen : 1+idLen+8]))
	start := 1 + idLen + 8
	if encLen < 0 || len(b) < start+encLen {
		return "", "", false
	}
	return id, string(b[start : start+encLen]), true
}

// UnpackDelayedMember is the exported test hook over unpackDelayedMember
// (wire contract tests assert the packed layout).
func UnpackDelayedMember(v string) (string, string, bool) { return unpackDelayedMember(v) }

var delayOfferScript = redis.NewScript(`
local value = struct.pack('Bc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3]);
redis.call('zadd', KEYS[2], ARGV[1], value);
redis.call('rpush', KEYS[3], value);
local v = redis.call('zrange', KEYS[2], 0, 0);
if v[1] == value then
    redis.call('publish', KEYS[4], ARGV[1]);
end;
`)

var delayMigrateScript = redis.NewScript(`
local expiredValues = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1], 'limit', 0, ARGV[2]);
if #expiredValues > 0 then
    for i, v in ipairs(expiredValues) do
        local randomId, value = struct.unpack('Bc0Lc0', v);
        redis.call('rpush', KEYS[1], value);
        redis.call('lrem', KEYS[3], 1, v);
    end;
    redis.call('zrem', KEYS[2], unpack(expiredValues));
end;
`)

// delayPollScript pops a value and removes its (random-id-disambiguated)
// counterpart from the timeout set.
var delayPollScript = redis.NewScript(`
local value = redis.call('lpop', KEYS[1]);
if value ~= false then
    local v = redis.call('zrange', KEYS[2], 0, -1);
    for i, m in ipairs(v) do
        local randomId, val = struct.unpack('Bc0Lc0', m);
        if val == value then
            redis.call('zrem', KEYS[2], m);
            break;
        end
    end
    return value;
end
return nil;
`)

// Offer adds element with the given delay.
func (q *RDelayedQueue) Offer(ctx context.Context, element any, delay time.Duration) error {
	enc, err := q.c.codec.Encode(element)
	if err != nil {
		return err
	}
	now, err := q.serverNowMs(ctx)
	if err != nil {
		return err
	}
	randomID := make([]byte, 8)
	if _, err := rand.Read(randomID); err != nil {
		return err
	}
	deadline := now + delay.Milliseconds()
	err = delayOfferScript.Run(ctx, q.rc(),
		[]string{q.name, q.timeoutSetKey, q.queueName, q.channel},
		deadline, string(randomID), enc).Err()
	if err == redis.Nil {
		return nil // EVAL_VOID: Redisson's offer script returns nothing
	}
	return err
}

// MigrateExpired moves all due members onto the target queue.
func (q *RDelayedQueue) MigrateExpired(ctx context.Context) (int64, error) {
	now, err := q.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	// The migration script only moves members; count via ZREM semantics is
	// not returned, so measure the target queue growth instead.
	before, err := q.rc().LLen(ctx, q.name).Result()
	if err != nil {
		return 0, err
	}
	if err := delayMigrateScript.Run(ctx, q.rc(),
		[]string{q.name, q.timeoutSetKey, q.queueName}, now, 100).Err(); err != nil && err != redis.Nil {
		return 0, err
	}
	after, err := q.rc().LLen(ctx, q.name).Result()
	if err != nil {
		return 0, err
	}
	return after - before, nil
}

// Poll removes and returns the head of the ready queue (nil when empty).
func (q *RDelayedQueue) Poll(ctx context.Context) (any, error) {
	if _, err := q.MigrateExpired(ctx); err != nil {
		return nil, err
	}
	res, err := delayPollScript.Run(ctx, q.rc(),
		[]string{q.name, q.timeoutSetKey}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s, _ := res.(string)
	return q.c.codec.Decode(s)
}

// Peek returns the head of the ready queue without removing it.
func (q *RDelayedQueue) Peek(ctx context.Context) (any, error) {
	if _, err := q.MigrateExpired(ctx); err != nil {
		return nil, err
	}
	v, err := q.rc().LIndex(ctx, q.name, 0).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return q.c.codec.Decode(v)
}

// Size returns ready + delayed counts.
func (q *RDelayedQueue) Size(ctx context.Context) (int64, error) {
	ready, err := q.rc().LLen(ctx, q.name).Result()
	if err != nil {
		return 0, err
	}
	delayed, err := q.rc().ZCard(ctx, q.timeoutSetKey).Result()
	if err != nil {
		return 0, err
	}
	return ready + delayed, nil
}

// ReadySize returns the number of immediately available elements.
func (q *RDelayedQueue) ReadySize(ctx context.Context) (int64, error) {
	return q.rc().LLen(ctx, q.name).Result()
}

// DelayedSize returns the number of elements still waiting.
func (q *RDelayedQueue) DelayedSize(ctx context.Context) (int64, error) {
	return q.rc().ZCard(ctx, q.timeoutSetKey).Result()
}

// Delete removes the target queue and both internal keys.
func (q *RDelayedQueue) Delete(ctx context.Context) error {
	return q.rc().Del(ctx, q.name, q.timeoutSetKey, q.queueName).Err()
}

// startWorker runs the migration loop until the client closes.
func (q *RDelayedQueue) startWorker() {
	q.startOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-q.c.ctx.Done():
					return
				case <-ticker.C:
					ctx, cancel := context.WithTimeout(q.c.ctx, 3*time.Second)
					if _, err := q.MigrateExpired(ctx); err != nil {
						q.c.logf("delayed queue %q migrate: %v", q.name, err)
					}
					cancel()
				}
			}
		}()
	})
}
