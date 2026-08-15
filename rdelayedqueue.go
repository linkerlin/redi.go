package redi

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// syncOnce is an alias kept for the worker start guard.
type syncOnce struct {
	sync.Once
}

// RDelayedQueue offers delayed delivery, wire-compatible with Redisson's
// RDelayedQueue:
//
//	ZSET    redisson_delay_queue:{name}          pending elements (score = delivery ts ms)
//	LIST    {name}                                target queue for expired elements
//	PUBSUB  redisson_delay_queue_channel:{name}   wake-up channel
//
// A background goroutine (started on first use, stopped by Client.Close)
// migrates expired elements every second. Times come from the Redis server
// clock (TIME) so hosts with skewed clocks interoperate correctly.
type RDelayedQueue struct {
	rObject
	delayedKey string
	channel    string

	startOnce syncOnce
}

func newRDelayedQueue(c *Client, name string) *RDelayedQueue {
	dq := &RDelayedQueue{
		rObject:    rObject{c: c, name: name},
		delayedKey: prefixName("redisson_delay_queue", name),
		channel:    prefixName("redisson_delay_queue_channel", name),
	}
	dq.startWorker()
	return dq
}

var delayMigrateScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1])
if #expired > 0 then
    redis.call('rpush', KEYS[2], unpack(expired))
    redis.call('zremrangebyscore', KEYS[1], 0, ARGV[1])
end
return #expired
`)

// Offer adds element with the given delay before it becomes visible on the
// target queue.
func (q *RDelayedQueue) Offer(ctx context.Context, element any, delay time.Duration) error {
	enc, err := q.c.codec.Encode(element)
	if err != nil {
		return err
	}
	now, err := q.serverNowMs(ctx)
	if err != nil {
		return err
	}
	delivery := now + delay.Milliseconds()
	if err := q.rc().ZAdd(ctx, q.delayedKey, redis.Z{
		Score:  float64(delivery),
		Member: enc,
	}).Err(); err != nil {
		return err
	}
	return q.rc().Publish(ctx, q.channel, formatFloat(float64(delivery))).Err()
}

// MigrateExpired moves all due elements to the target queue.
func (q *RDelayedQueue) MigrateExpired(ctx context.Context) (int64, error) {
	now, err := q.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	n, err := delayMigrateScript.Run(ctx, q.rc(),
		[]string{q.delayedKey, q.name}, now).Int()
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

// Poll removes and returns the head of the ready queue (nil when empty).
func (q *RDelayedQueue) Poll(ctx context.Context) (any, error) {
	if _, err := q.MigrateExpired(ctx); err != nil {
		return nil, err
	}
	v, err := q.rc().LPop(ctx, q.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return q.c.codec.Decode(v)
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

// Size returns ready + delayed element counts.
func (q *RDelayedQueue) Size(ctx context.Context) (int64, error) {
	ready, err := q.rc().LLen(ctx, q.name).Result()
	if err != nil {
		return 0, err
	}
	delayed, err := q.rc().ZCard(ctx, q.delayedKey).Result()
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
	return q.rc().ZCard(ctx, q.delayedKey).Result()
}

// Delete removes both the target queue and the delayed set.
func (q *RDelayedQueue) Delete(ctx context.Context) error {
	return q.rc().Del(ctx, q.name, q.delayedKey).Err()
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
