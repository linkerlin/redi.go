package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RBoundedBlockingQueue is Redisson's capacity-bounded FIFO queue. Values
// live in the queue LIST and available capacity in redisson_bqs:{name}.
type RBoundedBlockingQueue struct {
	*RQueue
	capacityKey string
	channel     string
}

func newRBoundedBlockingQueue(c *Client, name string) *RBoundedBlockingQueue {
	capacityKey := prefixName("redisson_bqs", name)
	return &RBoundedBlockingQueue{
		RQueue:      newRQueue(c, name),
		capacityKey: capacityKey,
		channel:     prefixName("redisson_sc", capacityKey),
	}
}

var boundedQueueOfferScript = redis.NewScript(`
local value = redis.call('get', KEYS[1])
assert(value ~= false, 'Capacity of queue ' .. KEYS[1] .. ' has not been set')
if tonumber(value) >= 1 then
    redis.call('decrby', KEYS[1], 1)
    redis.call('rpush', KEYS[2], ARGV[1])
    return 1
end
return 0
`)

var boundedQueuePollScript = redis.NewScript(`
local res = redis.call('lpop', KEYS[1])
if res ~= false then
    local value = redis.call('incrby', KEYS[2], 1)
    redis.call('publish', KEYS[3], value)
end
return res
`)

var boundedQueueCapacityScript = redis.NewScript(`
local value = redis.call('get', KEYS[1])
if value == false then
    redis.call('set', KEYS[1], ARGV[1])
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
end
return 0
`)

var boundedQueueClearScript = redis.NewScript(`
local len = redis.call('llen', KEYS[1])
if len > 0 then
    redis.call('del', KEYS[1])
    local value = redis.call('incrby', KEYS[2], len)
    redis.call('publish', KEYS[3], value)
end
return len
`)

// TrySetCapacity initializes capacity once. Later calls leave it unchanged.
func (q *RBoundedBlockingQueue) TrySetCapacity(
	ctx context.Context, capacity int64,
) (bool, error) {
	n, err := boundedQueueCapacityScript.Run(ctx, q.rc(),
		[]string{q.capacityKey, q.channel}, capacity).Int()
	return n == 1, err
}

// RemainingCapacity returns the number of values that can be added.
func (q *RBoundedBlockingQueue) RemainingCapacity(ctx context.Context) (int64, error) {
	n, err := q.rc().Get(ctx, q.capacityKey).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
}

// Offer adds value when capacity is available and returns false when full.
func (q *RBoundedBlockingQueue) Offer(ctx context.Context, value any) (bool, error) {
	encoded, err := q.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	return q.offerEncoded(ctx, encoded)
}

// OfferWait waits up to wait for capacity and then atomically appends value.
func (q *RBoundedBlockingQueue) OfferWait(
	ctx context.Context, value any, wait time.Duration,
) (bool, error) {
	if wait <= 0 {
		return q.Offer(ctx, value)
	}
	encoded, err := q.c.codec.Encode(value)
	if err != nil {
		return false, err
	}
	if ok, err := q.offerEncoded(ctx, encoded); err != nil || ok {
		return ok, err
	}

	deadline := time.Now().Add(wait)
	sub := q.subscribe(ctx, q.channel)
	defer sub.Close() //nolint:errcheck // connection teardown
	wake := sub.Channel()
	for {
		if ok, err := q.offerEncoded(ctx, encoded); err != nil || ok {
			return ok, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-wake:
		case <-time.After(minDuration(remaining, time.Second)):
		}
	}
}

// Put blocks until value is appended or ctx is cancelled.
func (q *RBoundedBlockingQueue) Put(ctx context.Context, value any) error {
	encoded, err := q.c.codec.Encode(value)
	if err != nil {
		return err
	}
	if ok, err := q.offerEncoded(ctx, encoded); err != nil || ok {
		return err
	}

	sub := q.subscribe(ctx, q.channel)
	defer sub.Close() //nolint:errcheck // connection teardown
	wake := sub.Channel()
	for {
		if ok, err := q.offerEncoded(ctx, encoded); err != nil || ok {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-time.After(time.Second):
		}
	}
}

// Poll removes the head and releases one capacity permit.
func (q *RBoundedBlockingQueue) Poll(ctx context.Context) (any, error) {
	value, err := q.pollEncoded(ctx)
	if err != nil || value == "" {
		return nil, err
	}
	return q.c.codec.Decode(value)
}

// PollInto removes the head and decodes it into target.
func (q *RBoundedBlockingQueue) PollInto(
	ctx context.Context, target any,
) (bool, error) {
	value, err := q.pollEncoded(ctx)
	if err != nil || value == "" {
		return false, err
	}
	return true, decodeInto(q.c.codec, value, target)
}

// Take blocks until an element is available or timeout elapses, then releases
// one capacity permit. A zero timeout blocks until ctx is cancelled.
func (q *RBoundedBlockingQueue) Take(
	ctx context.Context, timeout time.Duration,
) (any, error) {
	seconds := int64(timeout.Seconds())
	if timeout > 0 && seconds == 0 {
		seconds = 1
	}
	result, err := q.rc().BLPop(ctx, time.Duration(seconds)*time.Second, q.name).Result()
	if err == redis.Nil || err == context.DeadlineExceeded {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(result) < 2 {
		return nil, nil
	}
	if err := q.releaseCapacity(ctx, 1); err != nil {
		return nil, err
	}
	return q.c.codec.Decode(result[1])
}

// Clear removes all queued values and restores their capacity.
func (q *RBoundedBlockingQueue) Clear(ctx context.Context) error {
	return boundedQueueClearScript.Run(ctx, q.rc(),
		[]string{q.name, q.capacityKey, q.channel}).Err()
}

// Delete removes both the queue and its capacity companion.
func (q *RBoundedBlockingQueue) Delete(ctx context.Context) error {
	return q.rc().Del(ctx, q.name, q.capacityKey).Err()
}

func (q *RBoundedBlockingQueue) offerEncoded(
	ctx context.Context, encoded string,
) (bool, error) {
	n, err := boundedQueueOfferScript.Run(ctx, q.rc(),
		[]string{q.capacityKey, q.name}, encoded).Int()
	return n == 1, err
}

func (q *RBoundedBlockingQueue) pollEncoded(ctx context.Context) (string, error) {
	value, err := boundedQueuePollScript.Run(ctx, q.rc(),
		[]string{q.name, q.capacityKey, q.channel}).Text()
	if err == redis.Nil {
		return "", nil
	}
	return value, err
}

func (q *RBoundedBlockingQueue) releaseCapacity(
	ctx context.Context, permits int64,
) error {
	return semReleaseScript.Run(ctx, q.rc(),
		[]string{q.capacityKey, q.channel}, permits).Err()
}
