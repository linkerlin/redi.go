package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RQueue is a distributed FIFO queue backed by a Redis List.
// Offer appends to the tail (RPUSH) and Poll removes from the head (LPOP).
// Elements are codec (JSON) encoded, Redisson-compatible.
type RQueue struct {
	rObject
}

func newRQueue(c *Client, name string) *RQueue {
	return &RQueue{rObject{c: c, name: name}}
}

// Offer adds values to the tail of the queue.
func (q *RQueue) Offer(ctx context.Context, values ...any) error {
	enc, err := q.encodeAll(values)
	if err != nil {
		return err
	}
	return q.rc().RPush(ctx, q.name, enc...).Err()
}

// Poll removes and returns the head element.
// Returns (nil, nil) when the queue is empty.
func (q *RQueue) Poll(ctx context.Context) (any, error) {
	v, err := q.rc().LPop(ctx, q.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return q.c.codec.Decode(v)
}

// PollInto removes the head and decodes it into target.
// Returns false when the queue is empty.
func (q *RQueue) PollInto(ctx context.Context, target any) (bool, error) {
	v, err := q.rc().LPop(ctx, q.name).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, decodeInto(q.c.codec, v, target)
}

// Peek returns the head element without removing it.
// Returns (nil, nil) when the queue is empty.
func (q *RQueue) Peek(ctx context.Context) (any, error) {
	v, err := q.rc().LIndex(ctx, q.name, 0).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return q.c.codec.Decode(v)
}

// PeekInto decodes the head into target without removing it.
// Returns false when the queue is empty.
func (q *RQueue) PeekInto(ctx context.Context, target any) (bool, error) {
	v, err := q.rc().LIndex(ctx, q.name, 0).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, decodeInto(q.c.codec, v, target)
}

// Take blocks until an element is available or ctx is cancelled, then
// removes and returns it. Sub-second timeouts are rounded up to 1s
// (BLPOP has second granularity).
func (q *RQueue) Take(ctx context.Context, timeout time.Duration) (any, error) {
	secs := int64(timeout.Seconds())
	if timeout > 0 && secs == 0 {
		secs = 1
	}
	res, err := q.rc().BLPop(ctx, time.Duration(secs)*time.Second, q.name).Result()
	if err == redis.Nil || err == context.DeadlineExceeded {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) < 2 {
		return nil, nil
	}
	return q.c.codec.Decode(res[1])
}

// Size returns the number of elements in the queue.
func (q *RQueue) Size(ctx context.Context) (int64, error) {
	return q.rc().LLen(ctx, q.name).Result()
}

// ReadAll returns all elements without removing them (LRANGE 0 -1).
func (q *RQueue) ReadAll(ctx context.Context) ([]any, error) {
	vals, err := q.rc().LRange(ctx, q.name, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]any, len(vals))
	for i, v := range vals {
		d, err := q.c.codec.Decode(v)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

// Clear removes all elements.
func (q *RQueue) Clear(ctx context.Context) error { return q.Delete(ctx) }

func (q *RQueue) encodeAll(values []any) ([]any, error) {
	enc := make([]any, len(values))
	for i, v := range values {
		s, err := q.c.codec.Encode(v)
		if err != nil {
			return nil, err
		}
		enc[i] = s
	}
	return enc, nil
}
