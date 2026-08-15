package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RDeque is a distributed double-ended queue backed by a Redis List.
type RDeque struct {
	rObject
}

func newRDeque(c *Client, name string) *RDeque {
	return &RDeque{rObject{c: c, name: name}}
}

// AddFirst pushes values onto the head (LPUSH).
func (d *RDeque) AddFirst(ctx context.Context, values ...any) error {
	enc, err := d.encodeAll(values)
	if err != nil {
		return err
	}
	return d.rc().LPush(ctx, d.name, enc...).Err()
}

// AddLast pushes values onto the tail (RPUSH).
func (d *RDeque) AddLast(ctx context.Context, values ...any) error {
	enc, err := d.encodeAll(values)
	if err != nil {
		return err
	}
	return d.rc().RPush(ctx, d.name, enc...).Err()
}

// RemoveFirst pops the head element. Returns (nil, nil) when empty.
func (d *RDeque) RemoveFirst(ctx context.Context) (any, error) {
	v, err := d.rc().LPop(ctx, d.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return d.c.codec.Decode(v)
}

// RemoveLast pops the tail element. Returns (nil, nil) when empty.
func (d *RDeque) RemoveLast(ctx context.Context) (any, error) {
	v, err := d.rc().RPop(ctx, d.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return d.c.codec.Decode(v)
}

// PeekFirst returns the head element without removing it.
func (d *RDeque) PeekFirst(ctx context.Context) (any, error) {
	v, err := d.rc().LIndex(ctx, d.name, 0).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return d.c.codec.Decode(v)
}

// PeekLast returns the tail element without removing it.
func (d *RDeque) PeekLast(ctx context.Context) (any, error) {
	v, err := d.rc().LIndex(ctx, d.name, -1).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return d.c.codec.Decode(v)
}

// Size returns the number of elements.
func (d *RDeque) Size(ctx context.Context) (int64, error) {
	return d.rc().LLen(ctx, d.name).Result()
}

// Clear removes all elements.
func (d *RDeque) Clear(ctx context.Context) error {
	return d.Delete(ctx)
}

func (d *RDeque) encodeAll(values []any) ([]any, error) {
	enc := make([]any, len(values))
	for i, v := range values {
		s, err := d.c.codec.Encode(v)
		if err != nil {
			return nil, err
		}
		enc[i] = s
	}
	return enc, nil
}

// RBlockingQueue is an RQueue with blocking consumption (BLPOP).
type RBlockingQueue struct {
	*RQueue
}

func newRBlockingQueue(c *Client, name string) *RBlockingQueue {
	return &RBlockingQueue{RQueue: newRQueue(c, name)}
}

// Take blocks until an element is available or ctx is cancelled.
func (q *RBlockingQueue) Take(ctx context.Context) (any, error) {
	res, err := q.rc().BLPop(ctx, 0, q.name).Result()
	if err == redis.Nil {
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

// PollWithTimeout blocks up to timeout for an element.
// Sub-second timeouts are rounded up to 1s (BLPOP granularity).
func (q *RBlockingQueue) PollWithTimeout(ctx context.Context, timeout time.Duration) (any, error) {
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

// RBlockingDeque is an RDeque with blocking consumption from both ends.
type RBlockingDeque struct {
	*RDeque
}

func newRBlockingDeque(c *Client, name string) *RBlockingDeque {
	return &RBlockingDeque{RDeque: newRDeque(c, name)}
}

// TakeFirst blocks until an element is available at the head.
func (d *RBlockingDeque) TakeFirst(ctx context.Context) (any, error) {
	return blpopDecode(ctx, d.c, d.name)
}

// TakeLast blocks until an element is available at the tail.
func (d *RBlockingDeque) TakeLast(ctx context.Context) (any, error) {
	res, err := d.rc().BRPop(ctx, 0, d.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) < 2 {
		return nil, nil
	}
	return d.c.codec.Decode(res[1])
}

// PollFirstWithTimeout blocks up to timeout for a head element.
func (d *RBlockingDeque) PollFirstWithTimeout(ctx context.Context, timeout time.Duration) (any, error) {
	return blpopTimeoutDecode(ctx, d.c, d.name, timeout)
}

// PollLastWithTimeout blocks up to timeout for a tail element.
func (d *RBlockingDeque) PollLastWithTimeout(ctx context.Context, timeout time.Duration) (any, error) {
	secs := secondsAtLeast(timeout)
	res, err := d.rc().BRPop(ctx, time.Duration(secs)*time.Second, d.name).Result()
	if err == redis.Nil || err == context.DeadlineExceeded {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) < 2 {
		return nil, nil
	}
	return d.c.codec.Decode(res[1])
}

func blpopDecode(ctx context.Context, c *Client, name string) (any, error) {
	res, err := c.rc.BLPop(ctx, 0, name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) < 2 {
		return nil, nil
	}
	return c.codec.Decode(res[1])
}

func blpopTimeoutDecode(ctx context.Context, c *Client, name string, timeout time.Duration) (any, error) {
	secs := secondsAtLeast(timeout)
	res, err := c.rc.BLPop(ctx, time.Duration(secs)*time.Second, name).Result()
	if err == redis.Nil || err == context.DeadlineExceeded {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) < 2 {
		return nil, nil
	}
	return c.codec.Decode(res[1])
}

func secondsAtLeast(timeout time.Duration) int64 {
	secs := int64(timeout.Seconds())
	if timeout > 0 && secs == 0 {
		secs = 1
	}
	return secs
}
