package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RTransferQueue atomically moves elements from its head to the tail of a
// destination queue (single Lua round-trip, no TOCTOU window).
type RTransferQueue struct {
	*RQueue
}

func newRTransferQueue(c *Client, name string) *RTransferQueue {
	return &RTransferQueue{RQueue: newRQueue(c, name)}
}

var transferScript = redis.NewScript(`
local val = redis.call('lpop', KEYS[1])
if val == false then
    return nil
end
redis.call('rpush', KEYS[2], val)
return val
`)

// Transfer pops the head of this queue and pushes it onto the destination
// queue's tail, returning the transferred value (nil when this queue is
// empty).
func (q *RTransferQueue) Transfer(ctx context.Context, destination string) (any, error) {
	res, err := transferScript.Run(ctx, q.rc(), []string{q.name, destination}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s, _ := res.(string)
	return q.c.codec.Decode(s)
}

// TryTransfer transfers with a blocking wait for up to timeout, polling
// every 50ms. Returns the transferred value (nil on timeout).
func (q *RTransferQueue) TryTransfer(ctx context.Context, destination string, timeout time.Duration) (any, error) {
	if v, err := q.Transfer(ctx, destination); err != nil || v != nil {
		return v, err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		if v, err := q.Transfer(ctx, destination); err != nil || v != nil {
			return v, err
		}
	}
	return nil, nil
}
