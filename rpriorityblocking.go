package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RPriorityBlockingQueue is a blocking wrapper over RPriorityQueue (ZSET +
// BZPOPMIN). Same compatibility caveat as RPriorityQueue: NOT Java
// Comparator-based RPriorityBlockingQueue.
type RPriorityBlockingQueue struct {
	*RPriorityQueue
}

func newRPriorityBlockingQueue(c *Client, name string) *RPriorityBlockingQueue {
	return &RPriorityBlockingQueue{RPriorityQueue: newRPriorityQueue(c, name)}
}

// Take blocks until an element is available (BZPOPMIN timeout 0) or ctx ends.
func (q *RPriorityBlockingQueue) Take(ctx context.Context) (any, error) {
	return q.popMin(ctx, 0)
}

// PollWithTimeout blocks up to timeout for the lowest-score element.
// Returns (nil, nil) on timeout.
func (q *RPriorityBlockingQueue) PollWithTimeout(ctx context.Context, timeout time.Duration) (any, error) {
	if timeout < 0 {
		return nil, nil
	}
	return q.popMin(ctx, timeout)
}

func (q *RPriorityBlockingQueue) popMin(ctx context.Context, timeout time.Duration) (any, error) {
	z, err := q.rc().BZPopMin(ctx, timeout, q.name).Result()
	if err == redis.Nil || err == context.DeadlineExceeded {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	member, _ := z.Member.(string)
	return q.c.codec.Decode(member)
}

// RPriorityBlockingDeque adds highest-score (tail) blocking pops via BZPOPMAX.
// Still ZSET+score semantics — not Java Comparator RPriorityBlockingDeque.
type RPriorityBlockingDeque struct {
	*RPriorityBlockingQueue
}

func newRPriorityBlockingDeque(c *Client, name string) *RPriorityBlockingDeque {
	return &RPriorityBlockingDeque{
		RPriorityBlockingQueue: newRPriorityBlockingQueue(c, name),
	}
}

// TakeLast blocks for the highest-score element (BZPOPMAX).
func (q *RPriorityBlockingDeque) TakeLast(ctx context.Context) (any, error) {
	return q.popMax(ctx, 0)
}

// PollLastWithTimeout blocks up to timeout for the highest-score element.
func (q *RPriorityBlockingDeque) PollLastWithTimeout(ctx context.Context, timeout time.Duration) (any, error) {
	if timeout < 0 {
		return nil, nil
	}
	return q.popMax(ctx, timeout)
}

func (q *RPriorityBlockingDeque) popMax(ctx context.Context, timeout time.Duration) (any, error) {
	z, err := q.rc().BZPopMax(ctx, timeout, q.name).Result()
	if err == redis.Nil || err == context.DeadlineExceeded {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	member, _ := z.Member.(string)
	return q.c.codec.Decode(member)
}

// RPriorityDeque is a non-blocking double-ended view of the same ZSET priority
// queue (ZPOPMIN / ZPOPMAX). Not Java Comparator RPriorityDeque.
type RPriorityDeque struct {
	*RPriorityQueue
}

func newRPriorityDeque(c *Client, name string) *RPriorityDeque {
	return &RPriorityDeque{RPriorityQueue: newRPriorityQueue(c, name)}
}

// PollFirst removes and returns the lowest-score element (alias of Poll).
func (q *RPriorityDeque) PollFirst(ctx context.Context) (any, error) {
	return q.Poll(ctx)
}

// PollLast removes and returns the highest-score element.
func (q *RPriorityDeque) PollLast(ctx context.Context) (any, error) {
	z, err := q.rc().ZPopMax(ctx, q.name, 1).Result()
	if err != nil || len(z) == 0 {
		return nil, err
	}
	member, _ := z[0].Member.(string)
	return q.c.codec.Decode(member)
}

// PeekFirst returns the lowest-score element without removing it (alias of Peek).
func (q *RPriorityDeque) PeekFirst(ctx context.Context) (any, error) {
	return q.Peek(ctx)
}

// PeekLast returns the highest-score element without removing it.
func (q *RPriorityDeque) PeekLast(ctx context.Context) (any, error) {
	vals, err := q.rc().ZRevRange(ctx, q.name, 0, 0).Result()
	if err != nil || len(vals) == 0 {
		return nil, err
	}
	return q.c.codec.Decode(vals[0])
}
