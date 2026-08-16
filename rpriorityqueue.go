package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RPriorityQueue is a distributed priority queue backed by a ZSET: score
// decides priority (lower score = higher priority, polled first). Members
// are codec-encoded like every other structure.
type RPriorityQueue struct {
	rObject
}

func newRPriorityQueue(c *Client, name string) *RPriorityQueue {
	return &RPriorityQueue{rObject{c: c, name: name}}
}

// Offer adds an element with the given priority score.
func (q *RPriorityQueue) Offer(ctx context.Context, element any, score float64) error {
	enc, err := q.c.codec.Encode(element)
	if err != nil {
		return err
	}
	return q.rc().ZAdd(ctx, q.name, redis.Z{Score: score, Member: enc}).Err()
}

// Poll removes and returns the highest-priority element (lowest score).
// Returns (nil, nil) when empty.
func (q *RPriorityQueue) Poll(ctx context.Context) (any, error) {
	z, err := q.rc().ZPopMin(ctx, q.name, 1).Result()
	if err != nil || len(z) == 0 {
		return nil, err
	}
	member, _ := z[0].Member.(string)
	return q.c.codec.Decode(member)
}

// Peek returns the highest-priority element without removing it.
func (q *RPriorityQueue) Peek(ctx context.Context) (any, error) {
	vals, err := q.rc().ZRange(ctx, q.name, 0, 0).Result()
	if err != nil || len(vals) == 0 {
		return nil, err
	}
	return q.c.codec.Decode(vals[0])
}

// PeekScore returns the priority of an element (ok=false when absent).
func (q *RPriorityQueue) PeekScore(ctx context.Context, element any) (float64, bool, error) {
	enc, err := q.c.codec.Encode(element)
	if err != nil {
		return 0, false, err
	}
	score, err := q.rc().ZScore(ctx, q.name, enc).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return score, true, nil
}

// Remove deletes an element.
func (q *RPriorityQueue) Remove(ctx context.Context, element any) error {
	enc, err := q.c.codec.Encode(element)
	if err != nil {
		return err
	}
	return q.rc().ZRem(ctx, q.name, enc).Err()
}

// Size returns the number of queued elements.
func (q *RPriorityQueue) Size(ctx context.Context) (int64, error) {
	return q.rc().ZCard(ctx, q.name).Result()
}

// Clear removes the queue.
func (q *RPriorityQueue) Clear(ctx context.Context) error { return q.Delete(ctx) }
