package redi

import (
	"context"
	"time"

	"github.com/linkerlin/gotrycatch"
	"github.com/redis/go-redis/v9"
)

// RQueue is a distributed FIFO queue backed by a Redis List.
// Offer appends to the tail (RPUSH) and Poll removes from the head (LPOP).
type RQueue struct {
	rc   *redis.Client
	name string
	key  string
}

func newRQueue(rc *redis.Client, name string) *RQueue {
	return &RQueue{rc: rc, name: name, key: "redi:queue:" + name}
}

// Offer adds values to the tail of the queue.
func (q *RQueue) Offer(ctx context.Context, values ...any) error {
	var offerErr error
	tb := gotrycatch.Try(func() {
		if err := q.rc.RPush(ctx, q.key, values...).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { offerErr = err })
	tb.Finally(func() {})
	return offerErr
}

// Poll removes and returns the head element.
// Returns ("", redis.Nil) when the queue is empty.
func (q *RQueue) Poll(ctx context.Context) (string, error) {
	var val string
	var pollErr error
	tb := gotrycatch.Try(func() {
		v, err := q.rc.LPop(ctx, q.key).Result()
		if err != nil {
			panic(err)
		}
		val = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { pollErr = err })
	tb.Finally(func() {})
	return val, pollErr
}

// Peek returns the head element without removing it.
// Returns ("", redis.Nil) when the queue is empty.
func (q *RQueue) Peek(ctx context.Context) (string, error) {
	var val string
	var peekErr error
	tb := gotrycatch.Try(func() {
		v, err := q.rc.LIndex(ctx, q.key, 0).Result()
		if err != nil {
			panic(err)
		}
		val = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { peekErr = err })
	tb.Finally(func() {})
	return val, peekErr
}

// Take blocks until an element is available and then removes and returns it.
// The timeout parameter specifies the maximum time to block; 0 means block
// indefinitely.
func (q *RQueue) Take(ctx context.Context, timeout time.Duration) (string, error) {
	var val string
	var takeErr error
	tb := gotrycatch.Try(func() {
		result, err := q.rc.BLPop(ctx, timeout, q.key).Result()
		if err != nil {
			panic(err)
		}
		// BLPop returns [key, value].
		if len(result) < 2 {
			panic(redis.Nil)
		}
		val = result[1]
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { takeErr = err })
	tb.Finally(func() {})
	return val, takeErr
}

// Size returns the number of elements in the queue.
func (q *RQueue) Size(ctx context.Context) (int64, error) {
	var sz int64
	var szErr error
	tb := gotrycatch.Try(func() {
		v, err := q.rc.LLen(ctx, q.key).Result()
		if err != nil {
			panic(err)
		}
		sz = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { szErr = err })
	tb.Finally(func() {})
	return sz, szErr
}

// Clear removes all elements from the queue.
func (q *RQueue) Clear(ctx context.Context) error {
	var clearErr error
	tb := gotrycatch.Try(func() {
		if err := q.rc.Del(ctx, q.key).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { clearErr = err })
	tb.Finally(func() {})
	return clearErr
}
