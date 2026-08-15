package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RBatch queues structure operations into a single pipeline; Execute flushes
// them all in one round-trip (the classic ~10x latency win over sequential
// commands).
//
// The structures returned by the Get* factories below are bound to the
// pipeline: write methods (Put/Add/Offer/Set/…) queue commands and return
// immediately. Read methods return zero values until Execute has run — use
// a normal client structure to read results back (Redisson has the same
// constraint: batch reads need its Async API).
type RBatch struct {
	c    *Client
	pipe redis.Pipeliner
}

// NewBatch returns a batch bound to a fresh pipeline.
func (c *Client) NewBatch() *RBatch {
	return &RBatch{c: c, pipe: c.rc.Pipeline()}
}

// Execute flushes every queued command in one round-trip.
func (b *RBatch) Execute(ctx context.Context) error {
	_, err := b.pipe.Exec(ctx)
	return err
}

// Discard drops the queued commands without executing them.
func (b *RBatch) Discard() { b.pipe.Discard() }

// Len returns the number of queued commands.
func (b *RBatch) Len() int { return b.pipe.Len() }

func (b *RBatch) bind(o *rObject) *rObject {
	o.cmds = b.pipe
	return o
}

// GetMap returns a pipeline-bound map.
func (b *RBatch) GetMap(name string) *RMap { return &RMap{*b.bind(&rObject{c: b.c, name: name})} }

// GetBucket returns a pipeline-bound bucket.
func (b *RBatch) GetBucket(name string) *RBucket {
	return &RBucket{*b.bind(&rObject{c: b.c, name: name})}
}

// GetList returns a pipeline-bound list.
func (b *RBatch) GetList(name string) *RList { return &RList{*b.bind(&rObject{c: b.c, name: name})} }

// GetSet returns a pipeline-bound set.
func (b *RBatch) GetSet(name string) *RSet { return &RSet{*b.bind(&rObject{c: b.c, name: name})} }

// GetQueue returns a pipeline-bound queue.
func (b *RBatch) GetQueue(name string) *RQueue {
	return &RQueue{*b.bind(&rObject{c: b.c, name: name})}
}

// GetDeque returns a pipeline-bound deque.
func (b *RBatch) GetDeque(name string) *RDeque {
	return &RDeque{*b.bind(&rObject{c: b.c, name: name})}
}

// GetAtomicLong returns a pipeline-bound counter.
func (b *RBatch) GetAtomicLong(name string) *RAtomicLong {
	return &RAtomicLong{*b.bind(&rObject{c: b.c, name: name})}
}

// GetAtomicDouble returns a pipeline-bound double counter.
func (b *RBatch) GetAtomicDouble(name string) *RAtomicDouble {
	return &RAtomicDouble{*b.bind(&rObject{c: b.c, name: name})}
}

// GetScoredSortedSet returns a pipeline-bound sorted set.
func (b *RBatch) GetScoredSortedSet(name string) *RScoredSortedSet {
	return &RScoredSortedSet{*b.bind(&rObject{c: b.c, name: name})}
}
