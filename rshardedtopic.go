package redi

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

// RShardedTopic is a cluster-friendly topic using Redis 7+ sharded
// pub/sub (SSUBSCRIBE / SSPUBLISH): messages stay on the slot's shard
// instead of broadcasting cluster-wide, so heavy topics do not saturate
// the bus. Messages are codec-encoded like RTopic; cross-language interop
// is automatic (Redis-native protocol).
type RShardedTopic struct {
	rObject

	mu        sync.Mutex
	listeners map[int]func(msg any)
	nextID    int
	sub       *redis.PubSub
	wg        sync.WaitGroup
}

func newRShardedTopic(c *Client, name string) *RShardedTopic {
	return &RShardedTopic{
		rObject:   rObject{c: c, name: name},
		listeners: make(map[int]func(msg any)),
	}
}

// Publish broadcasts via SSPUBLISH. Returns receivers count.
func (t *RShardedTopic) Publish(ctx context.Context, message any) (int64, error) {
	enc, err := t.c.codec.Encode(message)
	if err != nil {
		return 0, err
	}
	return t.rc().SPublish(ctx, t.name, enc).Result()
}

// Subscribe registers a listener and (lazily) starts the SSUBSCRIBE
// goroutine; blocks until the subscription is live.
func (t *RShardedTopic) Subscribe(listener func(msg any)) (int, error) {
	t.mu.Lock()
	if t.sub == nil {
		if err := t.startListening(); err != nil {
			t.mu.Unlock()
			return 0, err
		}
	}
	id := t.nextID
	t.nextID++
	t.listeners[id] = listener
	t.mu.Unlock()
	return id, nil
}

// Unsubscribe removes a listener, stopping the goroutine when empty.
func (t *RShardedTopic) Unsubscribe(id int) bool {
	t.mu.Lock()
	_, ok := t.listeners[id]
	delete(t.listeners, id)
	shouldStop := len(t.listeners) == 0 && t.sub != nil
	sub := t.sub
	if shouldStop {
		t.sub = nil
	}
	t.mu.Unlock()
	if shouldStop && sub != nil {
		_ = sub.Close() // unblocks the listener loop
		t.wg.Wait()
	}
	return ok
}

func (t *RShardedTopic) startListening() error {
	sub := t.c.rc.SSubscribe(t.c.ctx, t.name)
	ready := make(chan error, 1)
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		if _, err := sub.Receive(t.c.ctx); err != nil {
			ready <- err
			return
		}
		ready <- nil
		for {
			msg, err := sub.ReceiveMessage(t.c.ctx)
			if err != nil {
				return
			}
			decoded, derr := t.c.codec.Decode(msg.Payload)
			if derr != nil {
				decoded = msg.Payload
			}
			t.mu.Lock()
			callbacks := make([]func(any), 0, len(t.listeners))
			for _, cb := range t.listeners {
				callbacks = append(callbacks, cb)
			}
			t.mu.Unlock()
			for _, cb := range callbacks {
				cb(decoded)
			}
		}
	}()
	if err := <-ready; err != nil {
		return err
	}
	t.sub = sub
	return nil
}

// CountSubscribers returns the sharded subscriber count (PUBSUB SHARDNUMSUB).
func (t *RShardedTopic) CountSubscribers(ctx context.Context) (int64, error) {
	m, err := t.rc().PubSubShardNumSub(ctx, t.name).Result()
	if err != nil {
		return 0, err
	}
	return m[t.name], nil
}
