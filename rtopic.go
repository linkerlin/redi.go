package redi

import (
	"context"
	"sync"
)

// RTopic is a pub/sub topic whose messages are codec (JSON) encoded,
// interoperable with Redisson's RTopic. Listeners run on a dedicated
// subscription goroutine; Publish never fans out locally (exactly-once
// delivery per listener).
type RTopic struct {
	rObject

	mu        sync.Mutex
	listeners map[int]func(msg any)
	nextID    int
	stop      func()
}

func newRTopic(c *Client, name string) *RTopic {
	return &RTopic{
		rObject:   rObject{c: c, name: name},
		listeners: make(map[int]func(msg any)),
	}
}

// Publish broadcasts message to all subscribers (local and remote).
// Returns the number of clients that received it.
func (t *RTopic) Publish(ctx context.Context, message any) (int64, error) {
	enc, err := t.c.codec.Encode(message)
	if err != nil {
		return 0, err
	}
	return t.rc().Publish(ctx, t.name, enc).Result()
}

// Subscribe registers a listener and (lazily) starts the subscription
// goroutine. It blocks until the subscription is confirmed by Redis.
func (t *RTopic) Subscribe(listener func(msg any)) (int, error) {
	t.mu.Lock()
	if len(t.listeners) == 0 {
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

// Unsubscribe removes a listener, stopping the subscription goroutine when
// the last one goes away.
func (t *RTopic) Unsubscribe(id int) bool {
	t.mu.Lock()
	_, ok := t.listeners[id]
	delete(t.listeners, id)
	shouldStop := len(t.listeners) == 0
	stop := t.stop
	if shouldStop {
		t.stop = nil
	}
	t.mu.Unlock()
	if shouldStop && stop != nil {
		stop()
	}
	return ok
}

// CountListeners returns the number of local listeners.
func (t *RTopic) CountListeners() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.listeners)
}

// RemoveAllListeners unregisters every local listener.
func (t *RTopic) RemoveAllListeners() {
	t.mu.Lock()
	t.listeners = make(map[int]func(msg any))
	stop := t.stop
	t.stop = nil
	t.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// ChannelNames returns the Redis channels used by this topic.
func (t *RTopic) ChannelNames() []string {
	return []string{t.name}
}

// GetChannelNames is the Redisson-style alias of ChannelNames.
func (t *RTopic) GetChannelNames() []string {
	return t.ChannelNames()
}

// CountSubscribers returns the number of remote subscribers (PUBSUB NUMSUB).
func (t *RTopic) CountSubscribers(ctx context.Context) (int64, error) {
	m, err := t.rc().PubSubNumSub(ctx, t.name).Result()
	if err != nil {
		return 0, err
	}
	return m[t.name], nil
}

func (t *RTopic) startListening() error {
	sub := t.subscribe(context.Background(), t.name)
	// Wait for the subscription ack so Publish right after Subscribe is seen.
	ctx, cancel := context.WithTimeout(context.Background(), t.c.cfg.DialTimeout)
	defer cancel()
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return err
	}

	ctx2, stop := context.WithCancel(t.c.ctx)
	t.stop = stop
	go func() {
		defer stop()
		defer sub.Close() //nolint:errcheck // connection teardown //nolint:errcheck
		for {
			msg, err := sub.ReceiveMessage(ctx2)
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
	return nil
}

// RPatternTopic subscribes to a glob pattern (PSUBSCRIBE).
type RPatternTopic struct {
	rObject

	mu        sync.Mutex
	listeners map[int]func(channel string, msg any)
	nextID    int
	stop      func()
}

func newRPatternTopic(c *Client, pattern string) *RPatternTopic {
	return &RPatternTopic{
		rObject:   rObject{c: c, name: pattern},
		listeners: make(map[int]func(channel string, msg any)),
	}
}

// Subscribe registers a pattern listener, blocking until confirmed.
func (t *RPatternTopic) Subscribe(listener func(channel string, msg any)) (int, error) {
	t.mu.Lock()
	if len(t.listeners) == 0 {
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

// Unsubscribe removes a pattern listener.
func (t *RPatternTopic) Unsubscribe(id int) bool {
	t.mu.Lock()
	_, ok := t.listeners[id]
	delete(t.listeners, id)
	shouldStop := len(t.listeners) == 0
	stop := t.stop
	if shouldStop {
		t.stop = nil
	}
	t.mu.Unlock()
	if ok && shouldStop && stop != nil {
		stop()
	}
	return ok
}

// CountListeners returns the number of local pattern listeners.
func (t *RPatternTopic) CountListeners() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.listeners)
}

// RemoveAllListeners unregisters every local pattern listener.
func (t *RPatternTopic) RemoveAllListeners() {
	t.mu.Lock()
	t.listeners = make(map[int]func(channel string, msg any))
	stop := t.stop
	t.stop = nil
	t.mu.Unlock()
	if stop != nil {
		stop()
	}
}

func (t *RPatternTopic) startListening() error {
	sub := t.psubscribe(context.Background(), t.name)
	ctx, cancel := context.WithTimeout(context.Background(), t.c.cfg.DialTimeout)
	defer cancel()
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return err
	}

	ctx2, stop := context.WithCancel(t.c.ctx)
	t.stop = stop
	go func() {
		defer stop()
		defer sub.Close() //nolint:errcheck // connection teardown //nolint:errcheck
		for {
			msg, err := sub.ReceiveMessage(ctx2)
			if err != nil {
				return
			}
			decoded, derr := t.c.codec.Decode(msg.Payload)
			if derr != nil {
				decoded = msg.Payload
			}
			t.mu.Lock()
			callbacks := make([]func(string, any), 0, len(t.listeners))
			for _, cb := range t.listeners {
				callbacks = append(callbacks, cb)
			}
			t.mu.Unlock()
			for _, cb := range callbacks {
				cb(msg.Channel, decoded)
			}
		}
	}()
	return nil
}
