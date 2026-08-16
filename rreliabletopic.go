package redi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RReliableTopic is a reliable pub/sub topic backed by a Redis Stream,
// wire-compatible with Redisson's RedissonReliableTopic (source-verified):
//
//	STREAM {name}            messages: XADD field "m" = codec-encoded payload
//	GROUP  {subscriberId}    ONE CONSUMER GROUP PER SUBSCRIBER (Java
//	                        semantics: every group receives every message —
//	                        unlike one-group-many-consumers, which would
//	                        load-balance instead of broadcasting)
//	ZSET   {name}:timeout    subscriber liveness (watchdog deadline, score=ts)
//
// Messages are acked only after the listener returns, so a crash
// mid-callback redelivers (reliable delivery).
type RReliableTopic struct {
	rObject
	timeoutKey string

	mu        sync.Mutex
	listeners map[string]func(msg any)
	stopped   map[string]bool
	wg        sync.WaitGroup
}

// reliableTopicWatchdog mirrors Redisson's reliableTopicWatchdogTimeout
// (default 7 min there; shortened here since we own both sides of the
// liveness check semantics — Java monitors only clean up long-dead
// subscribers, and Java's own watchdog refreshes its entries on its own
// schedule regardless of ours).
const reliableTopicWatchdog = 2 * time.Minute

func newRReliableTopic(c *Client, name string) *RReliableTopic {
	return &RReliableTopic{
		rObject:    rObject{c: c, name: name},
		timeoutKey: suffixName(name, "timeout"),
		listeners:  make(map[string]func(msg any)),
		stopped:    make(map[string]bool),
	}
}

// Publish appends a message for every subscriber group (XADD field "m").
// Returns the number of subscriber groups (Java semantics: XINFO GROUPS).
func (t *RReliableTopic) Publish(ctx context.Context, message any) (int64, error) {
	enc, err := t.c.codec.Encode(message)
	if err != nil {
		return 0, err
	}
	n, err := t.rc().XAdd(ctx, &redis.XAddArgs{
		Stream: t.name,
		Values: map[string]any{"m": enc},
	}).Result()
	if err != nil {
		return 0, err
	}
	_ = n
	groups, err := t.rc().XInfoGroups(ctx, t.name).Result()
	if err != nil {
		return 0, nil
	}
	return int64(len(groups)), nil
}

// Subscribe registers a listener that receives every message (its own
// consumer group, from the beginning of the stream like Java's
// StreamMessageId.ALL). Blocks until the group is created.
func (t *RReliableTopic) Subscribe(listener func(msg any)) (string, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return "", err
	}
	subscriberID := hex.EncodeToString(id)

	// Register liveness + create the group (idempotent; BUSYGROUP tolerated
	// for the cross-process case where the entry survived).
	now, err := t.serverNowMs(context.Background())
	if err != nil {
		return "", err
	}
	if err := t.rc().ZAdd(context.Background(), t.timeoutKey, redis.Z{
		Score:  float64(now + reliableTopicWatchdog.Milliseconds()),
		Member: subscriberID,
	}).Err(); err != nil {
		return "", err
	}
	if err := t.rc().XGroupCreateMkStream(context.Background(), t.name, subscriberID, "0").Err(); err != nil {
		if !isBusyGroup(err) {
			return "", err
		}
	}

	t.mu.Lock()
	t.listeners[subscriberID] = listener
	t.mu.Unlock()

	t.wg.Add(2)
	go t.consumeLoop(subscriberID)
	go t.renewLoop(subscriberID)
	return subscriberID, nil
}

// Unsubscribe stops a listener and removes its group and liveness entry.
func (t *RReliableTopic) Unsubscribe(id string) {
	t.mu.Lock()
	if _, ok := t.listeners[id]; ok {
		t.stopped[id] = true
		delete(t.listeners, id)
	}
	t.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = t.rc().XGroupDestroy(ctx, t.name, id).Err()
	_ = t.rc().ZRem(ctx, t.timeoutKey, id).Err()
}

func isBusyGroup(err error) bool {
	return err != nil && (err.Error() == "BUSYGROUP Consumer Group name already exists" ||
		contains(err.Error(), "BUSYGROUP"))
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func (t *RReliableTopic) isStopped(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped[id]
}

func (t *RReliableTopic) consumeLoop(id string) {
	defer t.wg.Done()
	for !t.isStopped(id) {
		res, err := t.rc().XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    id,
			Consumer: id,
			Streams:  []string{t.name, ">"},
			Count:    10,
			Block:    2 * time.Second,
		}).Result()
		if err != nil && err != redis.Nil {
			if t.isStopped(id) || t.c.ctx.Err() != nil {
				return
			}
			t.c.logf("reliable topic %q read: %v", t.name, err)
			time.Sleep(time.Second)
			continue
		}
		for _, sr := range res {
			for _, msg := range sr.Messages {
				t.mu.Lock()
				cb := t.listeners[id]
				t.mu.Unlock()
				if cb == nil {
					return
				}
				raw, _ := msg.Values["m"].(string)
				decoded, derr := t.c.codec.Decode(raw)
				if derr != nil {
					decoded = raw
				}
				cb(decoded)
				// Ack only after the callback returned (reliable delivery).
				_ = t.rc().XAck(context.Background(), t.name, id, msg.ID).Err()
			}
		}
	}
}

// renewLoop refreshes the subscriber liveness entry (Java's watchdog).
func (t *RReliableTopic) renewLoop(id string) {
	defer t.wg.Done()
	ticker := time.NewTicker(reliableTopicWatchdog / 3)
	defer ticker.Stop()
	for {
		select {
		case <-t.c.ctx.Done():
			return
		case <-ticker.C:
		}
		if t.isStopped(id) {
			return
		}
		now, err := t.serverNowMs(context.Background())
		if err != nil {
			continue
		}
		if err := t.rc().ZAdd(context.Background(), t.timeoutKey, redis.Z{
			Score:  float64(now + reliableTopicWatchdog.Milliseconds()),
			Member: id,
		}).Err(); err != nil {
			t.c.logf("reliable topic %q renew: %v", t.name, err)
		}
	}
}

// Size returns the number of messages in the stream.
func (t *RReliableTopic) Size(ctx context.Context) (int64, error) {
	return t.rc().XLen(ctx, t.name).Result()
}

// CountSubscribers returns the number of active subscriber groups.
func (t *RReliableTopic) CountSubscribers(ctx context.Context) (int64, error) {
	groups, err := t.rc().XInfoGroups(ctx, t.name).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}
	return int64(len(groups)), nil
}

// Delete stops all listeners and removes the topic, its timeout zset and
// every subscriber group.
func (t *RReliableTopic) Delete(ctx context.Context) error {
	t.mu.Lock()
	ids := make([]string, 0, len(t.listeners))
	for id := range t.listeners {
		ids = append(ids, id)
		t.stopped[id] = true
		delete(t.listeners, id)
	}
	t.mu.Unlock()
	for _, id := range ids {
		_ = t.rc().XGroupDestroy(ctx, t.name, id).Err()
	}
	return t.rc().Del(ctx, t.name, t.timeoutKey).Err()
}
