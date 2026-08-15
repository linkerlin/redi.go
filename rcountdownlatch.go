package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RCountDownLatch is a distributed countdown latch backed by a Redis String
// counter, Redisson wire-compatible: channel redisson_countdownlatch__channel__{name}
// receives message 0 when the count reaches zero.
type RCountDownLatch struct {
	rObject
	channel string
}

func newRCountDownLatch(c *Client, name string) *RCountDownLatch {
	return &RCountDownLatch{
		rObject: rObject{c: c, name: name},
		channel: "redisson_countdownlatch__channel__{" + name + "}",
	}
}

var latchCountdownScript = redis.NewScript(`
local v = redis.call('decr', KEYS[1])
if v <= 0 then
    redis.call('del', KEYS[1])
end
if v == 0 then
    redis.call('publish', KEYS[2], ARGV[1])
end
return v
`)

var latchNewCountScript = redis.NewScript(`
if redis.call('exists', KEYS[1]) == 0 then
    redis.call('set', KEYS[1], ARGV[2])
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
else
    return 0
end
`)

// TrySetCount initializes the latch count only when it does not exist.
func (l *RCountDownLatch) TrySetCount(ctx context.Context, count int64) (bool, error) {
	n, err := latchNewCountScript.Run(ctx, l.rc(), []string{l.name, l.channel}, 1, count).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// CountDown decrements the count, deleting the key at zero and publishing
// the wake-up message.
func (l *RCountDownLatch) CountDown(ctx context.Context) error {
	_, err := latchCountdownScript.Run(ctx, l.rc(), []string{l.name, l.channel}, 0).Int()
	return err
}

// GetCount returns the current count (0 when the latch is open/absent).
func (l *RCountDownLatch) GetCount(ctx context.Context) (int64, error) {
	v, err := l.rc().Get(ctx, l.name).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n := parseInt64(v)
	if n < 0 {
		n = 0
	}
	return n, nil
}

// Await blocks until the count reaches zero. A non-positive timeout waits
// indefinitely. Returns false on timeout.
func (l *RCountDownLatch) Await(ctx context.Context, timeout time.Duration) (bool, error) {
	n, err := l.GetCount(ctx)
	if err != nil || n <= 0 {
		return true, err
	}

	var deadline <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		deadline = timer.C
	}

	sub := l.subscribe(ctx, l.channel)
	defer sub.Close()
	wake := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			n, err := l.GetCount(ctx)
			return n <= 0, err
		case <-deadline:
			n, err := l.GetCount(ctx)
			return n <= 0, err
		case <-wake:
		case <-time.After(200 * time.Millisecond):
		}
		n, err := l.GetCount(ctx)
		if err != nil {
			return false, err
		}
		if n <= 0 {
			return true, nil
		}
	}
}
