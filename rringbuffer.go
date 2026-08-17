package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RRingBuffer is a capacity-bounded FIFO that evicts the oldest element
// on overflow, wire-compatible with Redisson's RedissonRingBuffer:
//
//	LIST {name}                    elements (RPUSH tail, LPOP on overflow)
//	STRING redisson_rb:{name}      capacity (plain decimal, SETNX)
type RRingBuffer struct {
	rObject
	settingsName string
}

func newRRingBuffer(c *Client, name string) *RRingBuffer {
	return &RRingBuffer{
		rObject:      rObject{c: c, name: name},
		settingsName: prefixName("redisson_rb", name),
	}
}

var ringAddScript = redis.NewScript(`
local limit = redis.call('get', KEYS[2]);
assert(limit ~= false, 'RingBuffer capacity is not defined');
local size = redis.call('rpush', KEYS[1], ARGV[1]);
if size > tonumber(limit) then
    redis.call('lpop', KEYS[1]);
end;
return 1;
`)

var ringAddAllScript = redis.NewScript(`
local limit = redis.call('get', KEYS[2]);
assert(limit ~= false, 'RingBuffer capacity is not defined');
local size = 0;
for i = 1, #ARGV, 5000 do
    size = redis.call('rpush', KEYS[1], unpack(ARGV, i, math.min(i+4999, #ARGV)));
end;
local extraSize = size - tonumber(limit);
if extraSize > 0 then
    redis.call('ltrim', KEYS[1], extraSize, -1);
end;
return 1;
`)

var ringRemainingScript = redis.NewScript(`
local limit = redis.call('get', KEYS[2]);
assert(limit ~= false, 'RingBuffer capacity is not defined');
local size = redis.call('llen', KEYS[1]);
return math.max(tonumber(limit) - size, 0);
`)

// TrySetCapacity initializes the capacity only when undefined.
func (r *RRingBuffer) TrySetCapacity(ctx context.Context, capacity int64) (bool, error) {
	return r.rc().SetNX(ctx, r.settingsName, capacity, 0).Result()
}

// SetCapacity (re)sets the capacity, trimming overflow immediately.
var ringSetCapacityScript = redis.NewScript(`
redis.call('set', KEYS[2], ARGV[1]);
local len = redis.call('llen', KEYS[1]);
if len > tonumber(ARGV[1]) then
    redis.call('ltrim', KEYS[1], len - tonumber(ARGV[1]), -1);
end;
`)

func (r *RRingBuffer) SetCapacity(ctx context.Context, capacity int64) error {
	err := ringSetCapacityScript.Run(ctx, r.rc(),
		[]string{r.name, r.settingsName}, capacity).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}

// Capacity returns the configured capacity (0 when undefined).
func (r *RRingBuffer) Capacity(ctx context.Context) (int64, error) {
	v, err := r.rc().Get(ctx, r.settingsName).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return parseInt64(v), nil
}

// Add appends an element, evicting the oldest on overflow.
func (r *RRingBuffer) Add(ctx context.Context, value any) error {
	enc, err := r.c.codec.Encode(value)
	if err != nil {
		return err
	}
	_, err = ringAddScript.Run(ctx, r.rc(),
		[]string{r.name, r.settingsName}, enc).Int()
	return err
}

// Offer is the queue-style alias of Add.
func (r *RRingBuffer) Offer(ctx context.Context, value any) error {
	return r.Add(ctx, value)
}

// AddAll appends multiple elements, trimming to capacity in one pass.
func (r *RRingBuffer) AddAll(ctx context.Context, values ...any) error {
	if len(values) == 0 {
		return nil
	}
	args := make([]any, len(values))
	for i, v := range values {
		enc, err := r.c.codec.Encode(v)
		if err != nil {
			return err
		}
		args[i] = enc
	}
	_, err := ringAddAllScript.Run(ctx, r.rc(),
		[]string{r.name, r.settingsName}, args...).Int()
	return err
}

// RemainingCapacity returns capacity minus current size (>= 0).
func (r *RRingBuffer) RemainingCapacity(ctx context.Context) (int64, error) {
	return ringRemainingScript.Run(ctx, r.rc(),
		[]string{r.name, r.settingsName}).Int64()
}

// Size returns the current number of elements.
func (r *RRingBuffer) Size(ctx context.Context) (int64, error) {
	return r.rc().LLen(ctx, r.name).Result()
}

// Poll removes and returns the oldest element, or (nil, nil) when empty.
func (r *RRingBuffer) Poll(ctx context.Context) (any, error) {
	v, err := r.rc().LPop(ctx, r.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return r.c.codec.Decode(v)
}

// Peek returns the oldest element without removing it.
func (r *RRingBuffer) Peek(ctx context.Context) (any, error) {
	v, err := r.rc().LIndex(ctx, r.name, 0).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return r.c.codec.Decode(v)
}

// ReadAll returns all elements from oldest to newest.
func (r *RRingBuffer) ReadAll(ctx context.Context) ([]any, error) {
	vals, err := r.rc().LRange(ctx, r.name, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return r.decodeAll(vals)
}

// ReadOldest returns up to count elements from the head (oldest first).
func (r *RRingBuffer) ReadOldest(ctx context.Context, count int64) ([]any, error) {
	vals, err := r.rc().LRange(ctx, r.name, 0, count-1).Result()
	if err != nil {
		return nil, err
	}
	return r.decodeAll(vals)
}

// ReadNewest returns up to count elements from the tail (newest last,
// same relative order as stored — matching Java's range(-count, -1)).
func (r *RRingBuffer) ReadNewest(ctx context.Context, count int64) ([]any, error) {
	vals, err := r.rc().LRange(ctx, r.name, -count, -1).Result()
	if err != nil {
		return nil, err
	}
	return r.decodeAll(vals)
}

// Clear removes queued elements while preserving the configured capacity.
func (r *RRingBuffer) Clear(ctx context.Context) error {
	return r.rc().Del(ctx, r.name).Err()
}

// Delete removes the buffer and its settings key.
func (r *RRingBuffer) Delete(ctx context.Context) error {
	return r.rc().Del(ctx, r.name, r.settingsName).Err()
}

func (r *RRingBuffer) decodeAll(vals []string) ([]any, error) {
	out := make([]any, len(vals))
	for i, v := range vals {
		d, err := r.c.codec.Decode(v)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}
