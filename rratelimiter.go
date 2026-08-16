package redi

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateType mirrors Redisson's RateType enum ordinals (wire format).
type RateType int

const (
	RateTypeOverall   RateType = 0
	RateTypePerClient RateType = 1
)

// RRateLimiter is a distributed sliding-window rate limiter, wire-compatible
// with Redisson 4.6.x:
//
//	HASH   {name}             config: rate, interval (ms), keepAliveTime, type
//	STRING {name}:value       remaining permits in the window
//	ZSET   {name}:permits     claims: score = claim ts(ms),
//	                          member = 0x10 + 16B client UUID + LE uint32 permits
//
// PER_CLIENT appends the client id to the value/permits keys.
type RRateLimiter struct {
	rObject
	cfgLoaded bool
	rate      int64
	interval  int64
	rtype     RateType
}

func newRRateLimiter(c *Client, name string) *RRateLimiter {
	return &RRateLimiter{rObject: rObject{c: c, name: name}}
}

// rateAcquireScript mirrors Redisson 4.6.1's RedissonRateLimiter Lua:
// members are struct.pack('Bc0I', len(id), id, permits) and expired ones
// are returned to the pool via struct.unpack. (The manual byte-math variant
// ported from redi.py read the permits tail big-endian while writing it
// little-endian — every expired permit released 16777216 instead of 1.)
var rateAcquireScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local rate = tonumber(ARGV[3])
local permits = tonumber(ARGV[5])

local expired = redis.call('zrangebyscore', KEYS[2], 0, now - interval)
local returned = 0
for i = 1, #expired do
    local rnd, p = struct.unpack('Bc0I', expired[i])
    returned = returned + p
end
if returned > 0 then
    redis.call('zremrangebyscore', KEYS[2], 0, now - interval)
end

local value = redis.call('get', KEYS[1])
if value == false then
    value = rate
else
    value = tonumber(value) + returned
end

if value >= permits then
    local member = struct.pack('Bc0I', string.len(ARGV[4]), ARGV[4], permits)
    redis.call('zadd', KEYS[2], now, member)
    redis.call('set', KEYS[1], value - permits)
    redis.call('pexpire', KEYS[2], interval * 2)
    return 1
end
redis.call('set', KEYS[1], value)
return 0
`)

func (r *RRateLimiter) valueKey() string {
	if r.rtype == RateTypePerClient {
		return suffixName(r.name, "value") + ":" + r.c.id
	}
	return suffixName(r.name, "value")
}

func (r *RRateLimiter) permitsKey() string {
	if r.rtype == RateTypePerClient {
		return suffixName(r.name, "permits") + ":" + r.c.id
	}
	return suffixName(r.name, "permits")
}

func (r *RRateLimiter) ensureConfig(ctx context.Context) error {
	if r.cfgLoaded {
		return nil
	}
	defer func() { r.cfgLoaded = true }()
	m, err := r.rc().HGetAll(ctx, r.name).Result()
	if err != nil {
		return err
	}
	if len(m) > 0 {
		r.rate = parseInt64(m["rate"])
		r.interval = parseInt64(m["interval"])
		if t, ok := m["type"]; ok {
			r.rtype = RateType(parseInt64(t))
		}
	}
	return nil
}

// TrySetRate sets the rate config only when not yet configured.
func (r *RRateLimiter) TrySetRate(ctx context.Context, rateType RateType, rate int64, interval time.Duration) (bool, error) {
	if err := r.ensureConfig(ctx); err != nil {
		return false, err
	}
	if r.rate > 0 || r.interval > 0 {
		return false, nil
	}
	return true, r.SetRate(ctx, rateType, rate, interval)
}

// SetRate (over)writes the persisted rate config.
func (r *RRateLimiter) SetRate(ctx context.Context, rateType RateType, rate int64, interval time.Duration) error {
	intervalMs := interval.Milliseconds()
	if _, err := r.rc().HSet(ctx, r.name, map[string]any{
		"rate":          rate,
		"interval":      intervalMs,
		"keepAliveTime": 0,
		"type":          int(rateType),
	}).Result(); err != nil {
		return err
	}
	r.rate, r.interval, r.rtype, r.cfgLoaded = rate, intervalMs, rateType, true
	return nil
}

// TryAcquire takes permits when the window allows it.
func (r *RRateLimiter) TryAcquire(ctx context.Context, permits int64) (bool, error) {
	if err := r.ensureConfig(ctx); err != nil {
		return false, err
	}
	if r.rate <= 0 {
		return true, nil
	}
	now, err := r.serverNowMs(ctx)
	if err != nil {
		return false, err
	}
	// Fresh random id per acquire (Java's generateIdArray() = 16 bytes): a
	// fixed id would make struct.pack produce identical members and ZADD
	// would collapse repeated acquires into one zset entry.
	acquireID := make([]byte, 16)
	if _, err := rand.Read(acquireID); err != nil {
		return false, err
	}
	n, err := rateAcquireScript.Run(ctx, r.rc(),
		[]string{r.valueKey(), r.permitsKey()},
		now, r.interval, r.rate, string(acquireID), permits).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Acquire blocks until permits are granted or ctx is cancelled.
func (r *RRateLimiter) Acquire(ctx context.Context, permits int64) error {
	for {
		ok, err := r.TryAcquire(ctx, permits)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// ratePurgeScript returns expired permits to the pool (shared by the
// acquire path and AvailablePermits so reads never shrink the pool).
var ratePurgeScript = redis.NewScript(`
local expired = redis.call('zrangebyscore', KEYS[2], 0, tonumber(ARGV[1]) - tonumber(ARGV[2]))
local returned = 0
for i = 1, #expired do
    local rnd, p = struct.unpack('Bc0I', expired[i])
    returned = returned + p
end
if returned > 0 then
    redis.call('zremrangebyscore', KEYS[2], 0, tonumber(ARGV[1]) - tonumber(ARGV[2]))
    local value = redis.call('get', KEYS[1])
    if value == false then
        value = 0
    end
    redis.call('set', KEYS[1], tonumber(value) + returned)
end
return returned
`)

// AvailablePermits returns the permits currently available in the window.
func (r *RRateLimiter) AvailablePermits(ctx context.Context) (int64, error) {
	if err := r.ensureConfig(ctx); err != nil {
		return 0, err
	}
	if r.rate <= 0 {
		return 0, nil
	}
	now, err := r.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	// Purge WITH return-to-pool: a bare ZRemRangeByScore would shrink the
	// pool every time it ran.
	if _, err := ratePurgeScript.Run(ctx, r.rc(),
		[]string{r.valueKey(), r.permitsKey()}, now, r.interval).Int(); err != nil {
		return 0, err
	}
	v, err := r.rc().Get(ctx, r.valueKey()).Result()
	if err == redis.Nil {
		return r.rate, nil
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

// Delete removes config, value and permits keys.
func (r *RRateLimiter) Delete(ctx context.Context) error {
	return r.rc().Del(ctx, r.name, r.valueKey(), r.permitsKey()).Err()
}
