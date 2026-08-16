package redi

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// TimeSeriesEntry is one stored point (Id is the sequence number Java
// assigns per entry; zero when not read back).
type TimeSeriesEntry struct {
	Timestamp int64
	Value     any
	Label     string
	Id        int64
}

// RTimeSeries is a time-series store, wire-compatible with Redisson's
// RedissonTimeSeries (source-verified):
//
//	ZSET {name}                        entries: score = timestamp ms,
//	                                   member = struct.pack('BBc0Lc0Lc0',
//	                                     4, idLen, id, valLen, encoded,
//	                                     lblLen, label)
//	ZSET redisson__ts_ttl:{name}       expiry deadlines (score = epoch ms)
//	STRING redisson__ts_seq:{name}     entry-id counter (zero-padded to
//	                                   20 digits, compared as strings)
//
// The label blob = byte(n) + label where n=2 means "no label" and n=3
// means "with label" (Java's convention).
type RTimeSeries struct {
	rObject
	timeoutSetName string
	sequenceName   string
}

func newRTimeSeries(c *Client, name string) *RTimeSeries {
	return &RTimeSeries{
		rObject:        rObject{c: c, name: name},
		timeoutSetName: prefixName("redisson__ts_ttl", name),
		sequenceName:   prefixName("redisson__ts_seq", name),
	}
}

// Java: default when no TTL = now + 100 years.
func hundredYearsFrom(now int64) int64 {
	return now + int64(365*100)*24*time.Hour.Milliseconds()
}

// tsAddScript (with TTL) uses the FIXED deadline as the timeout-set score
// (Java's addAllAsync TTL variant - no +1).
var tsAddTTLScript = redis.NewScript(`
local sequenceWidth = tonumber(ARGV[1]);
local previous;
local function nextId()
    if previous == nil then
        previous = redis.call('get', KEYS[3]);
        if previous == false then
            previous = '0';
        end;
        previous = string.rep('0', sequenceWidth - string.len(previous)) .. previous;
    end;
    previous = previous + 1;
    redis.call('set', KEYS[3], previous);
    return previous;
end;
for i = 3, #ARGV, 4 do
    local id = nextId();
    local lbl = string.char(ARGV[i+1]) .. ARGV[i+3];
    local val = struct.pack('BBc0Lc0Lc0', 4, string.len(id), id,
                            string.len(ARGV[i+2]), ARGV[i+2],
                            string.len(lbl), lbl);
    redis.call('zadd', KEYS[1], ARGV[i], val);
    redis.call('zadd', KEYS[2], ARGV[2], val);
end;
`)

// tsAddScript (no TTL) extends the deadline to now + 100 years, capped by
// any existing later deadline (Java's default variant, score = deadline+1).
var tsAddScript = redis.NewScript(`
local sequenceWidth = tonumber(ARGV[1]);
local previous;
local function nextId()
    if previous == nil then
        previous = redis.call('get', KEYS[3]);
        if previous == false then
            previous = '0';
        end;
        previous = string.rep('0', sequenceWidth - string.len(previous)) .. previous;
    end;
    previous = previous + 1;
    redis.call('set', KEYS[3], previous);
    return previous;
end;
local expirationTime = ARGV[2];
local lastValues = redis.call('zrange', KEYS[2], -1, -1, 'withscores');
if (#lastValues > 0 and tonumber(lastValues[2]) > tonumber(ARGV[2])) then
    expirationTime = tonumber(lastValues[2]);
end;
for i = 3, #ARGV, 4 do
    local id = nextId();
    local lbl = string.char(ARGV[i+1]) .. ARGV[i+3];
    local val = struct.pack('BBc0Lc0Lc0', 4, string.len(id), id,
                            string.len(ARGV[i+2]), ARGV[i+2],
                            string.len(lbl), lbl);
    redis.call('zadd', KEYS[1], ARGV[i], val);
    redis.call('zadd', KEYS[2], expirationTime + 1, val);
end;
`)

// Add stores value at timestamp (ms). ttl <= 0 means the Java default
// (effectively 100 years). label == "" stores an unlabeled entry.
func (s *RTimeSeries) Add(ctx context.Context, timestampMs int64, value any, label string, ttl time.Duration) error {
	enc, err := s.c.codec.Encode(value)
	if err != nil {
		return err
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return err
	}
	lblMark := byte(2)
	lblPayload := ""
	if label != "" {
		lblMark = 3
		lblPayload = label
	}
	script := tsAddScript
	expiration := hundredYearsFrom(now)
	if ttl > 0 {
		script = tsAddTTLScript
		expiration = now + ttl.Milliseconds()
	}
	err = script.Run(ctx, s.rc(),
		[]string{s.name, s.timeoutSetName, s.sequenceName},
		20, expiration, timestampMs, lblMark, enc, lblPayload).Err()
	if err == redis.Nil {
		return nil // EVAL_VOID
	}
	return err
}

// tsGetScript is Java's getEntryAsync: first unexpired entry at the exact
// timestamp; returns {mark, ts, val, label} or nil. NOTE the unpack's
// first return is the OUTER type byte (4); the label mark is the first
// byte of the label blob (2 = unlabeled, 3 = labeled) - Java handles this
// via DECODE_LABEL, we slice it in Lua.
var tsGetScript = redis.NewScript(`
local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[2]);
for i = 1, #values do
    local expirationDate = redis.call('zscore', KEYS[2], values[i]);
    if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then
        local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]);
        local mark = string.byte(label, 1);
        if mark == 2 then
            return {mark, ARGV[2], val, ''};
        end;
        return {mark, ARGV[2], val, string.sub(label, 2)};
    end;
end;
return nil;
`)

// Get returns the (unexpired) entry stored at exactly timestampMs
// (nil when absent).
func (s *RTimeSeries) Get(ctx context.Context, timestampMs int64) (*TimeSeriesEntry, error) {
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return nil, err
	}
	res, err := tsGetScript.Run(ctx, s.rc(),
		[]string{s.name, s.timeoutSetName}, now, timestampMs).Slice()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) < 3 {
		return nil, nil
	}
	val, _ := res[2].(string)
	decoded, derr := s.c.codec.Decode(val)
	if derr != nil {
		decoded = val
	}
	label, _ := res[3].(string)
	return &TimeSeriesEntry{Timestamp: timestampMs, Value: decoded, Label: label}, nil
}

// tsRangeScript mirrors Java's entryRangeAsync: unexpired entries with
// startTimestamp <= score <= endTimestamp, ascending, up to limit. The
// label blob's first byte is the mark (2/3); labeled payloads drop it.
var tsRangeScript = redis.NewScript(`
local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[3], 'limit', 0, tonumber(ARGV[4]));
local out = {};
for i = 1, #values do
    local expirationDate = redis.call('zscore', KEYS[2], values[i]);
    if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then
        local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]);
        local ts = redis.call('zscore', KEYS[1], values[i]);
        local mark = string.byte(label, 1);
        if mark == 2 then
            table.insert(out, ts); table.insert(out, val); table.insert(out, '');
        else
            table.insert(out, ts); table.insert(out, val); table.insert(out, string.sub(label, 2));
        end
    end;
end;
return out;
`)

// Range returns unexpired entries in [fromMs, toMs] ascending (limit <= 0
// means unlimited; the effective limit is capped at 1000 like Java's
// default batch).
func (s *RTimeSeries) Range(ctx context.Context, fromMs, toMs int64, limit int64) ([]TimeSeriesEntry, error) {
	if limit <= 0 {
		limit = 1000
	}
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return nil, err
	}
	res, err := tsRangeScript.Run(ctx, s.rc(),
		[]string{s.name, s.timeoutSetName}, now, fromMs, toMs, limit).Slice()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]TimeSeriesEntry, 0, len(res)/3)
	for i := 0; i+2 < len(res); i += 3 {
		ts, _ := res[i].(string)
		val, _ := res[i+1].(string)
		label, _ := res[i+2].(string)
		decoded, derr := s.c.codec.Decode(val)
		if derr != nil {
			decoded = val
		}
		out = append(out, TimeSeriesEntry{
			Timestamp: parseInt64(ts),
			Value:     decoded,
			Label:     label,
		})
	}
	return out, nil
}

// tsSizeScript is Java's sizeAsync: ZCARD minus expired (lazy, no
// deletion).
var tsSizeScript = redis.NewScript(`
local values = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1]);
return redis.call('zcard', KEYS[1]) - #values;
`)

// Size returns the number of unexpired entries (Java's lazy semantics:
// ZCARD minus expired, without deleting them).
func (s *RTimeSeries) Size(ctx context.Context) (int64, error) {
	now, err := s.serverNowMs(ctx)
	if err != nil {
		return 0, err
	}
	return tsSizeScript.Run(ctx, s.rc(),
		[]string{s.name, s.timeoutSetName}, now).Int64()
}

// Remove deletes the entries stored at exactly timestampMs.
func (s *RTimeSeries) Remove(ctx context.Context, timestampMs int64) (int64, error) {
	members, err := s.rc().ZRangeByScore(ctx, s.name, &redis.ZRangeBy{
		Min: strconv.FormatInt(timestampMs, 10),
		Max: strconv.FormatInt(timestampMs, 10),
	}).Result()
	if err != nil || len(members) == 0 {
		return 0, err
	}
	if err := s.rc().ZRem(ctx, s.name, membersToAny(members)...).Err(); err != nil {
		return 0, err
	}
	if err := s.rc().ZRem(ctx, s.timeoutSetName, membersToAny(members)...).Err(); err != nil {
		return 0, err
	}
	return int64(len(members)), nil
}

// Delete removes the series and its companion keys.
func (s *RTimeSeries) Delete(ctx context.Context) error {
	return s.rc().Del(ctx, s.name, s.timeoutSetName, s.sequenceName).Err()
}

func membersToAny(ms []string) []any {
	out := make([]any, len(ms))
	for i, m := range ms {
		out[i] = m
	}
	return out
}
