package redi

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// StreamEntry is one stream record with codec-decoded fields.
type StreamEntry struct {
	ID     string
	Fields map[string]any
}

// RStream is a distributed log backed by a Redis Stream. Field names and
// values are codec-encoded (matching Redisson's RStream with
// JsonJacksonCodec — verified via redi.py's bidirectional tests); consumer
// groups are Redis-native so cross-language interop is automatic.
type RStream struct {
	rObject
}

func newRStream(c *Client, name string) *RStream {
	return &RStream{rObject{c: c, name: name}}
}

func (s *RStream) encodeFields(fields map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(fields))
	for k, v := range fields {
		ek, err := s.c.codec.Encode(k)
		if err != nil {
			return nil, err
		}
		ev, err := s.c.codec.Encode(v)
		if err != nil {
			return nil, err
		}
		out[ek] = ev
	}
	return out, nil
}

func (s *RStream) decodeFields(fields map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(fields))
	for k, v := range fields {
		dk, err := s.c.codec.Decode(k)
		if err != nil {
			return nil, err
		}
		str, ok := v.(string)
		if !ok {
			out[toString(dk)] = v
			continue
		}
		dv, err := s.c.codec.Decode(str)
		if err != nil {
			return nil, err
		}
		out[toString(dk)] = dv
	}
	return out, nil
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// Add appends an entry, returning its stream id ("*" auto id).
func (s *RStream) Add(ctx context.Context, fields map[string]any) (string, error) {
	return s.AddWithMaxLen(ctx, fields, 0, false)
}

// AddWithMaxLen appends with optional trimming (maxLen > 0).
func (s *RStream) AddWithMaxLen(ctx context.Context, fields map[string]any, maxLen int64, approximate bool) (string, error) {
	enc, err := s.encodeFields(fields)
	if err != nil {
		return "", err
	}
	args := &redis.XAddArgs{Stream: s.name, Values: enc}
	if maxLen > 0 {
		args.MaxLen = maxLen
		args.Approx = approximate
	}
	return s.rc().XAdd(ctx, args).Result()
}

// ReadRange returns entries with id in [start, end] ("-" .. "+"),
// ascending, up to count (0 = unlimited).
func (s *RStream) ReadRange(ctx context.Context, start, end string, count int64) ([]StreamEntry, error) {
	var msgs []redis.XMessage
	var err error
	if count > 0 {
		msgs, err = s.rc().XRangeN(ctx, s.name, start, end, count).Result()
	} else {
		msgs, err = s.rc().XRange(ctx, s.name, start, end).Result()
	}
	if err != nil {
		return nil, err
	}
	return s.toEntries(msgs)
}

// ReadReverse returns entries in descending id order.
func (s *RStream) ReadReverse(ctx context.Context, end, start string, count int64) ([]StreamEntry, error) {
	var msgs []redis.XMessage
	var err error
	if count > 0 {
		msgs, err = s.rc().XRevRangeN(ctx, s.name, end, start, count).Result()
	} else {
		msgs, err = s.rc().XRevRange(ctx, s.name, end, start).Result()
	}
	if err != nil {
		return nil, err
	}
	return s.toEntries(msgs)
}

// Len returns the number of entries (XLEN).
func (s *RStream) Len(ctx context.Context) (int64, error) {
	return s.rc().XLen(ctx, s.name).Result()
}

// Trim removes old entries keeping at most maxLen (XTRIM MAXLEN).
func (s *RStream) Trim(ctx context.Context, maxLen int64, approximate bool) (int64, error) {
	if approximate {
		return s.rc().XTrimMaxLenApprox(ctx, s.name, maxLen, 0).Result()
	}
	return s.rc().XTrimMaxLen(ctx, s.name, maxLen).Result()
}

// TrimMinID removes entries with ids older than minID (XTRIM MINID).
func (s *RStream) TrimMinID(ctx context.Context, minID string, approximate bool) (int64, error) {
	if approximate {
		return s.rc().XTrimMinIDApprox(ctx, s.name, minID, 0).Result()
	}
	return s.rc().XTrimMinID(ctx, s.name, minID).Result()
}

// Remove deletes specific entries by id.
func (s *RStream) Remove(ctx context.Context, ids ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return s.rc().XDel(ctx, s.name, ids...).Result()
}

// CreateGroup creates a consumer group starting at id ("$" = only new
// entries, "0" = whole history). Returns false when the group exists.
func (s *RStream) CreateGroup(ctx context.Context, group, id string) (bool, error) {
	err := s.rc().XGroupCreateMkStream(ctx, s.name, group, id).Err()
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// DeleteGroup removes a consumer group.
func (s *RStream) DeleteGroup(ctx context.Context, group string) (bool, error) {
	n, err := s.rc().XGroupDestroy(ctx, s.name, group).Result()
	return n > 0, err
}

// CreateConsumer registers a consumer in a group.
func (s *RStream) CreateConsumer(ctx context.Context, group, consumer string) (bool, error) {
	n, err := s.rc().XGroupCreateConsumer(ctx, s.name, group, consumer).Result()
	return n > 0, err
}

// RemoveConsumer removes a consumer and its pending entries.
func (s *RStream) RemoveConsumer(ctx context.Context, group, consumer string) (int64, error) {
	return s.rc().XGroupDelConsumer(ctx, s.name, group, consumer).Result()
}

// ReadGroup delivers undelivered (">") entries to a group consumer.
// block > 0 waits up to that duration; block <= 0 returns immediately
// (go-redis emits BLOCK for any Block >= 0, and "block 0" means block
// forever — hence the -1 sentinels).
func (s *RStream) ReadGroup(ctx context.Context, group, consumer string, count int64, block time.Duration) ([]StreamEntry, error) {
	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{s.name, ">"},
		Count:    count,
		Block:    -1,
	}
	if block > 0 {
		args.Block = block
	}
	res, err := s.rc().XReadGroup(ctx, args).Result()
	if err == redis.Nil {
		return nil, nil // no new entries within block
	}
	if err != nil {
		return nil, err
	}
	for _, sr := range res {
		if sr.Stream == s.name {
			return s.toEntries(sr.Messages)
		}
	}
	return nil, nil
}

// Ack acknowledges entries previously delivered to the group.
func (s *RStream) Ack(ctx context.Context, group string, ids ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return s.rc().XAck(ctx, s.name, group, ids...).Result()
}

// PendingEntry describes an entry delivered but not yet acknowledged.
type PendingEntry struct {
	ID         string
	Consumer   string
	IdleMs     int64
	Deliveries int64
}

// PendingRange lists unacknowledged entries (optionally for one consumer).
func (s *RStream) PendingRange(ctx context.Context, group, consumer string, count int64) ([]PendingEntry, error) {
	args := &redis.XPendingExtArgs{
		Stream: s.name,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  count,
	}
	if consumer != "" {
		args.Consumer = consumer
	}
	vals, err := s.rc().XPendingExt(ctx, args).Result()
	if err != nil {
		return nil, err
	}
	out := make([]PendingEntry, 0, len(vals))
	for _, v := range vals {
		out = append(out, PendingEntry{
			ID:         v.ID,
			Consumer:   v.Consumer,
			IdleMs:     v.Idle.Milliseconds(),
			Deliveries: v.RetryCount,
		})
	}
	return out, nil
}

// Claim atomically transfers ownership of ids to consumer (minIdle filters).
func (s *RStream) Claim(ctx context.Context, group, consumer string, minIdle time.Duration, ids ...string) ([]StreamEntry, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	vals, err := s.rc().XClaim(ctx, &redis.XClaimArgs{
		Stream:   s.name,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		return nil, err
	}
	return s.toEntries(vals)
}

// AutoClaim claims up to count entries idle for at least minIdle, starting
// the scan at start ("0-0"). Returns the entries and the next cursor.
func (s *RStream) AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, start string, count int64) ([]StreamEntry, string, error) {
	vals, cursor, err := s.rc().XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s.name,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()
	if err != nil {
		return nil, "", err
	}
	entries, err := s.toEntries(vals)
	return entries, cursor, err
}

func (s *RStream) toEntries(msgs []redis.XMessage) ([]StreamEntry, error) {
	out := make([]StreamEntry, 0, len(msgs))
	for _, m := range msgs {
		fields, err := s.decodeFields(m.Values)
		if err != nil {
			return nil, err
		}
		out = append(out, StreamEntry{ID: m.ID, Fields: fields})
	}
	return out, nil
}
