package redi

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// RScoredSortedSet is a distributed sorted set backed by a Redis ZSET.
// Members are codec (JSON) encoded; scores are float64.
type RScoredSortedSet struct {
	rObject
}

func newRScoredSortedSet(c *Client, name string) *RScoredSortedSet {
	return &RScoredSortedSet{rObject{c: c, name: name}}
}

// Add adds member with score. Returns true when newly added.
func (s *RScoredSortedSet) Add(ctx context.Context, member any, score float64) (bool, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return false, err
	}
	n, err := s.rc().ZAddNX(ctx, s.name, redis.Z{Score: score, Member: enc}).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// AddScore adds delta to member's score and returns the new score.
func (s *RScoredSortedSet) AddScore(ctx context.Context, member any, delta float64) (float64, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return 0, err
	}
	return s.rc().ZIncrBy(ctx, s.name, delta, enc).Result()
}

// Remove removes members.
func (s *RScoredSortedSet) Remove(ctx context.Context, members ...any) error {
	enc := make([]any, len(members))
	for i, m := range members {
		str, err := s.c.codec.Encode(m)
		if err != nil {
			return err
		}
		enc[i] = str
	}
	return s.rc().ZRem(ctx, s.name, enc...).Err()
}

// Score returns member's score. Returns (0, nil) when member is absent.
func (s *RScoredSortedSet) Score(ctx context.Context, member any) (float64, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return 0, err
	}
	v, err := s.rc().ZScore(ctx, s.name, enc).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return v, err
}

// Rank returns the 0-based ascending rank. Returns (-1, nil) when absent.
func (s *RScoredSortedSet) Rank(ctx context.Context, member any) (int64, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return 0, err
	}
	v, err := s.rc().ZRank(ctx, s.name, enc).Result()
	if err == redis.Nil {
		return -1, nil
	}
	return v, err
}

// Range returns members in ascending rank order within [start, stop].
func (s *RScoredSortedSet) Range(ctx context.Context, start, stop int64) ([]any, error) {
	vals, err := s.rc().ZRange(ctx, s.name, start, stop).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

// RangeWithScores returns member/score pairs within [start, stop].
func (s *RScoredSortedSet) RangeWithScores(ctx context.Context, start, stop int64) ([]redis.Z, error) {
	zs, err := s.rc().ZRangeWithScores(ctx, s.name, start, stop).Result()
	if err != nil {
		return nil, err
	}
	for i := range zs {
		if m, err := s.c.codec.Decode(fmtMember(zs[i].Member)); err == nil {
			zs[i].Member = m
		}
	}
	return zs, nil
}

// RangeByScore returns members with min <= score <= max, ascending.
func (s *RScoredSortedSet) RangeByScore(ctx context.Context, min, max float64) ([]any, error) {
	opt := &redis.ZRangeBy{Min: formatFloat(min), Max: formatFloat(max)}
	vals, err := s.rc().ZRangeByScore(ctx, s.name, opt).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

// Count returns the number of members with min <= score <= max.
func (s *RScoredSortedSet) Count(ctx context.Context, min, max float64) (int64, error) {
	return s.rc().ZCount(ctx, s.name, formatFloat(min), formatFloat(max)).Result()
}

// Size returns the number of members.
func (s *RScoredSortedSet) Size(ctx context.Context) (int64, error) {
	return s.rc().ZCard(ctx, s.name).Result()
}

// Clear removes all members.
func (s *RScoredSortedSet) Clear(ctx context.Context) error { return s.Delete(ctx) }

func (s *RScoredSortedSet) decodeAll(vals []string) ([]any, error) {
	out := make([]any, len(vals))
	for i, v := range vals {
		d, err := s.c.codec.Decode(v)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

func fmtMember(m any) string {
	if s, ok := m.(string); ok {
		return s
	}
	return ""
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
