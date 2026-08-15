package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RLexSortedSet is a lexicographically sorted set. Members are stored RAW
// (not codec-encoded), matching Redisson's RLexSortedSet which bypasses the
// codec so lexicographic ordering uses the natural byte form (redi.py
// verified this Redisson special case).
type RLexSortedSet struct {
	rObject
}

func newRLexSortedSet(c *Client, name string) *RLexSortedSet {
	return &RLexSortedSet{rObject{c: c, name: name}}
}

// Add adds a raw string member (score 0).
func (s *RLexSortedSet) Add(ctx context.Context, element string) (bool, error) {
	n, err := s.rc().ZAdd(ctx, s.name, redis.Z{Score: 0, Member: element}).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Remove removes a raw member.
func (s *RLexSortedSet) Remove(ctx context.Context, element string) (bool, error) {
	n, err := s.rc().ZRem(ctx, s.name, element).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Contains reports membership.
func (s *RLexSortedSet) Contains(ctx context.Context, element string) (bool, error) {
	_, err := s.rc().ZScore(ctx, s.name, element).Result()
	if err == redis.Nil {
		return false, nil
	}
	return err == nil, err
}

// Size returns the member count.
func (s *RLexSortedSet) Size(ctx context.Context) (int64, error) {
	return s.rc().ZCard(ctx, s.name).Result()
}

// lexOpt builds a ZRangeBy with optional offset/count.
func lexOpt(min, max string, offset, count int64) *redis.ZRangeBy {
	by := &redis.ZRangeBy{Min: min, Max: max}
	if offset > 0 || count >= 0 {
		by.Offset = offset
		if count >= 0 {
			by.Count = count
		}
	}
	return by
}

// RangeByLex returns members in the inclusive [min, max] lex range.
func (s *RLexSortedSet) RangeByLex(ctx context.Context, min, max string, offset, count int64) ([]string, error) {
	return s.rc().ZRangeByLex(ctx, s.name, lexOpt("["+min, "["+max, offset, count)).Result()
}

// RangeHead returns members lexicographically before toValue (open range).
func (s *RLexSortedSet) RangeHead(ctx context.Context, toValue string, offset, count int64) ([]string, error) {
	return s.rc().ZRangeByLex(ctx, s.name, lexOpt("-", "("+toValue, offset, count)).Result()
}

// RangeTail returns members lexicographically after fromValue (open range).
func (s *RLexSortedSet) RangeTail(ctx context.Context, fromValue string, offset, count int64) ([]string, error) {
	return s.rc().ZRangeByLex(ctx, s.name, lexOpt("("+fromValue, "+", offset, count)).Result()
}

// CountHead counts members before toValue (open range).
func (s *RLexSortedSet) CountHead(ctx context.Context, toValue string) (int64, error) {
	return s.rc().ZLexCount(ctx, s.name, "-", "("+toValue).Result()
}

// CountTail counts members after fromValue (open range).
func (s *RLexSortedSet) CountTail(ctx context.Context, fromValue string) (int64, error) {
	return s.rc().ZLexCount(ctx, s.name, "("+fromValue, "+").Result()
}

// CountRange counts members in the inclusive [min, max] range.
func (s *RLexSortedSet) CountRange(ctx context.Context, min, max string) (int64, error) {
	return s.rc().ZLexCount(ctx, s.name, "["+min, "["+max).Result()
}

// RemoveRangeHead removes members before toValue (open range).
func (s *RLexSortedSet) RemoveRangeHead(ctx context.Context, toValue string) (int64, error) {
	return s.rc().ZRemRangeByLex(ctx, s.name, "-", "("+toValue).Result()
}

// RemoveRangeTail removes members after fromValue (open range).
func (s *RLexSortedSet) RemoveRangeTail(ctx context.Context, fromValue string) (int64, error) {
	return s.rc().ZRemRangeByLex(ctx, s.name, "("+fromValue, "+").Result()
}

// RemoveRangeByLex removes members in the inclusive [min, max] range.
func (s *RLexSortedSet) RemoveRangeByLex(ctx context.Context, min, max string) (int64, error) {
	return s.rc().ZRemRangeByLex(ctx, s.name, "["+min, "["+max).Result()
}

// First returns the smallest member ("" when empty).
func (s *RLexSortedSet) First(ctx context.Context) (string, error) {
	v, err := s.rc().ZRange(ctx, s.name, 0, 0).Result()
	if err != nil || len(v) == 0 {
		return "", err
	}
	return v[0], nil
}

// Last returns the largest member ("" when empty).
func (s *RLexSortedSet) Last(ctx context.Context) (string, error) {
	v, err := s.rc().ZRevRange(ctx, s.name, 0, 0).Result()
	if err != nil || len(v) == 0 {
		return "", err
	}
	return v[0], nil
}

// PollFirst removes and returns the smallest member ("" when empty).
func (s *RLexSortedSet) PollFirst(ctx context.Context) (string, error) {
	z, err := s.rc().ZPopMin(ctx, s.name, 1).Result()
	if err != nil || len(z) == 0 {
		return "", err
	}
	m, _ := z[0].Member.(string)
	return m, nil
}

// PollLast removes and returns the largest member ("" when empty).
func (s *RLexSortedSet) PollLast(ctx context.Context) (string, error) {
	z, err := s.rc().ZPopMax(ctx, s.name, 1).Result()
	if err != nil || len(z) == 0 {
		return "", err
	}
	m, _ := z[0].Member.(string)
	return m, nil
}

// Range returns members by rank [start, stop].
func (s *RLexSortedSet) Range(ctx context.Context, start, stop int64) ([]string, error) {
	return s.rc().ZRange(ctx, s.name, start, stop).Result()
}

// Clear removes the set.
func (s *RLexSortedSet) Clear(ctx context.Context) error { return s.Delete(ctx) }
