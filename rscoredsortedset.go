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

var scoredAddAndRankScript = redis.NewScript(`
redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
return redis.call('zrank', KEYS[1], ARGV[2])
`)

var scoredIncrAndRankScript = redis.NewScript(`
redis.call('zincrby', KEYS[1], ARGV[1], ARGV[2])
return redis.call('zrank', KEYS[1], ARGV[2])
`)

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

// AddIfAbsent adds member only when it isn't present.
func (s *RScoredSortedSet) AddIfAbsent(ctx context.Context, member any, score float64) (bool, error) {
	return s.Add(ctx, member, score)
}

// TryAdd is the Redisson-style alias of AddIfAbsent.
func (s *RScoredSortedSet) TryAdd(ctx context.Context, member any, score float64) (bool, error) {
	return s.AddIfAbsent(ctx, member, score)
}

// AddIfExists updates an existing member and reports whether its score changed.
func (s *RScoredSortedSet) AddIfExists(ctx context.Context, member any, score float64) (bool, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return false, err
	}
	n, err := s.rc().ZAddArgs(ctx, s.name, redis.ZAddArgs{
		XX:      true,
		Ch:      true,
		Members: []redis.Z{{Score: score, Member: enc}},
	}).Result()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// AddIfGreater adds a missing member or updates it only when score is greater.
func (s *RScoredSortedSet) AddIfGreater(
	ctx context.Context, member any, score float64,
) (bool, error) {
	return s.addIfScore(ctx, member, score, true)
}

// AddIfLess adds a missing member or updates it only when score is lower.
func (s *RScoredSortedSet) AddIfLess(
	ctx context.Context, member any, score float64,
) (bool, error) {
	return s.addIfScore(ctx, member, score, false)
}

func (s *RScoredSortedSet) addIfScore(
	ctx context.Context, member any, score float64, greater bool,
) (bool, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return false, err
	}
	args := redis.ZAddArgs{
		Ch:      true,
		Members: []redis.Z{{Score: score, Member: enc}},
	}
	if greater {
		args.GT = true
	} else {
		args.LT = true
	}
	n, err := s.rc().ZAddArgs(ctx, s.name, args).Result()
	return n == 1, err
}

// AddAllIfAbsent adds only members that aren't already present.
func (s *RScoredSortedSet) AddAllIfAbsent(
	ctx context.Context, entries map[any]float64,
) (int64, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	members := make([]redis.Z, 0, len(entries))
	for member, score := range entries {
		enc, err := s.c.codec.Encode(member)
		if err != nil {
			return 0, err
		}
		members = append(members, redis.Z{Score: score, Member: enc})
	}
	return s.rc().ZAddArgs(ctx, s.name, redis.ZAddArgs{
		NX:      true,
		Members: members,
	}).Result()
}

// AddAndGetRank sets score and returns the member's ascending rank.
func (s *RScoredSortedSet) AddAndGetRank(
	ctx context.Context, member any, score float64,
) (int64, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return 0, err
	}
	return scoredAddAndRankScript.Run(ctx, s.rc(), []string{s.name},
		formatFloat(score), enc).Int64()
}

// AddScore adds delta to member's score and returns the new score.
func (s *RScoredSortedSet) AddScore(ctx context.Context, member any, delta float64) (float64, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return 0, err
	}
	return s.rc().ZIncrBy(ctx, s.name, delta, enc).Result()
}

// AddScoreAndGetRank increments score and returns the new ascending rank.
func (s *RScoredSortedSet) AddScoreAndGetRank(
	ctx context.Context, member any, delta float64,
) (int64, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return 0, err
	}
	return scoredIncrAndRankScript.Run(ctx, s.rc(), []string{s.name},
		formatFloat(delta), enc).Int64()
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

// Contains reports whether member is present.
func (s *RScoredSortedSet) Contains(ctx context.Context, member any) (bool, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return false, err
	}
	_, err = s.rc().ZScore(ctx, s.name, enc).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

// RevRank returns the 0-based descending rank. Returns (-1, nil) when absent.
func (s *RScoredSortedSet) RevRank(ctx context.Context, member any) (int64, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return 0, err
	}
	v, err := s.rc().ZRevRank(ctx, s.name, enc).Result()
	if err == redis.Nil {
		return -1, nil
	}
	return v, err
}

// First returns the lowest-score member without removing it.
func (s *RScoredSortedSet) First(ctx context.Context) (any, error) {
	vals, err := s.rc().ZRange(ctx, s.name, 0, 0).Result()
	if err != nil || len(vals) == 0 {
		return nil, err
	}
	return s.c.codec.Decode(vals[0])
}

// Last returns the highest-score member without removing it.
func (s *RScoredSortedSet) Last(ctx context.Context) (any, error) {
	vals, err := s.rc().ZRevRange(ctx, s.name, 0, 0).Result()
	if err != nil || len(vals) == 0 {
		return nil, err
	}
	return s.c.codec.Decode(vals[0])
}

// FirstEntry returns the lowest-score member and score, or nil when empty.
func (s *RScoredSortedSet) FirstEntry(ctx context.Context) (*redis.Z, error) {
	entries, err := s.EntryRange(ctx, 0, 0)
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	return &entries[0], nil
}

// LastEntry returns the highest-score member and score, or nil when empty.
func (s *RScoredSortedSet) LastEntry(ctx context.Context) (*redis.Z, error) {
	entries, err := s.EntryRange(ctx, -1, -1)
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	return &entries[0], nil
}

// FirstScore returns the lowest score, or 0 when empty.
func (s *RScoredSortedSet) FirstScore(ctx context.Context) (float64, error) {
	values, err := s.rc().ZRangeWithScores(ctx, s.name, 0, 0).Result()
	if err != nil || len(values) == 0 {
		return 0, err
	}
	return values[0].Score, nil
}

// LastScore returns the highest score, or 0 when empty.
func (s *RScoredSortedSet) LastScore(ctx context.Context) (float64, error) {
	values, err := s.rc().ZRevRangeWithScores(ctx, s.name, 0, 0).Result()
	if err != nil || len(values) == 0 {
		return 0, err
	}
	return values[0].Score, nil
}

// Random returns one random member, or nil when empty.
func (s *RScoredSortedSet) Random(ctx context.Context) (any, error) {
	values, err := s.rc().ZRandMember(ctx, s.name, 1).Result()
	if err != nil || len(values) == 0 {
		return nil, err
	}
	return s.c.codec.Decode(values[0])
}

// RandomN returns random members using Redis ZRANDMEMBER count semantics.
func (s *RScoredSortedSet) RandomN(ctx context.Context, count int64) ([]any, error) {
	if count == 0 {
		return []any{}, nil
	}
	values, err := s.rc().ZRandMember(ctx, s.name, int(count)).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(values)
}

// PollFirst removes and returns the lowest-score member.
func (s *RScoredSortedSet) PollFirst(ctx context.Context) (any, error) {
	z, err := s.rc().ZPopMin(ctx, s.name, 1).Result()
	if err != nil || len(z) == 0 {
		return nil, err
	}
	return s.c.codec.Decode(fmtMember(z[0].Member))
}

// PollLast removes and returns the highest-score member.
func (s *RScoredSortedSet) PollLast(ctx context.Context) (any, error) {
	z, err := s.rc().ZPopMax(ctx, s.name, 1).Result()
	if err != nil || len(z) == 0 {
		return nil, err
	}
	return s.c.codec.Decode(fmtMember(z[0].Member))
}

// PollFirstEntries removes and returns up to count lowest-score entries.
func (s *RScoredSortedSet) PollFirstEntries(
	ctx context.Context, count int64,
) ([]redis.Z, error) {
	if count <= 0 {
		return []redis.Z{}, nil
	}
	entries, err := s.rc().ZPopMin(ctx, s.name, count).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeScored(entries)
}

// PollLastEntries removes and returns up to count highest-score entries in
// ascending score order, matching Redisson's zrange(-count, -1) result.
func (s *RScoredSortedSet) PollLastEntries(
	ctx context.Context, count int64,
) ([]redis.Z, error) {
	if count <= 0 {
		return []redis.Z{}, nil
	}
	entries, err := s.rc().ZPopMax(ctx, s.name, count).Result()
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	return s.decodeScored(entries)
}

// RemoveRangeByRank removes members with ranks in [start, stop].
func (s *RScoredSortedSet) RemoveRangeByRank(ctx context.Context, start, stop int64) (int64, error) {
	return s.rc().ZRemRangeByRank(ctx, s.name, start, stop).Result()
}

// RemoveRangeByScore removes members with min <= score <= max.
func (s *RScoredSortedSet) RemoveRangeByScore(ctx context.Context, min, max float64) (int64, error) {
	return s.rc().ZRemRangeByScore(ctx, s.name, formatFloat(min), formatFloat(max)).Result()
}

// RangeReversed returns members in descending rank order within [start, stop].
func (s *RScoredSortedSet) RangeReversed(ctx context.Context, start, stop int64) ([]any, error) {
	vals, err := s.rc().ZRevRange(ctx, s.name, start, stop).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
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
	return s.decodeScored(zs)
}

// EntryRange returns member/score pairs by ascending rank.
func (s *RScoredSortedSet) EntryRange(
	ctx context.Context, start, stop int64,
) ([]redis.Z, error) {
	return s.RangeWithScores(ctx, start, stop)
}

func (s *RScoredSortedSet) decodeScored(zs []redis.Z) ([]redis.Z, error) {
	for i := range zs {
		member, err := s.c.codec.Decode(fmtMember(zs[i].Member))
		if err != nil {
			return nil, err
		}
		zs[i].Member = member
	}
	return zs, nil
}

// Intersection returns members present in this sorted set and every named
// sorted set. Redis sums scores when determining the result ordering.
func (s *RScoredSortedSet) Intersection(ctx context.Context, otherNames ...string) ([]any, error) {
	keys := append([]string{s.name}, otherNames...)
	values, err := s.rc().ZInter(ctx, &redis.ZStore{Keys: keys}).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(values)
}

// Union returns members present in this sorted set or any named sorted set.
// Redis sums scores for members present in multiple sets.
func (s *RScoredSortedSet) Union(ctx context.Context, otherNames ...string) ([]any, error) {
	keys := append([]string{s.name}, otherNames...)
	values, err := s.rc().ZUnion(ctx, redis.ZStore{Keys: keys}).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(values)
}

// Diff returns members present in this sorted set but absent from all named
// sorted sets.
func (s *RScoredSortedSet) Diff(ctx context.Context, otherNames ...string) ([]any, error) {
	keys := append([]string{s.name}, otherNames...)
	values, err := s.rc().ZDiff(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(values)
}

// IntersectionStore stores the intersection in dest and returns its size.
func (s *RScoredSortedSet) IntersectionStore(
	ctx context.Context, dest string, otherNames ...string,
) (int64, error) {
	keys := append([]string{s.name}, otherNames...)
	return s.rc().ZInterStore(ctx, dest, &redis.ZStore{Keys: keys}).Result()
}

// UnionStore stores the union in dest and returns its size.
func (s *RScoredSortedSet) UnionStore(
	ctx context.Context, dest string, otherNames ...string,
) (int64, error) {
	keys := append([]string{s.name}, otherNames...)
	return s.rc().ZUnionStore(ctx, dest, &redis.ZStore{Keys: keys}).Result()
}

// DiffStore stores the difference in dest and returns its size.
func (s *RScoredSortedSet) DiffStore(
	ctx context.Context, dest string, otherNames ...string,
) (int64, error) {
	keys := append([]string{s.name}, otherNames...)
	return s.rc().ZDiffStore(ctx, dest, keys...).Result()
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
