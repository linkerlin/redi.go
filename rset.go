package redi

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RSet is a distributed set backed by a Redis Set.
// Members are codec (JSON) encoded, Redisson-compatible.
type RSet struct {
	rObject
}

func newRSet(c *Client, name string) *RSet {
	return &RSet{rObject{c: c, name: name}}
}

var setTryAddScript = redis.NewScript(`
for i = 1, #ARGV do
    if redis.call('sismember', KEYS[1], ARGV[i]) == 1 then
        return 0
    end
end
redis.call('sadd', KEYS[1], unpack(ARGV))
return 1
`)

// Add adds members to the set.
func (s *RSet) Add(ctx context.Context, members ...any) error {
	_, err := s.AddCounted(ctx, members...)
	return err
}

// AddCounted adds members and returns how many were newly inserted
// (Redisson addAllCounted / SADD).
func (s *RSet) AddCounted(ctx context.Context, members ...any) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	enc, err := s.encodeAll(members)
	if err != nil {
		return 0, err
	}
	return s.rc().SAdd(ctx, s.name, enc...).Result()
}

// TryAdd atomically adds all members only when none is already present.
func (s *RSet) TryAdd(ctx context.Context, members ...any) (bool, error) {
	if len(members) == 0 {
		return true, nil
	}
	encoded, err := s.encodeAll(members)
	if err != nil {
		return false, err
	}
	n, err := setTryAddScript.Run(ctx, s.rc(), []string{s.name}, encoded...).Int()
	return n == 1, err
}

// Remove removes members from the set.
func (s *RSet) Remove(ctx context.Context, members ...any) error {
	_, err := s.RemoveCounted(ctx, members...)
	return err
}

// RemoveCounted removes members and returns how many were actually removed
// (Redisson removeAllCounted / SREM).
func (s *RSet) RemoveCounted(ctx context.Context, members ...any) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	enc, err := s.encodeAll(members)
	if err != nil {
		return 0, err
	}
	return s.rc().SRem(ctx, s.name, enc...).Result()
}

// Contains reports whether member is in the set.
func (s *RSet) Contains(ctx context.Context, member any) (bool, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return false, err
	}
	return s.rc().SIsMember(ctx, s.name, enc).Result()
}

// ContainsEach returns the subset of members that are in the set (SMISMEMBER).
func (s *RSet) ContainsEach(ctx context.Context, members ...any) ([]any, error) {
	if len(members) == 0 {
		return []any{}, nil
	}
	enc, err := s.encodeAll(members)
	if err != nil {
		return nil, err
	}
	flags, err := s.rc().SMIsMember(ctx, s.name, enc...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(members))
	for i, ok := range flags {
		if ok {
			out = append(out, members[i])
		}
	}
	return out, nil
}

// Members returns all members in the set (decoded).
func (s *RSet) Members(ctx context.Context) ([]any, error) {
	vals, err := s.rc().SMembers(ctx, s.name).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

// ReadAll is the Redisson-style alias of Members.
func (s *RSet) ReadAll(ctx context.Context) ([]any, error) {
	return s.Members(ctx)
}

// Union returns members present in this set or any named set.
func (s *RSet) Union(ctx context.Context, otherNames ...string) ([]any, error) {
	keys := append([]string{s.name}, otherNames...)
	vals, err := s.rc().SUnion(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

// Diff returns members present in this set but absent from all named sets.
func (s *RSet) Diff(ctx context.Context, otherNames ...string) ([]any, error) {
	keys := append([]string{s.name}, otherNames...)
	vals, err := s.rc().SDiff(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

// Intersection returns members present in this set and every named set.
func (s *RSet) Intersection(ctx context.Context, otherNames ...string) ([]any, error) {
	keys := append([]string{s.name}, otherNames...)
	vals, err := s.rc().SInter(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

// CountIntersection returns the intersection cardinality without materializing
// the members.
func (s *RSet) CountIntersection(
	ctx context.Context, otherNames ...string,
) (int64, error) {
	keys := append([]string{s.name}, otherNames...)
	return s.rc().SInterCard(ctx, 0, keys...).Result()
}

// UnionStore stores the union in dest and returns its cardinality.
func (s *RSet) UnionStore(ctx context.Context, dest string, otherNames ...string) (int64, error) {
	keys := append([]string{s.name}, otherNames...)
	return s.rc().SUnionStore(ctx, dest, keys...).Result()
}

// DiffStore stores the difference in dest and returns its cardinality.
func (s *RSet) DiffStore(ctx context.Context, dest string, otherNames ...string) (int64, error) {
	keys := append([]string{s.name}, otherNames...)
	return s.rc().SDiffStore(ctx, dest, keys...).Result()
}

// IntersectionStore stores the intersection in dest and returns its cardinality.
func (s *RSet) IntersectionStore(ctx context.Context, dest string, otherNames ...string) (int64, error) {
	keys := append([]string{s.name}, otherNames...)
	return s.rc().SInterStore(ctx, dest, keys...).Result()
}

// Random returns one random member, or (nil, nil) when the set is empty.
func (s *RSet) Random(ctx context.Context) (any, error) {
	v, err := s.rc().SRandMember(ctx, s.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.c.codec.Decode(v)
}

// RandomN returns up to n random members (may contain duplicates when n
// exceeds cardinality — Redis SRANDMEMBER semantics).
func (s *RSet) RandomN(ctx context.Context, n int64) ([]any, error) {
	if n <= 0 {
		return nil, nil
	}
	vals, err := s.rc().SRandMemberN(ctx, s.name, n).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

// RemoveRandom pops and returns one random member, or (nil, nil) when empty.
func (s *RSet) RemoveRandom(ctx context.Context) (any, error) {
	v, err := s.rc().SPop(ctx, s.name).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.c.codec.Decode(v)
}

// RemoveRandomN pops up to n random members.
func (s *RSet) RemoveRandomN(ctx context.Context, n int64) ([]any, error) {
	if n <= 0 {
		return nil, nil
	}
	vals, err := s.rc().SPopN(ctx, s.name, n).Result()
	if err != nil {
		return nil, err
	}
	return s.decodeAll(vals)
}

// Move moves member from this set to destination (another RSet name).
// Returns true when the member was moved.
func (s *RSet) Move(ctx context.Context, destination string, member any) (bool, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return false, err
	}
	return s.rc().SMove(ctx, s.name, destination, enc).Result()
}

// Size returns the cardinality of the set.
func (s *RSet) Size(ctx context.Context) (int64, error) {
	return s.rc().SCard(ctx, s.name).Result()
}

// Clear removes all members.
func (s *RSet) Clear(ctx context.Context) error { return s.Delete(ctx) }

func (s *RSet) encodeAll(members []any) ([]any, error) {
	enc := make([]any, len(members))
	for i, m := range members {
		str, err := s.c.codec.Encode(m)
		if err != nil {
			return nil, err
		}
		enc[i] = str
	}
	return enc, nil
}

func (s *RSet) decodeAll(vals []string) ([]any, error) {
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
