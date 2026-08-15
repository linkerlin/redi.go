package redi

import (
	"context"
)

// RSet is a distributed set backed by a Redis Set.
// Members are codec (JSON) encoded, Redisson-compatible.
type RSet struct {
	rObject
}

func newRSet(c *Client, name string) *RSet {
	return &RSet{rObject{c: c, name: name}}
}

// Add adds members to the set.
func (s *RSet) Add(ctx context.Context, members ...any) error {
	enc, err := s.encodeAll(members)
	if err != nil {
		return err
	}
	return s.rc().SAdd(ctx, s.name, enc...).Err()
}

// Remove removes members from the set.
func (s *RSet) Remove(ctx context.Context, members ...any) error {
	enc, err := s.encodeAll(members)
	if err != nil {
		return err
	}
	return s.rc().SRem(ctx, s.name, enc...).Err()
}

// Contains reports whether member is in the set.
func (s *RSet) Contains(ctx context.Context, member any) (bool, error) {
	enc, err := s.c.codec.Encode(member)
	if err != nil {
		return false, err
	}
	return s.rc().SIsMember(ctx, s.name, enc).Result()
}

// Members returns all members in the set (decoded).
func (s *RSet) Members(ctx context.Context) ([]any, error) {
	vals, err := s.rc().SMembers(ctx, s.name).Result()
	if err != nil {
		return nil, err
	}
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
