package redi

import (
	"context"

	"github.com/linkerlin/gotrycatch"
	"github.com/redis/go-redis/v9"
)

// RSet is a distributed set backed by a Redis Set.
type RSet struct {
	rc   *redis.Client
	name string
	key  string
}

func newRSet(rc *redis.Client, name string) *RSet {
	return &RSet{rc: rc, name: name, key: "redi:set:" + name}
}

// Add adds members to the set.
func (s *RSet) Add(ctx context.Context, members ...any) error {
	var addErr error
	tb := gotrycatch.Try(func() {
		if err := s.rc.SAdd(ctx, s.key, members...).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { addErr = err })
	tb.Finally(func() {})
	return addErr
}

// Remove removes members from the set.
func (s *RSet) Remove(ctx context.Context, members ...any) error {
	var rmErr error
	tb := gotrycatch.Try(func() {
		if err := s.rc.SRem(ctx, s.key, members...).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { rmErr = err })
	tb.Finally(func() {})
	return rmErr
}

// Contains reports whether member is in the set.
func (s *RSet) Contains(ctx context.Context, member any) (bool, error) {
	var exists bool
	var existsErr error
	tb := gotrycatch.Try(func() {
		v, err := s.rc.SIsMember(ctx, s.key, member).Result()
		if err != nil {
			panic(err)
		}
		exists = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { existsErr = err })
	tb.Finally(func() {})
	return exists, existsErr
}

// Members returns all members in the set.
func (s *RSet) Members(ctx context.Context) ([]string, error) {
	var members []string
	var memErr error
	tb := gotrycatch.Try(func() {
		v, err := s.rc.SMembers(ctx, s.key).Result()
		if err != nil {
			panic(err)
		}
		members = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { memErr = err })
	tb.Finally(func() {})
	return members, memErr
}

// Size returns the cardinality of the set.
func (s *RSet) Size(ctx context.Context) (int64, error) {
	var sz int64
	var szErr error
	tb := gotrycatch.Try(func() {
		v, err := s.rc.SCard(ctx, s.key).Result()
		if err != nil {
			panic(err)
		}
		sz = v
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { szErr = err })
	tb.Finally(func() {})
	return sz, szErr
}

// Clear removes all members from the set.
func (s *RSet) Clear(ctx context.Context) error {
	var clearErr error
	tb := gotrycatch.Try(func() {
		if err := s.rc.Del(ctx, s.key).Err(); err != nil {
			panic(err)
		}
	})
	tb = gotrycatch.Catch[error](tb, func(err error) { clearErr = err })
	tb.Finally(func() {})
	return clearErr
}
