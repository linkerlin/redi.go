package redi_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisAvailable returns true when a real Redis server is reachable at the
// default address. Tests that require Redis are skipped when it is absent.
func redisAvailable(t *testing.T) bool {
	t.Helper()
	rc := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rc.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available – skipping integration test:", err)
		return false
	}
	return true
}

// uniqueKey returns a test-scoped key prefix to prevent cross-test pollution.
func uniqueKey(t *testing.T, suffix string) string {
	t.Helper()
	return "redi:test:" + t.Name() + ":" + suffix
}

// isNilErr reports whether err is redis.Nil.
func isNilErr(err error) bool {
	return errors.Is(err, redis.Nil)
}
