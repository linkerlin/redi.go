package redi_test

import (
	"context"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

// redisAvailable returns true when a real Redis server is reachable at the
// default address. Tests that require Redis are skipped when it is absent.
func redisAvailable(t *testing.T) bool {
	t.Helper()
	client, err := redi.NewClient(redi.DefaultConfig())
	if err != nil {
		t.Skip("Redis not available – skipping integration test:", err)
		return false
	}
	t.Cleanup(func() { _ = client.Close() })
	return true
}

// newTestClient returns a shared-shape client for integration tests.
func newTestClient(t *testing.T) *redi.Client {
	t.Helper()
	if !redisAvailable(t) {
		return nil
	}
	client, err := redi.NewClient(redi.DefaultConfig())
	if err != nil {
		t.Fatal("NewClient:", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// uniqueKey returns a test-scoped key prefix to prevent cross-test pollution.
func uniqueKey(t *testing.T, suffix string) string {
	t.Helper()
	return "redi:test:" + t.Name() + ":" + suffix
}

var testCtx = context.Background()

func eventual(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}
