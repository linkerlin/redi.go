package redi_test

import (
	"os"
	"strings"
	"testing"
	"time"

	redi "github.com/linkerlin/redi.go"
)

// Optional smoke: set REDIS_CLUSTER_ADDRS=host1:7000,host2:7001 to exercise
// ModeCluster. Skips by default so CI (single redis:8) stays green.
func TestCluster_ModeConnectSmoke(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv("REDIS_CLUSTER_ADDRS"))
	if raw == "" {
		t.Skip("REDIS_CLUSTER_ADDRS not set")
	}
	cfg := redi.DefaultConfig()
	cfg.Mode = redi.ModeCluster
	cfg.Addrs = strings.Split(raw, ",")
	c, err := redi.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck

	name := uniqueKey(t, "cl-ht")
	rl := c.GetRateLimiter(name)
	ok, err := rl.TrySetRate(testCtx, redi.RateTypeOverall, 10, time.Second)
	if err != nil || !ok {
		t.Fatalf("TrySetRate on cluster = %v, %v (hash-tag CROSSSLOT?)", ok, err)
	}
	acquired, err := rl.TryAcquire(testCtx, 1)
	if err != nil || !acquired {
		t.Fatalf("TryAcquire on cluster = %v, %v", acquired, err)
	}
}
