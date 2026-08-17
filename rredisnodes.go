package redi

import (
	"context"
	"fmt"
)

// RRedisNodes is a thin topology probe analogous to Redisson getRedisNodes.
type RRedisNodes struct{ c *Client }

func newRRedisNodes(c *Client) *RRedisNodes { return &RRedisNodes{c: c} }

// Mode returns the configured topology.
func (n *RRedisNodes) Mode() Mode { return n.c.cfg.Mode }

// Ping checks connectivity.
func (n *RRedisNodes) Ping(ctx context.Context) error {
	return n.c.rc.Ping(ctx).Err()
}

// Info returns INFO server section (best-effort; cluster returns one node).
func (n *RRedisNodes) Info(ctx context.Context) (string, error) {
	return n.c.rc.Info(ctx, "server").Result()
}

// String describes the mode and addresses.
func (n *RRedisNodes) String() string {
	return fmt.Sprintf("mode=%d addrs=%v", n.c.cfg.Mode, n.c.cfg.Addrs)
}
