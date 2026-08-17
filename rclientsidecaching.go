package redi

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrClientSideCachingUnavailable is returned when a scoped CSC client cannot
// be created (non-single mode, non-zero DB, or RESP2 forced).
var ErrClientSideCachingUnavailable = errors.New("redi: client-side caching requires ModeSingle, DB 0, and RESP3")

// RClientSideCaching mirrors Redisson getClientSideCaching: factories return
// structures whose Redis traffic may be served from go-redis RESP3 CLIENT
// TRACKING cache.
//
// Two construction paths:
//
//  1. Config.ClientSideCaching at NewClient — CSC on the shared pool; GetClientSideCaching()
//     is a zero-cost facade over that Client.
//  2. GetClientSideCachingWithOptions — Redisson-shaped runtime scope: a dedicated
//     redis.Client (Protocol 3 + ClientSideCacheConfig) owned by this facade;
//     Destroy() closes it.
//
// Still PARTIAL vs Java: no in-process read-proxy with evictionPolicy LRU/LFU/SOFT/WEAK
// over method arguments; go-redis owns the reply cache. Cluster/Sentinel unsupported.
type RClientSideCaching struct {
	c     *Client
	owned bool // true when c is a dedicated CSC child client

	mu       sync.Mutex
	destroyed bool
}

func newRClientSideCaching(c *Client) *RClientSideCaching {
	return &RClientSideCaching{c: c, owned: false}
}

func newOwnedRClientSideCaching(c *Client) *RClientSideCaching {
	return &RClientSideCaching{c: c, owned: true}
}

// Enabled reports whether this facade's Client has CSC configured.
func (csc *RClientSideCaching) Enabled() bool {
	if csc == nil || csc.c == nil {
		return false
	}
	csc.mu.Lock()
	defer csc.mu.Unlock()
	if csc.destroyed {
		return false
	}
	return csc.c.cfg.ClientSideCaching != nil || csc.owned
}

// Owned reports whether Destroy closes a dedicated CSC client.
func (csc *RClientSideCaching) Owned() bool {
	return csc != nil && csc.owned
}

// Destroy closes an owned CSC client (GetClientSideCachingWithOptions).
// Shared-pool facades are a no-op (pool lives with Client.Close).
func (csc *RClientSideCaching) Destroy() error {
	if csc == nil {
		return nil
	}
	csc.mu.Lock()
	defer csc.mu.Unlock()
	if csc.destroyed {
		return nil
	}
	csc.destroyed = true
	if !csc.owned || csc.c == nil {
		return nil
	}
	return csc.c.Close()
}

func (csc *RClientSideCaching) live() *Client {
	csc.mu.Lock()
	defer csc.mu.Unlock()
	if csc.destroyed {
		return nil
	}
	return csc.c
}

func (csc *RClientSideCaching) must() *Client {
	c := csc.live()
	if c == nil {
		// Keep call sites simple: destroyed facade still returns structures that
		// fail on use via closed pool rather than panicking here.
		return csc.c
	}
	return c
}

// GetBucket forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetBucket(name string) *RBucket { return csc.must().GetBucket(name) }

// GetMap forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetMap(name string) *RMap { return csc.must().GetMap(name) }

// GetSet forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetSet(name string) *RSet { return csc.must().GetSet(name) }

// GetList forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetList(name string) *RList { return csc.must().GetList(name) }

// GetQueue forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetQueue(name string) *RQueue { return csc.must().GetQueue(name) }

// GetDeque forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetDeque(name string) *RDeque { return csc.must().GetDeque(name) }

// GetBlockingQueue forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetBlockingQueue(name string) *RBlockingQueue {
	return csc.must().GetBlockingQueue(name)
}

// GetBlockingDeque forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetBlockingDeque(name string) *RBlockingDeque {
	return csc.must().GetBlockingDeque(name)
}

// GetScoredSortedSet forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetScoredSortedSet(name string) *RScoredSortedSet {
	return csc.must().GetScoredSortedSet(name)
}

// GetStream forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetStream(name string) *RStream { return csc.must().GetStream(name) }

// GetGeo forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetGeo(name string) *RGeo { return csc.must().GetGeo(name) }

// GetAtomicLong forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetAtomicLong(name string) *RAtomicLong {
	return csc.must().GetAtomicLong(name)
}

// GetAtomicDouble forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetAtomicDouble(name string) *RAtomicDouble {
	return csc.must().GetAtomicDouble(name)
}

// GetBitSet forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetBitSet(name string) *RBitSet {
	return csc.must().GetBitSet(name)
}

// GetHyperLogLog forwards to the CSC-backed Client.
func (csc *RClientSideCaching) GetHyperLogLog(name string) *RHyperLogLog {
	return csc.must().GetHyperLogLog(name)
}

// newScopedCSCClient builds a dedicated standalone Client with RESP3 CSC.
func newScopedCSCClient(parent *Client, opts *ClientSideCachingOptions) (*Client, error) {
	if parent == nil {
		return nil, ErrClientSideCachingUnavailable
	}
	if parent.cfg.Mode != ModeSingle {
		return nil, fmt.Errorf("%w: mode=%v", ErrClientSideCachingUnavailable, parent.cfg.Mode)
	}
	if parent.cfg.DB != 0 {
		return nil, fmt.Errorf("%w: db=%d", ErrClientSideCachingUnavailable, parent.cfg.DB)
	}
	if parent.cfg.Protocol == 2 {
		return nil, fmt.Errorf("%w: Protocol=2", ErrClientSideCachingUnavailable)
	}
	if len(parent.cfg.Addrs) == 0 {
		return nil, ErrClientSideCachingUnavailable
	}

	cfg := parent.cfg
	if opts == nil {
		opts = &ClientSideCachingOptions{}
	}
	cfg.ClientSideCaching = opts
	cfg.Protocol = 3
	cfg.Mode = ModeSingle

	return NewClient(cfg)
}

// pingCSCReady verifies the child client can talk RESP3 to Redis.
func pingCSCReady(ctx context.Context, c *Client) error {
	return c.rc.Ping(ctx).Err()
}
