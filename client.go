// Package redi provides a Pure Go Redisson-like client for Redis 8.x.
//
// It wraps github.com/redis/go-redis/v9, uses github.com/linkerlin/gotrycatch
// for structured error handling, and github.com/linkerlin/GoExecutors for
// concurrency management.
package redi

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds connection options for the Redis client.
type Config struct {
	// Addr is the Redis server address, e.g. "localhost:6379".
	Addr string
	// Password for authentication (empty if none).
	Password string
	// DB is the database number to select.
	DB int
	// PoolSize is the number of connections in the pool.
	PoolSize int
	// DialTimeout for establishing new connections.
	DialTimeout time.Duration
	// ReadTimeout for socket reads.
	ReadTimeout time.Duration
	// WriteTimeout for socket writes.
	WriteTimeout time.Duration
}

// DefaultConfig returns a Config with sensible defaults for Redis 8.x.
func DefaultConfig() Config {
	return Config{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		PoolSize:     10,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

// Client is the central entry point for all redi.go operations.
// It holds a redis.Client and exposes factory methods for distributed
// data-structure objects (RLock, RMap, RList, …).
type Client struct {
	rc  *redis.Client
	cfg Config
}

// NewClient creates a new Client using the provided Config.
func NewClient(cfg Config) (*Client, error) {
	rc := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if err := rc.Ping(ctx).Err(); err != nil {
		_ = rc.Close()
		return nil, err
	}

	return &Client{rc: rc, cfg: cfg}, nil
}

// Close shuts down the underlying Redis connection pool.
func (c *Client) Close() error {
	return c.rc.Close()
}

// Redis returns the underlying *redis.Client for advanced direct use.
func (c *Client) Redis() *redis.Client {
	return c.rc
}

// GetRLock returns a distributed lock object for the given name.
func (c *Client) GetRLock(name string) *RLock {
	return newRLock(c.rc, name)
}

// GetRMap returns a distributed map (Redis Hash) for the given name.
func (c *Client) GetRMap(name string) *RMap {
	return newRMap(c.rc, name)
}

// GetRList returns a distributed list (Redis List) for the given name.
func (c *Client) GetRList(name string) *RList {
	return newRList(c.rc, name)
}

// GetRSet returns a distributed set (Redis Set) for the given name.
func (c *Client) GetRSet(name string) *RSet {
	return newRSet(c.rc, name)
}

// GetRAtomicLong returns a distributed atomic counter (Redis String) for
// the given name.
func (c *Client) GetRAtomicLong(name string) *RAtomicLong {
	return newRAtomicLong(c.rc, name)
}

// GetRQueue returns a distributed FIFO queue (Redis List) for the given name.
func (c *Client) GetRQueue(name string) *RQueue {
	return newRQueue(c.rc, name)
}
