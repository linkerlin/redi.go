// Package redi provides a Pure Go Redisson-like client for Redis 8.x.
//
// Wire formats (key names, Lua scripts, value encoding) are aligned with Java
// Redisson configured with JsonJacksonCodec, and with the sibling ports
// redi.py (Python). See COMPATIBILITY.md for the per-structure status.
package redi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Mode selects the Redis deployment topology.
type Mode int

const (
	// ModeSingle connects to a single Redis server (default).
	ModeSingle Mode = iota
	// ModeCluster connects to a Redis Cluster.
	ModeCluster
	// ModeSentinel connects via Redis Sentinel (MasterName required).
	ModeSentinel
)

// Logger is the minimal logging interface used to report background
// goroutine failures (lock watchdog renewals, delayed-queue migration…).
type Logger interface {
	Printf(format string, v ...any)
}

// Config holds connection and behaviour options for a Client.
type Config struct {
	// Mode selects single / cluster / sentinel topology.
	Mode Mode
	// Addrs holds server addresses: the node for single, seed nodes for
	// cluster, sentinel addresses for sentinel mode.
	// Defaults to ["localhost:6379"].
	Addrs []string
	// MasterName is the sentinel master set name (sentinel mode only).
	MasterName string
	// Username for AUTH (empty if none).
	Username string
	// Password for authentication (empty if none).
	Password string
	// DB is the database number (single mode only).
	DB int
	// PoolSize is the number of connections in the pool.
	PoolSize int
	// DialTimeout for establishing new connections.
	DialTimeout time.Duration
	// ReadTimeout for socket reads.
	ReadTimeout time.Duration
	// WriteTimeout for socket writes.
	WriteTimeout time.Duration
	// Codec encodes/decodes values. Defaults to JSONCodec
	// (Redisson JsonJacksonCodec-compatible).
	Codec Codec
	// LockWatchdogTimeout is the lease time used by RLock when no explicit
	// TTL is given; the watchdog renews it every LockWatchdogTimeout/3.
	// Defaults to 30s (matching Redisson's lockWatchdogTimeout).
	LockWatchdogTimeout time.Duration
	// Logger receives background-goroutine error reports.
	// Defaults to the standard log package.
	Logger Logger
}

// DefaultConfig returns a Config with sensible defaults for Redis 8.x.
func DefaultConfig() Config {
	return Config{
		Mode:                ModeSingle,
		Addrs:               []string{"localhost:6379"},
		PoolSize:            10,
		DialTimeout:         5 * time.Second,
		ReadTimeout:         3 * time.Second,
		WriteTimeout:        3 * time.Second,
		Codec:               JSONCodec{},
		LockWatchdogTimeout: 30 * time.Second,
	}
}

// Client is the central entry point for all redi.go operations.
// It wraps a redis.UniversalClient (single / cluster / sentinel) and exposes
// factory methods for distributed data-structure objects.
type Client struct {
	rc    redis.UniversalClient
	cfg   Config
	codec Codec
	id    string // random per-client id (16 bytes hex), used where Redisson uses its connection manager id

	ctx    context.Context
	cancel context.CancelFunc
}

// NewClient creates a new Client using the provided Config and verifies the
// connection with a PING.
func NewClient(cfg Config) (*Client, error) {
	if len(cfg.Addrs) == 0 {
		cfg.Addrs = []string{"localhost:6379"}
	}
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 10
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 3 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 3 * time.Second
	}
	if cfg.Codec == nil {
		cfg.Codec = JSONCodec{}
	}
	if cfg.LockWatchdogTimeout <= 0 {
		cfg.LockWatchdogTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}

	var rc redis.UniversalClient
	switch cfg.Mode {
	case ModeCluster:
		rc = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        cfg.Addrs,
			Username:     cfg.Username,
			Password:     cfg.Password,
			PoolSize:     cfg.PoolSize,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
	case ModeSentinel:
		rc = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.MasterName,
			SentinelAddrs: cfg.Addrs,
			Username:      cfg.Username,
			Password:      cfg.Password,
			DB:            cfg.DB,
			PoolSize:      cfg.PoolSize,
			DialTimeout:   cfg.DialTimeout,
			ReadTimeout:   cfg.ReadTimeout,
			WriteTimeout:  cfg.WriteTimeout,
		})
	default:
		rc = redis.NewClient(&redis.Options{
			Addr:         cfg.Addrs[0],
			Username:     cfg.Username,
			Password:     cfg.Password,
			DB:           cfg.DB,
			PoolSize:     cfg.PoolSize,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()
	if err := rc.Ping(ctx).Err(); err != nil {
		_ = rc.Close()
		return nil, err
	}

	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		_ = rc.Close()
		return nil, err
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	return &Client{
		rc:     rc,
		cfg:    cfg,
		codec:  cfg.Codec,
		id:     hex.EncodeToString(id),
		ctx:    rootCtx,
		cancel: rootCancel,
	}, nil
}

// Close cancels all background goroutines (watchdogs, delayed-queue workers,
// topic listeners) and shuts down the underlying connection pool.
func (c *Client) Close() error {
	c.cancel()
	return c.rc.Close()
}

// Redis returns the underlying redis.UniversalClient for advanced direct use.
func (c *Client) Redis() redis.UniversalClient { return c.rc }

// ID returns this client's random id (hex string).
func (c *Client) ID() string { return c.id }

func (c *Client) logf(format string, v ...any) {
	c.cfg.Logger.Printf("redi: "+format, v...)
}

// GetRLock returns a distributed re-entrant lock for the given name.
func (c *Client) GetRLock(name string) *RLock { return newRLock(c, name) }

// GetLock is the Redisson-style alias for GetRLock.
func (c *Client) GetLock(name string) *RLock { return newRLock(c, name) }

// GetReadWriteLock returns a distributed read-write lock for the given name.
func (c *Client) GetReadWriteLock(name string) *RReadWriteLock {
	return newRReadWriteLock(c, name)
}

// GetRMap returns a distributed map (Redis Hash) for the given name.
func (c *Client) GetRMap(name string) *RMap { return newRMap(c, name) }

// GetMap is the Redisson-style alias for GetRMap.
func (c *Client) GetMap(name string) *RMap { return newRMap(c, name) }

// GetMapCache returns a distributed map with per-entry TTL/maxIdle.
func (c *Client) GetMapCache(name string) *RMapCache { return newRMapCache(c, name) }

// GetRList returns a distributed list (Redis List) for the given name.
func (c *Client) GetRList(name string) *RList { return newRList(c, name) }

// GetList is the Redisson-style alias for GetRList.
func (c *Client) GetList(name string) *RList { return newRList(c, name) }

// GetRSet returns a distributed set (Redis Set) for the given name.
func (c *Client) GetRSet(name string) *RSet { return newRSet(c, name) }

// GetSet is the Redisson-style alias for GetRSet.
func (c *Client) GetSet(name string) *RSet { return newRSet(c, name) }

// GetRQueue returns a distributed FIFO queue for the given name.
func (c *Client) GetRQueue(name string) *RQueue { return newRQueue(c, name) }

// GetQueue is the Redisson-style alias for GetRQueue.
func (c *Client) GetQueue(name string) *RQueue { return newRQueue(c, name) }

// GetBlockingQueue returns a queue with blocking consumption.
func (c *Client) GetBlockingQueue(name string) *RBlockingQueue {
	return newRBlockingQueue(c, name)
}

// GetBlockingDeque returns a deque with blocking consumption from both ends.
func (c *Client) GetBlockingDeque(name string) *RBlockingDeque {
	return newRBlockingDeque(c, name)
}

// GetDeque returns a distributed double-ended queue.
func (c *Client) GetDeque(name string) *RDeque { return newRDeque(c, name) }

// GetDelayedQueue returns a delayed-delivery queue feeding the named queue.
func (c *Client) GetDelayedQueue(name string) *RDelayedQueue {
	return newRDelayedQueue(c, name)
}

// GetTransferQueue returns a queue that can atomically transfer elements to
// a destination queue.
func (c *Client) GetTransferQueue(name string) *RTransferQueue {
	return newRTransferQueue(c, name)
}

// GetLexSortedSet returns a lexicographically sorted set (raw members).
func (c *Client) GetLexSortedSet(name string) *RLexSortedSet {
	return newRLexSortedSet(c, name)
}

// GetHyperLogLog returns a cardinality estimator (PFADD/PFCOUNT).
func (c *Client) GetHyperLogLog(name string) *RHyperLogLog {
	return newRHyperLogLog(c, name)
}

// GetGeo returns a geospatial index (GEOADD/GEOSEARCH).
func (c *Client) GetGeo(name string) *RGeo { return newRGeo(c, name) }

// GetBitSet returns a distributed bitset (raw Redis bit numbering).
func (c *Client) GetBitSet(name string) *RBitSet { return newRBitSet(c, name) }

// GetStream returns a distributed log stream (Redis Stream).
func (c *Client) GetStream(name string) *RStream { return newRStream(c, name) }

// GetScoredSortedSet returns a distributed sorted set (Redis ZSET).
func (c *Client) GetScoredSortedSet(name string) *RScoredSortedSet {
	return newRScoredSortedSet(c, name)
}

// GetBucket returns an object holder (Redis String).
func (c *Client) GetBucket(name string) *RBucket { return newRBucket(c, name) }

// GetRAtomicLong returns a distributed atomic counter for the given name.
func (c *Client) GetRAtomicLong(name string) *RAtomicLong {
	return newRAtomicLong(c, name)
}

// GetAtomicLong is the Redisson-style alias for GetRAtomicLong.
func (c *Client) GetAtomicLong(name string) *RAtomicLong {
	return newRAtomicLong(c, name)
}

// GetAtomicDouble returns a distributed atomic double.
func (c *Client) GetAtomicDouble(name string) *RAtomicDouble {
	return newRAtomicDouble(c, name)
}

// GetSemaphore returns a distributed counting semaphore.
func (c *Client) GetSemaphore(name string) *RSemaphore {
	return newRSemaphore(c, name)
}

// GetCountDownLatch returns a distributed countdown latch.
func (c *Client) GetCountDownLatch(name string) *RCountDownLatch {
	return newRCountDownLatch(c, name)
}

// GetRateLimiter returns a distributed rate limiter.
func (c *Client) GetRateLimiter(name string) *RRateLimiter {
	return newRRateLimiter(c, name)
}

// GetTopic returns a pub/sub topic.
func (c *Client) GetTopic(name string) *RTopic { return newRTopic(c, name) }

// GetPatternTopic returns a pattern (PSUBSCRIBE) topic.
func (c *Client) GetPatternTopic(pattern string) *RPatternTopic {
	return newRPatternTopic(c, pattern)
}

// GetSetCache returns a set with per-element TTL/maxIdle.
func (c *Client) GetSetCache(name string) *RSetCache { return newRSetCache(c, name) }

// GetSetMultimap returns a multimap with unordered value sets per key.
func (c *Client) GetSetMultimap(name string) *RSetMultimap {
	return newRSetMultimap(c, name)
}

// GetListMultimap returns a multimap with ordered value lists per key.
func (c *Client) GetListMultimap(name string) *RListMultimap {
	return newRListMultimap(c, name)
}

// GetBloomFilter returns a distributed bloom filter.
func (c *Client) GetBloomFilter(name string) *RBloomFilter {
	return newRBloomFilter(c, name)
}

// GetIdGenerator returns a distributed id generator.
func (c *Client) GetIdGenerator(name string) *RIdGenerator {
	return newRIdGenerator(c, name)
}

// GetKeys returns the keyspace management facade.
func (c *Client) GetKeys() *RKeys { return newRKeys(c) }

// GetBuckets returns the batch bucket operations facade.
func (c *Client) GetBuckets() *RBuckets { return newRBuckets(c) }

// GetScript returns the Lua script evaluation facade.
func (c *Client) GetScript() *RScript { return newRScript(c) }
