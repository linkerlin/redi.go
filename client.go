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
	// Protocol is the RESP version passed to go-redis (2 or 3). Zero lets
	// go-redis pick its default (currently 3). Required to be 3 when
	// ClientSideCaching is set.
	Protocol int
	// ClientSideCaching enables go-redis RESP3 client-side caching on
	// standalone ModeSingle clients (DB 0). Cluster/Sentinel ignore this
	// option. Prefer GetClientSideCachingWithOptions for a Redisson-shaped
	// runtime scope that owns its own tracked connection pool.
	ClientSideCaching *ClientSideCachingOptions
}

// ClientSideCachingOptions configures go-redis ClientSideCacheConfig (alias of
// CacheConfig). Eviction is LRU-ish inside go-redis LocalCache — not Java's
// EvictionPolicy enum (LRU/LFU/SOFT/WEAK).
type ClientSideCachingOptions struct {
	// MaxEntries limits cached replies; ≤0 lets go-redis apply its default bound.
	MaxEntries int
	// MaxMemoryBytes limits estimated cache memory; ≤0 means unlimited (subject
	// to MaxEntries defaulting).
	MaxMemoryBytes int64
	// DrainInterval bounds how often buffered invalidate frames are drained
	// (go-redis default ~5ms when zero).
	DrainInterval time.Duration
	// MaxStaleness caps how long a cached entry may be served without a fresh
	// fetch (0 = disabled). A backstop for lost invalidations.
	MaxStaleness time.Duration
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
		opt := &redis.ClusterOptions{
			Addrs:        cfg.Addrs,
			Username:     cfg.Username,
			Password:     cfg.Password,
			PoolSize:     cfg.PoolSize,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		}
		if cfg.Protocol >= 2 {
			opt.Protocol = cfg.Protocol
		}
		rc = redis.NewClusterClient(opt)
	case ModeSentinel:
		opt := &redis.FailoverOptions{
			MasterName:    cfg.MasterName,
			SentinelAddrs: cfg.Addrs,
			Username:      cfg.Username,
			Password:      cfg.Password,
			DB:            cfg.DB,
			PoolSize:      cfg.PoolSize,
			DialTimeout:   cfg.DialTimeout,
			ReadTimeout:   cfg.ReadTimeout,
			WriteTimeout:  cfg.WriteTimeout,
		}
		if cfg.Protocol >= 2 {
			opt.Protocol = cfg.Protocol
		}
		rc = redis.NewFailoverClient(opt)
	default:
		opt := &redis.Options{
			Addr:         cfg.Addrs[0],
			Username:     cfg.Username,
			Password:     cfg.Password,
			DB:           cfg.DB,
			PoolSize:     cfg.PoolSize,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		}
		if cfg.Protocol >= 2 {
			opt.Protocol = cfg.Protocol
		}
		if cfg.ClientSideCaching != nil {
			if opt.Protocol == 0 {
				opt.Protocol = 3
			}
			opt.ClientSideCacheConfig = &redis.ClientSideCacheConfig{
				MaxEntries:     cfg.ClientSideCaching.MaxEntries,
				MaxMemoryBytes: cfg.ClientSideCaching.MaxMemoryBytes,
				DrainInterval:  cfg.ClientSideCaching.DrainInterval,
				MaxStaleness:   cfg.ClientSideCaching.MaxStaleness,
			}
		}
		rc = redis.NewClient(opt)
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

// HolderID builds a Redisson-shaped lock HASH field "{clientUUID}:{threadID}".
// Use the same threadID for Lock/Unlock/re-entry on one logical holder.
// An empty threadID becomes "0".
func (c *Client) HolderID(threadID string) string {
	if threadID == "" {
		threadID = "0"
	}
	return c.id + ":" + threadID
}

func (c *Client) logf(format string, v ...any) {
	c.cfg.Logger.Printf("redi: "+format, v...)
}

// GetRLock returns a distributed re-entrant lock for the given name.
func (c *Client) GetRLock(name string) *RLock { return newRLock(c, name) }

// GetLock is the Redisson-style alias for GetRLock.
func (c *Client) GetLock(name string) *RLock { return newRLock(c, name) }

// GetFairLock returns a FIFO, re-entrant distributed lock.
func (c *Client) GetFairLock(name string) *RFairLock { return newRFairLock(c, name) }

// GetNonReentrantFairLock returns a FIFO lock that rejects holder re-entry.
func (c *Client) GetNonReentrantFairLock(name string) *RNonReentrantFairLock {
	return newRNonReentrantFairLock(c, name)
}

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

// GetMapCacheNative returns a map with per-entry TTL via Redis HPEXPIRE (Redis ≥7.4).
func (c *Client) GetMapCacheNative(name string) *RMapCacheNative {
	return newRMapCacheNative(c, name)
}

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

// GetBoundedBlockingQueue returns a capacity-bounded blocking queue.
func (c *Client) GetBoundedBlockingQueue(name string) *RBoundedBlockingQueue {
	return newRBoundedBlockingQueue(c, name)
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

// GetPermitExpirableSemaphore returns a semaphore with individually leased,
// self-expiring permits.
func (c *Client) GetPermitExpirableSemaphore(name string) *RPermitExpirableSemaphore {
	return newRPermitExpirableSemaphore(c, name)
}

// GetReliableTopic returns a reliable (Stream-backed) pub/sub topic.
func (c *Client) GetReliableTopic(name string) *RReliableTopic {
	return newRReliableTopic(c, name)
}

// GetLocalCachedMap returns a map with an in-process near cache
// (write-through + cross-instance invalidation broadcast).
func (c *Client) GetLocalCachedMap(name string) *RLocalCachedMap {
	return newRLocalCachedMap(c, name)
}

// GetPriorityQueue returns a ZSET-backed priority queue (lower score =
// higher priority). Not Java Comparator RPriorityQueue — see COMPATIBILITY.
func (c *Client) GetPriorityQueue(name string) *RPriorityQueue {
	return newRPriorityQueue(c, name)
}

// GetPriorityBlockingQueue returns a BZPOPMIN wrapper over GetPriorityQueue.
// Not Java Comparator RPriorityBlockingQueue.
func (c *Client) GetPriorityBlockingQueue(name string) *RPriorityBlockingQueue {
	return newRPriorityBlockingQueue(c, name)
}

// GetPriorityBlockingDeque returns a BZPOPMIN/BZPOPMAX wrapper over the ZSET
// priority queue. Not Java Comparator RPriorityBlockingDeque.
func (c *Client) GetPriorityBlockingDeque(name string) *RPriorityBlockingDeque {
	return newRPriorityBlockingDeque(c, name)
}

// GetPriorityDeque returns non-blocking double-ended ZSET pops.
// Not Java Comparator RPriorityDeque.
func (c *Client) GetPriorityDeque(name string) *RPriorityDeque {
	return newRPriorityDeque(c, name)
}

// GetArray returns a Redis 8.8+ ARRAY object (AR* commands).
func (c *Client) GetArray(name string) *RArray { return newRArray(c, name) }

// GetClientSideCaching returns a facade over this Client.
// Caching only occurs when Config.ClientSideCaching was set at NewClient, or
// use GetClientSideCachingWithOptions for a dedicated tracked pool (closer to
// Redisson getClientSideCaching(options)).
func (c *Client) GetClientSideCaching() *RClientSideCaching {
	return newRClientSideCaching(c)
}

// GetClientSideCachingWithOptions creates a dedicated RESP3 CLIENT TRACKING
// client (ModeSingle, DB 0) and returns an owned RClientSideCaching.
// Caller must Destroy() it (or rely on parent Close after registering via
// t.Cleanup in tests). Nil opts use go-redis defaults.
func (c *Client) GetClientSideCachingWithOptions(opts *ClientSideCachingOptions) (*RClientSideCaching, error) {
	child, err := newScopedCSCClient(c, opts)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.DialTimeout)
	defer cancel()
	if err := pingCSCReady(ctx, child); err != nil {
		_ = child.Close()
		return nil, err
	}
	return newOwnedRClientSideCaching(child), nil
}

// GetLongAdder returns a high-contention distributed counter (local buffer
// + coordinated flush on Sum).
func (c *Client) GetLongAdder(name string) *RLongAdder {
	return newRLongAdder(c, name)
}

// GetDoubleAdder returns the double variant of GetLongAdder.
func (c *Client) GetDoubleAdder(name string) *RDoubleAdder {
	return newRDoubleAdder(c, name)
}

// GetFencedLock returns a re-entrant lock whose acquisitions increment a
// fencing token (redisson_lock_token:{name}).
func (c *Client) GetFencedLock(name string) *RFencedLock {
	return newRFencedLock(c, name)
}

// GetSpinLock returns a re-entrant lock that busy-waits instead of pub/sub.
func (c *Client) GetSpinLock(name string) *RSpinLock {
	return newRSpinLock(c, name)
}

// GetNonReentrantLock returns a lock that refuses re-entry by the same holder.
func (c *Client) GetNonReentrantLock(name string) *RNonReentrantLock {
	return newRNonReentrantLock(c, name)
}

// NewMultiLock groups RLocks into one all-or-nothing lock.
func (c *Client) NewMultiLock(locks ...*RLock) *RMultiLock {
	return &RMultiLock{locks: locks}
}

// NewRedLock groups independent RLocks using RedissonRedLock majority rules.
func (c *Client) NewRedLock(locks ...*RLock) *RRedLock {
	return &RRedLock{locks: locks}
}

// GetTimeSeries returns a time-series store (Redisson wire-compatible).
func (c *Client) GetTimeSeries(name string) *RTimeSeries {
	return newRTimeSeries(c, name)
}

// GetRingBuffer returns a capacity-bounded FIFO that evicts on overflow.
func (c *Client) GetRingBuffer(name string) *RRingBuffer {
	return newRRingBuffer(c, name)
}

// GetShardedTopic returns a Redis 7+ sharded pub/sub topic (SSUBSCRIBE).
func (c *Client) GetShardedTopic(name string) *RShardedTopic {
	return newRShardedTopic(c, name)
}

// GetFunction returns the Redis functions facade (FUNCTION/FCALL).
func (c *Client) GetFunction() *RFunction { return newRFunction(c) }

// GetScoredSortedSet returns a distributed sorted set (Redis ZSET).
func (c *Client) GetScoredSortedSet(name string) *RScoredSortedSet {
	return newRScoredSortedSet(c, name)
}

// GetBucket returns an object holder (Redis String).
func (c *Client) GetBucket(name string) *RBucket { return newRBucket(c, name) }

// GetBinaryStream returns a raw-byte stream backed by a Redis String.
func (c *Client) GetBinaryStream(name string) *RBinaryStream {
	return newRBinaryStream(c, name)
}

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

// GetSetMultimapCache returns a set multimap with per-key expiration.
func (c *Client) GetSetMultimapCache(name string) *RSetMultimapCache {
	return newRSetMultimapCache(c, name)
}

// GetListMultimapCache returns a list multimap with per-key expiration.
func (c *Client) GetListMultimapCache(name string) *RListMultimapCache {
	return newRListMultimapCache(c, name)
}

// GetSetMultimapCacheNative returns a set multimap with native per-key HPEXPIRE.
func (c *Client) GetSetMultimapCacheNative(name string) *RSetMultimapCacheNative {
	return newRSetMultimapCacheNative(c, name)
}

// GetListMultimapCacheNative returns a list multimap with native per-key HPEXPIRE.
func (c *Client) GetListMultimapCacheNative(name string) *RListMultimapCacheNative {
	return newRListMultimapCacheNative(c, name)
}

// GetBloomFilter returns a distributed bloom filter.
func (c *Client) GetBloomFilter(name string) *RBloomFilter {
	return newRBloomFilter(c, name)
}

// GetBloomFilterNative returns a bloom filter backed by Redis BF.* commands.
func (c *Client) GetBloomFilterNative(name string) *RBloomFilterNative {
	return newRBloomFilterNative(c, name)
}

// GetCuckooFilter returns a cuckoo filter backed by Redis CF.* commands.
func (c *Client) GetCuckooFilter(name string) *RCuckooFilter {
	return newRCuckooFilter(c, name)
}

// GetTopK returns a Top-K sketch backed by Redis TOPK.* commands.
func (c *Client) GetTopK(name string) *RTopK { return newRTopK(c, name) }

// GetTDigest returns a t-digest sketch backed by Redis TDIGEST.* commands.
func (c *Client) GetTDigest(name string) *RTDigest { return newRTDigest(c, name) }

// GetGcra returns a GCRA rate limiter (Redis GCRA command when available).
func (c *Client) GetGcra(name string) *RGcra { return newRGcra(c, name) }

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
