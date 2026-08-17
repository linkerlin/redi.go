# redi.go

[中文](README.md) | **English** | [Français](README_FR.md) | [日本語](README_JA.md) | [한국어](README_KO.md)

A Pure Go Redis client library that mirrors Redisson, targeting **Redis 8.x**.
**Wire format (key layout / Lua algorithms / value encoding) aligns with Java Redisson (JsonJacksonCodec) and [redi.py](https://github.com/linkerlin/redi.py) for interoperability** — see [COMPATIBILITY.md](COMPATIBILITY.md).
It has passed **Go ↔ real Redisson 4.6.1 bidirectional interop tests** (33 `TestJavaInterop_*` test functions) and Go ↔ redi.py bidirectional regression (including Multimap). `Client.Get*` factories currently cover **66 unique R\* return types**. See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## Features

| Feature | Description |
|---|---|
| **RLock / RFairLock / RSpinLock / RNonReentrantLock / RNonReentrantFairLock** | Reentrant, fair FIFO, spin, and non-reentrant locks; fixed lease or watchdog |
| **RReadWriteLock** | Atomic read/write lock on a single HASH + `mode` field (including same-thread write→read downgrade) |
| **RSemaphore / RCountDownLatch** | Distributed semaphore / countdown latch with pub/sub wakeups |
| **RRateLimiter** | Sliding-window rate limiter; config persisted in Redis (cross-process); Redisson 4.6 binary format |
| **RMap / RMapCache / RMapCacheNative** | Distributed hash; MapCache supports entry TTL/maxIdle, packed surface and max-size eviction; MapCacheNative uses Redis HPEXPIRE (≥7.4); map keys can derive Lock/FairLock/RWLock/Semaphore |
| **RSetCache** | Set with TTL/maxIdle (single ZSET, random/bulk set surface, Redisson format) |
| **RMultimap / RMultimapCache / *CacheNative (Set/List)** | One key, many values; internal ID = HighwayHash-128 big-endian base64; Cache uses per-key TTL; Native uses HPEXPIRE+PEXPIRE |
| **RList / RSet / RQueue / RDeque / RBlockingQueue / RBlockingDeque / RBoundedBlockingQueue** | Full collection and queue family; bounded blocking queue uses `redisson_bqs:{name}` capacity companion |
| **RDelayedQueue** | Delayed queue (timeout ZSET + packed LIST + background transfer; query/remove/clear pending items) |
| **RScoredSortedSet / RLexSortedSet** | Scored / lexicographic sets (rank, random, first/last poll, forward/reverse ranges; Lex members stored raw) |
| **RBucket** | Object bucket (millisecond TTL, CAS, GetAndDelete, …) |
| **RBinaryStream** | Raw byte stream (APPEND/GETRANGE/SETRANGE, sequential streams and seekable channel; bypasses Codec) |
| **RAtomicLong / RAtomicDouble** | Atomic counters (decimal strings, CAS) |
| **RTopic / RPatternTopic** | Pub/sub (including pattern subscription) |
| **RBloomFilter / RBloomFilterNative / RCuckooFilter / RTopK / RTDigest / RGcra** | Classic bloom / Redis BF.* / CF.* / TOPK.* / TDIGEST.* / GCRA (tests auto-skip when commands are missing) |
| **RIdGenerator** | ID generator (batched allocation cache) |
| **RTransferQueue / GetQueueTransfer** | **GO_ONLY** atomic queue migrate (not Java RemoteService TransferQueue) |
| **RHyperLogLog / RGeo / RBitSet / RStream** | Cardinality / geo index (bulk GEO, GEOSEARCHSTORE) / distributed bitmap (Java bit order) / distributed log (consumer groups, Pending, Claim/AutoClaim) |
| **RPermitExpirableSemaphore / RReliableTopic** | Semaphore with per-permit leases / Stream-based reliable topic (per-subscriber consumer group + crash redelivery) |
| **RLocalCachedMap** | Near cache (write-through + Go invalidation; use `DisableNearCache` when mixed with Java); data layer = RMap wire |
| **RPriorityQueue / RPriorityBlockingQueue / RPriorityBlockingDeque / RPriorityDeque** | ZSET+score priority queue and blocking/double-ended wrappers (**not** Java Comparator same-name protocol) |
| **RArray / RCircularBuffer** | Redis 8.8+ ARRAY; CircularBuffer stable slots (≠ RingBuffer); skip when missing |
| **RJsonBucket / RVectorSet / RSearch / RMaps** | RedisJSON / Vector Set / RediSearch subset / bulk HASH import (skip when modules missing) |
| **RClientSideCaching** | PARTIAL: go-redis RESP3 TRACKING (standalone DB0); not Java EvictionPolicy |
| **RLongAdder / RDoubleAdder** | High-contention counters: local zero-network accumulation; `Sum()` coordinates flush across instances (including Java); non-destructive |
| **RFencedLock / RMultiLock / RRedLock** | Fenced token lock / all-or-nothing multi-lock / RedLock strict majority (rollback on failure) |
| **RTimeSeries** | Time series (multiple entries per timestamp, entry TTL, first/last read/poll, forward/reverse ranges; Redisson wire compatible) |
| **RRingBuffer / RShardedTopic / RFunction** | Fixed-capacity ring buffer (evict oldest on overflow) / Redis 7+ sharded pub/sub (SSUBSCRIBE, cluster-friendly) / Redis Functions (FUNCTION/FCALL) |
| **RKeys / RBuckets / RScript / GetRedisNodes** | Keyspace / bulk buckets / Lua / topology probe |
| **RBatch** | Pipeline batching (`NewBatch()` → enqueue structure writes → `Execute()` in one round-trip; measured **~7×** speedup) |
| **Topologies** | single / cluster / sentinel (`redis.UniversalClient`) |
| **TUI Dashboard** | Terminal monitoring panel based on charmbracelet/bubbles |
| **Alignment roadmap** | See [演进方案.md](演进方案.md) and [COMPATIBILITY.md](COMPATIBILITY.md) |

Default value codec is `JSONCodec`: interoperable with Redisson `JsonJacksonCodec` (integers beyond int32 are wrapped as `["java.lang.Long",v]`; decode strips `@class` typing). Override via `Config.Codec`. Structured reads can use `GetInto` / `PeekInto` / `PollInto` / `Remove*Into` on `RBucket` / `RMap` / `RList` / `RQueue` / `RDeque` / `RLocalCachedMap` to bind into typed pointers; `RMap` also exposes `PutAll` / `FastPut*` / `Replace*` / `Keys` / `Values`.

## Dependencies

- [redis/go-redis v9](https://github.com/redis/go-redis/v9) — Redis driver (single/cluster/sentinel)
- [minio/highwayhash](https://github.com/minio/highwayhash) — Redisson-aligned hash for RBloomFilter
- [charmbracelet/bubbles](https://github.com/charmbracelet/bubbles) — TUI components (dashboard submodule only)

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    redi "github.com/linkerlin/redi.go"
)

func main() {
    client, err := redi.NewClient(redi.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // --- RLock (ttl<=0 enables watchdog renewal) ---
    lock := client.GetLock("my-lock")
    if err := lock.Lock(ctx, "instance-1:1", 0); err != nil {
        log.Fatal(err)
    }
    defer lock.Unlock(ctx, "instance-1:1")

    // --- RMap ---
    m := client.GetMap("my-map")
    _ = m.Put(ctx, "key", "value")
    val, _ := m.Get(ctx, "key")
    fmt.Println(val) // value

    // --- RAtomicLong ---
    counter := client.GetAtomicLong("hits")
    n, _ := counter.IncrementAndGet(ctx)
    fmt.Println(n) // 1
}
```

### Rate limiter

```go
rl := client.GetRateLimiter("api-limiter")
rl.TrySetRate(ctx, redi.RateTypeOverall, 100, time.Second) // persisted in Redis
if ok, _ := rl.TryAcquire(ctx, 1); !ok {
    // rate limited
}
```

### RBatch (pipeline)

```go
batch := client.NewBatch()
m := batch.GetMap("my-map")
for i := 0; i < 1000; i++ {
    _ = m.Put(ctx, fmt.Sprint(i), i) // enqueued; no network round-trip yet
}
if err := batch.Execute(ctx); err != nil { // one round-trip for all writes
    log.Fatal(err)
}
```

(Reads on batch-bound structures return zero values before `Execute` — use a normal structure to read back.)

### Cluster / Sentinel

```go
cfg := redi.DefaultConfig()
cfg.Mode = redi.ModeCluster
cfg.Addrs = []string{"node1:7000", "node2:7001", "node3:7002"}

// or
cfg.Mode = redi.ModeSentinel
cfg.MasterName = "mymaster"
cfg.Addrs = []string{"sentinel-1:26379", "sentinel-2:26379"}
```

## TUI Dashboard

```go
import "github.com/linkerlin/redi.go/dashboard"

// Blocks until the user presses 'q'.
// Locks tab scans the given lock-name patterns (default "*" — Redisson
// lock keys are the raw name, so pass your prefixes to scope the scan).
if err := dashboard.Run(client.Redis(), "order-lock", "job-*"); err != nil {
    log.Fatal(err)
}
```

Three tabs: **INFO** (server metrics), **Locks** (holders / re-entry count / TTL), **Limiters** (limiter config and remaining permits). Switch with `1/2/3` or `←/→`.

## Development

```bash
go test ./...          # needs local Redis (localhost:6379); auto-skips without Redis
go test -race ./...    # full CI mode
go test -run TestInterop -v .        # Go ↔ redi.py bidirectional interop (needs Python + redi.py; auto-skips if missing)
go test -run TestJavaInterop -v .    # Go ↔ Java Redisson 4.6.1 bidirectional (needs java+mvn; probe auto-builds on first run; CI has a dedicated java-interop job)
go test -bench . -benchtime 1s .     # benchmarks
go vet ./...
```

Wire-format contract tests live in `wire_compat_test.go`; cross-language bidirectional regression is in `interop_redipy_test.go` (via redi.py — whose format has been verified bidirectionally against real Redisson — for transitive validation). Any change that breaks interoperability will fail in these suites.

## Versioning

See [CHANGELOG.md](CHANGELOG.md). Since v0.2.0, keys no longer use a private `redi:` prefix (breaking change in exchange for interoperability). Contribution workflow: [CONTRIBUTING.md](CONTRIBUTING.md).
