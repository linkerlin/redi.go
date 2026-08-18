# redi.go

**中文** | [English](README_EN.md) | [Français](README_FR.md) | [日本語](README_JA.md) | [한국어](README_KO.md)

一个用 Pure Go 复刻 Redisson 的 Redis 客户端库，面向 **Redis 8.x**。
**Wire format（key 布局 / Lua 算法 / 值编码）与 Java Redisson（JsonJacksonCodec 配置）及 [redi.py](https://github.com/linkerlin/redi.py) 对齐，可互操作**——详见 [COMPATIBILITY.md](COMPATIBILITY.md)。
已通过 **Go ↔ 真实 Redisson 4.6.1 直接双向互操作测试**（56 个 `TestJavaInterop_*` 测试函数）与 Go ↔ redi.py 双向回归（含 Multimap 等）。`Client.Get*` 工厂当前覆盖 **66 种唯一 R* 返回类型**。贡献指南见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 特性

| 特性 | 说明 |
|---|---|
| **RLock / RFairLock / RSpinLock / RNonReentrantLock / RNonReentrantFairLock** | 可重入、公平 FIFO、自旋与禁止重入锁；支持固定租约或 watchdog |
| **RReadWriteLock** | 单 HASH + mode 字段的原子读写锁（含同线程 write→read 降级） |
| **RSemaphore / RCountDownLatch** | 分布式信号量 / 倒计时闩，pub/sub 唤醒 |
| **RRateLimiter** | 滑动窗口限流器，配置持久化到 Redis（跨进程生效），Redisson 4.6 二进制格式 |
| **RMap / RMapCache / RMapCacheNative** | 分布式 Hash；MapCache 支持 entry TTL/maxIdle、packed 表面与容量淘汰；MapCacheNative 用 Redis HPEXPIRE（≥7.4）；Map key 可派生 Lock/FairLock/RWLock/Semaphore |
| **RSetCache** | 带 TTL/maxIdle 的集合（单 ZSET，含随机/批量集合表面，Redisson 格式） |
| **RMultimap / RMultimapCache / *CacheNative（Set/List）** | 一键多值；内部 ID = HighwayHash-128 大端 base64；Cache 按 key TTL；Native 用 HPEXPIRE+PEXPIRE |
| **RList / RSet / RQueue / RDeque / RBlockingQueue / RBlockingDeque / RBoundedBlockingQueue** | 集合与队列全家桶；有界阻塞队列使用 `redisson_bqs:{name}` 容量 companion |
| **RDelayedQueue** | 延迟队列（timeout ZSET + packed LIST + 后台迁移；支持待投递项查询/删除/清空） |
| **RScoredSortedSet / RLexSortedSet** | 分数/字典序集合（rank、随机、首尾弹出、正反向范围；Lex 成员裸存储） |
| **RBucket** | 对象桶（毫秒 TTL、CAS、GetAndDelete…） |
| **RBinaryStream** | 原始字节流（APPEND/GETRANGE/SETRANGE、顺序流与可 seek 通道，不经过 Codec） |
| **RAtomicLong / RAtomicDouble** | 原子计数器（十进制字符串，CAS） |
| **RTopic / RPatternTopic** | 发布订阅（含模式订阅） |
| **RBloomFilter / RBloomFilterNative / RCuckooFilter / RTopK / RTDigest / RGcra** | 自研布隆 / Redis BF.* / CF.* / TOPK.* / TDIGEST.* / GCRA（命令缺失时测试自动 skip） |
| **RIdGenerator** | ID 生成器（批量分配缓存） |
| **RTransferQueue / GetQueueTransfer** | **GO_ONLY** 跨队列原子迁移（非 Java RemoteService TransferQueue） |
| **RHyperLogLog / RGeo / RBitSet / RStream** | 基数估计 / 地理索引（批量 GEO、GEOSEARCHSTORE）/ 分布式位图（Java 位序）/ 分布式日志（消费组、Pending、Claim/AutoClaim） |
| **RPermitExpirableSemaphore / RReliableTopic** | 按许可独立租约过期的信号量 / Stream 可靠广播主题（每订阅者独立消费组 + 崩溃重投递） |
| **RLocalCachedMap** | 近端缓存 Map（写穿 + Go 失效广播；混部可用 `DisableNearCache`）；数据层 RMap wire |
| **RPriorityQueue / RPriorityBlockingQueue / RPriorityBlockingDeque / RPriorityDeque** | ZSET+score 优先队列及阻塞/双端包装（**非** Java Comparator 同名协议） |
| **RArray / RCircularBuffer** | Redis 8.8+ ARRAY；CircularBuffer 为稳定槽位环（≠ RingBuffer）；命令缺失 skip |
| **RJsonBucket / RVectorSet / RSearch / RMaps** | RedisJSON / Vector Set / RediSearch 子集 / 批量 HASH 导入（模块缺失 skip） |
| **RClientSideCaching** | PARTIAL：go-redis RESP3 TRACKING（standalone DB0）；非 Java EvictionPolicy |
| **RLongAdder / RDoubleAdder** | 高争用计数器：本地零网络累积，`Sum()` 跨实例（含 Java）协同 flush；非破坏性 |
| **RFencedLock / RMultiLock / RRedLock** | 栅栏令牌锁 / 多锁全有或全无 / RedLock 严格多数派（失败回滚） |
| **RTimeSeries** | 时间序列（同刻多条、entry TTL、首尾读取/弹出、正反向范围，Redisson wire 兼容） |
| **RRingBuffer / RShardedTopic / RFunction** | 定容环形缓冲（溢出淘汰最旧）/ Redis 7+ 分片 pub/sub（SSUBSCRIBE，cluster 友好）/ Redis Functions（FUNCTION/FCALL） |
| **RKeys / RBuckets / RScript / GetRedisNodes** | 键空间 / 批量桶 / Lua / 拓扑探测 |
| **RBatch** | 管道批处理（`NewBatch()` → 结构写操作入队 → `Execute()` 单次往返，实测 **~7x** 加速） |
| **多拓扑** | single / cluster / sentinel（redis.UniversalClient） |
| **TUI Dashboard** | 基于 charmbracelet/bubbles 的终端监控面板 |
| **对齐路线** | 见 [演进方案.md](演进方案.md) 与 [COMPATIBILITY.md](COMPATIBILITY.md) 工厂总表 |

值编码默认 `JSONCodec`：与 Redisson `JsonJacksonCodec` 互通（超 int32 的整数自动包裹 `["java.lang.Long",v]`，解码剥离 `@class` 类型信息），可通过 `Config.Codec` 替换。结构化读取可用 `GetInto` / `PeekInto` / `PollInto` / `Remove*Into`（`RBucket` / `RMap` / `RList` / `RQueue` / `RDeque` / `RLocalCachedMap`）绑定到类型化指针；`RMap` 另有 `PutAll` / `FastPut*` / `Replace*` / `Keys` / `Values`。

## 依赖

- [redis/go-redis v9](https://github.com/redis/go-redis/v9) — Redis 驱动（single/cluster/sentinel 通用）
- [minio/highwayhash](https://github.com/minio/highwayhash) — RBloomFilter 的 Redisson 对齐哈希
- [charmbracelet/bubbles](https://github.com/charmbracelet/bubbles) — TUI 组件（仅 dashboard 子模块）

## 快速开始

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

    // --- RLock（ttl<=0 启用 watchdog 自动续期） ---
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

### 限流器

```go
rl := client.GetRateLimiter("api-limiter")
rl.TrySetRate(ctx, redi.RateTypeOverall, 100, time.Second) // 持久化到 Redis
if ok, _ := rl.TryAcquire(ctx, 1); !ok {
    // 被限流
}
```

### RBatch（管道批处理）

```go
batch := client.NewBatch()
m := batch.GetMap("my-map")
for i := 0; i < 1000; i++ {
    _ = m.Put(ctx, fmt.Sprint(i), i) // 入队，无网络往返
}
if err := batch.Execute(ctx); err != nil { // 单次往返写入全部
    log.Fatal(err)
}
```

（批处理结构上的读方法在 `Execute` 前返回零值 —— 用普通结构读回。）

### Cluster / Sentinel

```go
cfg := redi.DefaultConfig()
cfg.Mode = redi.ModeCluster
cfg.Addrs = []string{"node1:7000", "node2:7001", "node3:7002"}

// 或
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

三个页签：**INFO**（服务器指标）、**Locks**（持锁者/重入计数/TTL）、**Limiters**（限流器配置与剩余许可）。`1/2/3` 或 `←/→` 切换。

## 开发

```bash
go test ./...          # 需要本地 Redis（localhost:6379），无 Redis 自动 skip
go test -race ./...    # CI 完整模式
go test -run TestInterop -v .        # Go ↔ redi.py 双向互操作（需 Python + redi.py，缺失自动 skip）
go test -run TestJavaInterop -v .    # Go ↔ Java Redisson 4.6.1 直接双向（需 java+mvn，首次自动编译探针；CI 有独立 java-interop job）
go test -bench . -benchtime 1s .     # 基准
go vet ./...
```

wire-format 契约测试在 `wire_compat_test.go`；跨语言双向回归在 `interop_redipy_test.go`（经 redi.py——其格式已与真实 Redisson 双向实测——传递性验证）。任何破坏互操作的改动都会在这两处失败。

## 版本

见 [CHANGELOG.md](CHANGELOG.md)。v0.2.0 起 key 不再带 `redi:` 私有前缀（破坏性变更，换取互操作）。贡献流程见 [CONTRIBUTING.md](CONTRIBUTING.md)。
