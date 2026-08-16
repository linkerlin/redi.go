# redi.go

一个用 Pure Go 复刻 Redisson 的 Redis 客户端库，面向 **Redis 8.x**。
**Wire format（key 布局 / Lua 算法 / 值编码）与 Java Redisson（JsonJacksonCodec 配置）及 [redi.py](https://github.com/linkerlin/redi.py) 对齐，可互操作**——详见 [COMPATIBILITY.md](COMPATIBILITY.md)。
已通过 **Go ↔ 真实 Redisson 4.6.1 直接双向互操作测试**（锁互斥、值编码、Bloom 哈希、限流共享窗口、跨语言唤醒等 7 组用例）与 Go ↔ redi.py 双向回归（10 组）。

## 特性

| 特性 | 说明 |
|---|---|
| **RLock** | 分布式可重入锁：Redisson 原版 HASH + Lua，pub/sub 唤醒（~0ms），watchdog 自动续期 |
| **RReadWriteLock** | 单 HASH + mode 字段的原子读写锁（含同线程 write→read 降级） |
| **RSemaphore / RCountDownLatch** | 分布式信号量 / 倒计时闩，pub/sub 唤醒 |
| **RRateLimiter** | 滑动窗口限流器，配置持久化到 Redis（跨进程生效），Redisson 4.6 二进制格式 |
| **RMap / RMapCache** | 分布式 Hash（MapCache 支持每 entry TTL + maxIdle + Redisson struct 打包格式 + entry 事件） |
| **RSetCache** | 带 TTL/maxIdle 的集合（单 ZSET，Redisson 格式） |
| **RMultimap（Set/List）** | 一键多值；内部 ID = HighwayHash-128 大端 base64，与 Redisson 字节级一致 |
| **RList / RSet / RQueue / RDeque / RBlockingQueue / RBlockingDeque** | 集合与队列全家桶（阻塞消费双端） |
| **RDelayedQueue** | 延迟队列（ZSET + 后台迁移 + Redis 服务器时钟） |
| **RScoredSortedSet** | 有序集合（ZSET） |
| **RBucket** | 对象桶（毫秒 TTL、CAS、GetAndDelete…） |
| **RAtomicLong / RAtomicDouble** | 原子计数器（十进制字符串，CAS） |
| **RTopic / RPatternTopic** | 发布订阅（含模式订阅） |
| **RBloomFilter** | 布隆过滤器（HighwayHash-128，与 Redisson 位级对齐） |
| **RIdGenerator** | ID 生成器（批量分配缓存） |
| **RLexSortedSet** | 字典序集合（裸成员，Redisson 特例） |
| **RTransferQueue** | 跨队列原子迁移（单 Lua） |
| **RHyperLogLog / RGeo / RBitSet / RStream** | 基数估计 / 地理索引（GEOSEARCH）/ 分布式位图（Java 位序）/ 分布式日志（消费组、Pending、Claim/AutoClaim） |
| **RKeys / RBuckets / RScript** | 键空间管理（SCAN/模式删除）/ 批量桶（MSET/MGET）/ Lua 脚本执行 |
| **RBatch** | 管道批处理（`NewBatch()` → 结构写操作入队 → `Execute()` 单次往返，实测 **~7x** 加速） |
| **多拓扑** | single / cluster / sentinel（redis.UniversalClient） |
| **TUI Dashboard** | 基于 charmbracelet/bubbles 的终端监控面板 |

值编码默认 `JSONCodec`：与 Redisson `JsonJacksonCodec` 互通（超 int32 的整数自动包裹 `["java.lang.Long",v]`，解码剥离 `@class` 类型信息），可通过 `Config.Codec` 替换。

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
go test -run TestJavaInterop -v .    # Go ↔ Java Redisson 4.6.1 直接双向（需 java+mvn，首次自动编译探针）
go test -bench . -benchtime 1s .     # 基准
go vet ./...
```

wire-format 契约测试在 `wire_compat_test.go`；跨语言双向回归在 `interop_redipy_test.go`（经 redi.py——其格式已与真实 Redisson 双向实测——传递性验证）。任何破坏互操作的改动都会在这两处失败。

## 版本

见 [CHANGELOG.md](CHANGELOG.md)。v0.2.0 起 key 不再带 `redi:` 私有前缀（破坏性变更，换取互操作）。
