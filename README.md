# redi.go

一个用 Pure Go 复刻 Redisson 的 Redis 客户端库，面向 **Redis 8.x**。

## 特性

| 特性 | 说明 |
|---|---|
| **RLock** | 分布式可重入锁，含 watchdog 自动续期 |
| **RMap** | 分布式 Hash Map（基于 Redis Hash） |
| **RList** | 分布式列表（基于 Redis List） |
| **RSet** | 分布式集合（基于 Redis Set） |
| **RAtomicLong** | 分布式原子计数器（CAS 支持） |
| **RQueue** | 分布式 FIFO 队列，支持阻塞消费 |
| **TUI Dashboard** | 基于 [charmbracelet/bubbles](https://github.com/charmbracelet/bubbles) 的终端监控面板 |

## 依赖

- [redis/go-redis v9](https://github.com/redis/go-redis) — Redis 驱动
- [linkerlin/gotrycatch](https://github.com/linkerlin/gotrycatch) — 结构化异常处理
- [linkerlin/GoExecutors](https://github.com/linkerlin/GoExecutors) — 并发执行器（watchdog 管理）
- [charmbracelet/bubbles](https://github.com/charmbracelet/bubbles) — TUI 组件

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

    // --- RLock ---
    lock := client.GetRLock("my-lock")
    if err := lock.Lock(ctx, "instance-1", 30*time.Second); err != nil {
        log.Fatal(err)
    }
    defer lock.Unlock(ctx, "instance-1")

    // --- RMap ---
    m := client.GetRMap("my-map")
    _ = m.Put(ctx, "key", "value")
    val, _ := m.Get(ctx, "key")
    fmt.Println(val) // value

    // --- RAtomicLong ---
    counter := client.GetRAtomicLong("hits")
    n, _ := counter.IncrementAndGet(ctx)
    fmt.Println(n) // 1
}
```

## TUI Dashboard

```go
import "github.com/linkerlin/redi.go/dashboard"

// Blocks until the user presses 'q'.
if err := dashboard.Run(client.Redis()); err != nil {
    log.Fatal(err)
}
```

## 开发

```bash
go test ./...          # 需要本地 Redis（localhost:6379）
go build ./...
go vet ./...
```
