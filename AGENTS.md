# AGENTS.md — redi.go 协作约定

> 面向在此仓库工作的 AI 编码代理与人类贡献者。先读本文，再动代码。

## 项目定位

Pure Go 复刻 Redisson（面向 Redis 8.x）。**wire format（key 布局/Lua 算法/值编码）必须与 Java Redisson 4.6.1 + JsonJacksonCodec 互操作**，并兼容 redi.py（Python 版，注意其已知偏差，见下）。

## 命令

```bash
go vet ./... && go test -race ./... -count=1 -timeout 600s   # 全量（本机需 Redis localhost:6379，缺失自动 skip）
golangci-lint run ./... --timeout 300s                        # CI 门禁，必须 0 issues
go test -run TestInterop -v .          # Go ↔ redi.py（需 Python + C:/GitHub/redi.py）
go test -run TestJavaInterop -v .      # Go ↔ Redisson 4.6.1（需 java+mvn；探针首次自动编译，之后单 JVM 复用）
go test -bench . -benchtime 1s -run XXX_none .   # 基准
```

## 铁律（违反即回归）

1. **编码层（codec.go）**：map 必须加 `@class:"java.util.LinkedHashMap"`、slice 必须包 `["java.util.ArrayList",[...]]`、超 int32 整数必须包 `["java.lang.Long",v]` —— JsonJacksonCodec default typing 读裸 JSON 对象会抛错（Java 实测）。解码端剥离一切包装。任何编码改动必须过 `wire_compat_test.go`。
2. **禁止手算二进制布局**：Lua 内一律 `struct.pack/struct.unpack`。教训：RRateLimiter 曾因字节端序手算（写 LE 读 BE）导致每个过期许可归还 16777216；DelayedQueue 的 `Bc0Lc0` 长度前缀是**小端 8 字节**（hex 实测）。
3. **每次获取必须随机 id**：RRateLimiter/PermitExpirableSemaphore 等用 member 去重的结构，固定 id 会因 ZADD 去重塌缩计数（曾泄漏许可）。对齐 Java `generateIdArray()`。
4. **companion key 一律 hash-tag**：`prefixName`/`suffixName`（`prefix:{name}` / `{name}:suffix`），保证 cluster 同槽。
5. **订阅不可批处理**：结构绑定 RBatch（pipeline）时，Pub/Sub 必须走 `rObject.subscribe/psubscribe`（固定路由真实连接）。
6. **go-redis 陷阱清单**（已踩）：
   - `XReadGroup Block:0` = 永久阻塞（库对 `Block>=0` 恒发 BLOCK）→ 非阻塞传 `-1`
   - `ZAddXX` 返回**新增**数（更新已有 member 返回 0）→ 先 ZScore 查存在
   - `GeoSearchLocation` 无 WITH* 标志时解析崩溃 → 恒带 WITHCOORD
   - `PubSub.Receive` 阻塞时不响应 ctx 取消，只有 `Close()` 能唤醒 → 需要 goroutine 退出时由 owner 调 Close 再 Wait（勿把 Close 放 goroutine 自己的 defer + 外面 wg.Wait，死锁）
   - Lua EVAL_VOID（无返回）以 `redis.Nil` 错误形式返回 → 忽略之

## 新增结构的流程

1. **先查 Java 源码**（`C:/GitHub/redisson/.../RedissonXxx.java`）：key 名、channel 名、Lua 原文、消息格式。**不轻信 redi.py**（其 DelayedQueue 是 4.x 前旧格式、RWLock 曾拆双 key、RateLimiter 端序错、LocalCachedMap 语义偏差——均已实测证伪）。
2. 实现：嵌入 `rObject`，走 `c.codec` 编解码，Lua 用 `redis.NewScript`。
3. `Client` 加工厂方法（Redisson 风格名 + 必要时旧别名）。
4. 测试：单元测试 + （涉及 wire 时）`wire_compat_test.go` 契约断言 + Java interop 用例（`interop/java-probe/RedigoProbe.java` 加命令 → `interop_java*_test.go` 断言）。
5. 文档四处同步：README 特性表、COMPATIBILITY.md 矩阵行、CHANGELOG、演进方案.md 执行状态。

## 测试约定

- 测试键用 `uniqueKey(t, ...)`；cleanup 用 `interopCleanup`（裸名）或 `interopCleanupPattern`（SCAN 通配，multimap/limiter 等 companion 键以 `{` 开头时必须用后者）。
- 互操作测试在依赖缺失（python/java/mvn/redi.py）时必须 skip 而非 fail。
- 示例（example_test.go）自带键清理，可 `-count>1` 重跑。
- 阻塞类断言用 `eventual(t, timeout, cond)`，勿用固定 sleep。

## 已知不做（勿重复提案）

- RSortedSet（LIST+Comparator 版）—— 勿用 ZSET 冒充。
- Java LocalCachedMapInvalidation 二进制失效协议（keyHash + 更新日志 + Java 序列化）—— Go 失效广播为内部协议，COMPATIBILITY 已标注。
- reactive/RxJava 范式、手写连接池、RExecutorService/RRemoteService 的 Java 互操作。
