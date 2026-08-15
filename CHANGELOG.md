# Changelog

## v0.2.0 (2026-08-15) — 全面演进：正确性 + 互操作 + 扩面

### 破坏性变更
- **key 去 `redi:` 私有前缀**：所有结构直接使用裸 `{name}`（Redisson NameMapper=identity 语义），换取跨语言互操作。旧版本数据不迁移。
- `RMap.Get/RQueue.Poll/RList.Get` 等读取方法返回 `(any, error)`（codec 解码），缺失时 `(nil, nil)`（Redisson null 语义），不再暴露 `redis.Nil`。
- `RLock` 语义对齐 Redisson：`ttl>0` = 固定租约不续期；`ttl<=0` = watchdog 模式（lease = `Config.LockWatchdogTimeout`，默认 30s，每 lease/3 续期）。
- `RAtomicLong.Set` 返回旧值；`Get` 缺 key 返回 0（原返回 redis.Nil）。
- 依赖变更：移除 `gotrycatch`、`GoExecutors`；新增 `minio/highwayhash`（BloomFilter）。

### 正确性修复（原实现的静默 bug）
- watchdog 固定 10s 续期间隔导致短 TTL 锁必然过期（C1）
- 重入 Lock 替换 cancelWatchdog 导致续期丢失（C2）
- Unlock 在所有权验证前取消 watchdog，非持有者可杀死持有者续期（C3）
- `RQueue.Take` 亚秒超时截断为 0 → 永久阻塞（P0-8）
- `RAtomicLong.CompareAndSet` 缺 key 时字符串比较恒失败（P0-7）
- `Client.Close` 不停止后台 goroutine（P0-9）

### 互操作（wire 对齐 Redisson / redi.py）
- RLock：HASH 裸名 + `redisson_lock__channel:{name}` 唤醒（消息 0），unlock/renew Lua 对齐
- 值编码层 `Codec`（默认 JSON，Long 包裹 + `@class` 剥离）
- RRateLimiter：config HASH + `{name}:value` + `{name}:permits` 二进制 member（LE uint32）
- RDelayedQueue：`redisson_delay_queue:{name}` ZSET + channel + Redis 服务器时钟
- RBloomFilter：HighwayHash-128（Redisson 固定 KEY）+ `{name}:config` + Java 截断取整
- RMapCache：`<dQ` struct 打包 + `redisson__timeout__set`/`redisson__idle__set`
- RCountDownLatch channel、RSemaphore channel、RReadWriteLock mode 字段均对齐
- 新增 `wire_compat_test.go` 锁定全部 wire 布局

### 新结构（14 个）
RReadWriteLock、RSemaphore、RCountDownLatch、RRateLimiter、RDelayedQueue、RTopic、RPatternTopic、RBloomFilter、RIdGenerator、RMapCache、RBucket、RDeque、RBlockingQueue、RScoredSortedSet、RAtomicDouble

### 架构
- `rObject` 基类统一 RObject/RExpirable 表面（Delete/Exists/Touch/Rename/Expire/…）
- `redis.UniversalClient`：single / cluster / sentinel 三拓扑
- Redisson 风格工厂别名（GetMap/GetLock/GetSemaphore…）与旧名并存
- gotrycatch/GoExecutors 全部移除（原生 goroutine + 直接返回错误），主库源码 -35% 样板
- 源码行数从 ~1,100 降至 ~4,300 行（结构数 6 → 21）

### 工程
- GitHub Actions CI（redis:8 service + race detector）
- README 重写、COMPATIBILITY.md、本 CHANGELOG
- **Go ↔ redi.py 双向互操作回归**（`interop_redipy_test.go` + `interop/redipy_probe.py`，8 组用例，经 redi.py 传递性对齐真实 Redisson）
- 基准测试 `benchmark_test.go`（AtomicLong / Map / Lock / Bucket）

### 追加（同日第二轮）
- **RMapCache entry 事件**：`AddListener`/`RemoveListener`（created/updated/removed/expired），channel `redisson_map_cache_{kind}:{name}`，消息为 Redisson 实测的 8 字节 LE 长度前缀 + codec 段格式（wire 契约测试锁定）
- **RReadWriteLock watchdog**：`ttl<=0` 自动续期（lease/3），Unlock 验权成功且计数归零后才停止

### 追加（同日第三轮）
- **RSetCache**：单 ZSET（score=绝对过期时刻，无 TTL=MaxInt64）+ idle ZSET + Contains 刷新 idle（Redisson RedissonSetCache 格式）
- **RBlockingDeque**：双端阻塞消费（TakeFirst/TakeLast/Poll{First,Last}WithTimeout）
- **RMultimap（RSetMultimap/RListMultimap）**：HASH + `{name}:{id}` 集合；内部 ID 与 Java `Hash.hash128toBase64` 字节级一致（HighwayHash-128 大端 + 无填充 base64），redi.py 双向互操作实测

### 追加（同日第四轮）
- **Go ↔ Java Redisson 4.6.1 直接双向互操作**：`interop/java-probe/`（单 JVM REPL 探针，Redisson + JsonJacksonCodec）+ `interop_java_test.go` 7 组用例全过（锁/Map/AtomicLong/Bloom/RateLimiter/Semaphore/Latch）
- **codec 缺口修复（Java 实测发现）**：JsonJacksonCodec default typing 读裸 JSON 对象/数组抛 "missing type id" → 编码端对 map 加 `@class:"java.util.LinkedHashMap"`、slice 包 `["java.util.ArrayList",[...]]`；解码端照旧剥离（兼容裸 JSON）
- **RLexSortedSet**：字典序集合（裸成员存储，Redisson 跳过 codec 的特例）
- **RTransferQueue**：LPOP+RPUSH 单 Lua 原子迁移 + TryTransfer 阻塞等待

### 追加（同日第五轮）
- **RKeys**：键空间管理（DBSIZE/CountExists/RandomKey/Copy/Type/ForEachKey SCAN 迭代/DeleteByPattern/UnlinkByPattern/FlushDB）
- **RBuckets**：批量桶（Get=MGET / Set=MSET+批量 TTL pipeline / TrySet=MSETNX 全有或全无）
- **RScript**：Lua 执行（Eval/EvalSha/ScriptLoad/ScriptExists + Boolean/Integer/Status/Value 返回类型转换，Lua nil → Go nil）
- 工厂：`GetKeys()` / `GetBuckets()` / `GetScript()`（无 name 的 facade）

### 追加（同日第六轮）
- **RBatch**：管道批处理 —— `Client.NewBatch()` 返回绑定 pipeline 的结构 facade（Map/Bucket/List/Set/Queue/Deque/AtomicLong/AtomicDouble/ScoredSortedSet），`Execute()` 单次往返刷入（基准：10 Put 顺序 2.16ms → 批量 0.30ms，**7.1x**）
- 架构：`rObject` 增加 `cmds redis.Cmdable` 覆写点（go-redis Pipeliner 与 Client 同接口，一个字段打通批处理）；订阅类操作固定路由真实连接（订阅不可批处理）

### 追加（同日第七轮）
- **Dashboard 三页签**：INFO（服务器指标）/ Locks（持锁者、重入计数×N、TTL，`Run(client.Redis(), patterns...)` 指定扫描范围）/ Limiters（限流器 rate/interval/剩余许可）；`1/2/3` 或 `←/→` 切换
- dashboard 首次引入单元测试（parseInfo/buildRows/buildLockRows/buildLimiterRows/formatHolders）

## v0.1.0
初始版本：RLock/RMap/RList/RSet/RAtomicLong/RQueue + TUI dashboard。
