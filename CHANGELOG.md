# Changelog

## Unreleased

- **演进续作**：Java 探针补 AtomicDouble / SpinLock / NonReentrantLock / BoundedBlockingQueue 双向互操作；e2e 扩 Keys/Object/Batch/Queue/Stream/Topic 表面
- **全面对齐演进方案**：重写 [演进方案.md](演进方案.md)；COMPATIBILITY 增加 RedissonClient 工厂一态总表；AGENTS 交叉引用
- **阶段 A**：RateLimiter `SetRateWithKeepAlive` / config TTL 刷新；MultimapCache `StartAutoEviction`；锁族/AtomicDouble/BoundedBQ 对齐冒烟
- **阶段 B**：`RJsonBucket`/`RJsonBuckets`、`RVectorSet`、`RSearch`（模块缺失 skip）
- **阶段 C**：CSC 明确 Cluster/Sentinel 不可用边界（PARTIAL）
- **阶段 D**：`LocalCachedMapOptions.DisableNearCache`（混部关近端；二进制失效未移植）
- **阶段 E**：`Client.Config`/`GetRedisNodes`；`GetQueueTransfer` 别名；RMaps；Batch 门面既有 List/Set/Deque/AtomicDouble
- **阶段 F**：`RCircularBuffer`（ARRAY 环，≠ RingBuffer；ARRING 缺失 skip）
- **RArray**：Redis 8.8+ ARRAY（ARSET/ARGET/ARMGET/ARINSERT/…），`Client.GetArray`；命令缺失时测试 skip
- **Priority* ZSET 包装**：`GetPriorityBlockingQueue` / `GetPriorityBlockingDeque` / `GetPriorityDeque`（BZPOPMIN/MAX）；COMPATIBILITY 明确非 Java Comparator
- **RClientSideCaching 继续**：`GetClientSideCachingWithOptions` 创建独立 RESP3 CLIENT TRACKING 客户端（Destroy 关闭）；扩展 MaxMemoryBytes/DrainInterval/MaxStaleness；跨连接写后读失效回归
- **RClientSideCaching PARTIAL**：`Config.ClientSideCaching` 接线 go-redis RESP3 CSC（standalone DB0）；`GetClientSideCaching()` 工厂转发门面
- **不做清单收紧**：PRO-only 开源不可复刻；RTransaction / LCM 二进制失效 / reactive / Executor·Remote / LiveObject / RSortedSet 保持 REFUSE_AS_FAKE
- **文档**：新增英文版 [README_EN.md](README_EN.md)，中英文 README 顶部互链
- **Native / 概率结构**：`RMapCacheNative`（HPEXPIRE）、`RSet/ListMultimapCacheNative`、`RBloomFilterNative` / `RCuckooFilter` / `RTopK` / `RTDigest` / `RGcra`；命令缺失时测试 skip
- **RBinaryStream**：原始字节流（APPEND/GETRANGE/SETRANGE + channel），Java 4.6.1 双向实测
- **Redisson 4.6.1 API 补齐**：扩展 MultimapCache、ScoredSortedSet、Stream、Map/Set 键同步器、TimeSeries 及常用原子/集合/键空间方法，并补齐 companion TTL 与条件更新语义
- **同步 API 补齐**：补充集合/MapCache/Stream/锁/许可/限流/Topic/Buckets 等常用条件、批量与状态查询操作
- **锁族补齐**：新增 RFairLock（LIST+ZSET FIFO、holder 专属唤醒、watchdog）、RSpinLock、RNonReentrantLock 与 RNonReentrantFairLock；新增 RRedLock 严格多数派编排；RLock 增加限时等待与剩余 TTL，`Client.HolderID` 生成 Redisson 形 holder；FairLock 新增 Java 4.6.1 双向互斥验证
- **Map 修复与扩面**：修复 `RMap.Clear` 无参误走字段删除的空操作；RMap/LocalCachedMap 增加 `PutAll`、Fast/Replace/Keys/Values 表面，LocalCachedMap 全部写路径继续写穿并广播失效
- **RMapCache packed/容量补齐**：补齐 map-cache struct 打包、companion 清理与 entry 到期策略；新增 `SetMaxSize` / `TrySetMaxSize` 及 LRU/LFU 淘汰，对齐 options HASH 和 last-access ZSET
- **类型化读取**：`GetInto` 铺到 Bucket/Map/MapCache/List/Queue/Deque/LocalCachedMap；统一先经 Codec 剥离 Java 类型包装再绑定目标
- **List/Queue/Deque**：新增 RBoundedBlockingQueue（`redisson_bqs` 容量 companion、阻塞 Offer/Take、Poll 释放并唤醒）；List 增加 counted、索引查找、LINSERT、Trim、按索引删除与 FastSet；Queue/Deque/RingBuffer 增加常用表面
- **Set/SetCache**：RSet 增加 counted、ReadAll、随机/弹出/Move，以及 Union/Diff/Intersection 与 Store 变体；RSetCache 增加 ReadAll、随机、ContainsAll/RemoveAll/RetainAll，并保持 TTL/maxIdle companion 一致
- **Sorted/Geo/Delayed**：ScoredSortedSet 增加反向 range、首尾操作及 Intersection/Union/Diff（含 Store）；LexSortedSet 增加 rank、随机及反向范围；Geo 扩展批量与 Store；DelayedQueue 按 `Bc0Lc0` 原版 Lua 扩面
- **Atomic/Bucket/Bit/Bloom**：AtomicLong/Double 增加 GetAnd*；Bucket 增加 keep-TTL、带 TTL 条件写、Size/CompareAndDelete；BitSet 增加范围/批量/BitField；BloomFilter 增加 AddAll/ContainsAll
- **RBinaryStream**：新增不经过 Codec 的原始字节流，提供 Set/Get、范围读写、追加、顺序输入输出流与可 seek/truncate 通道；对齐 Redisson 4.6.1 的 STRING/APPEND/GETRANGE/SETRANGE wire
- **协调原语**：Semaphore 增加限时 TryAcquire；PermitExpirableSemaphore 增加批量/限时获取与 ReleaseAll；RateLimiter 增加配置读取和许可归还
- **Stream/TimeSeries**：Stream 增加显式 ID、普通阻塞读、group/consumer/pending/info 查询；TimeSeries 增加首尾 entry/timestamp、批量首尾、PollFirst/PollLast 与反向范围
- **Multimap**：新增 RSetMultimapCache/RListMultimapCache per-key TTL 核心子集（惰性淘汰）；Set/List Multimap 增加 PutAll、ReplaceValues/FastReplaceValues、ContainsValue、全量 key/value、KeySize/IsEmpty，并保留各自集合顺序语义
- **Map/Set/Multimap 键级同步器**：补齐 `GetLock` / `GetFairLock` / `GetReadWriteLock` / `GetSemaphore`，名称使用 Java `Hash.hash128toBase64(codec(key))` 与 hash-tag
- **兼容性诚实标注**：明确 RPriorityQueue（ZSET+score）和 RTransferQueue（队列迁移）不是 Java 同名结构协议，不再暗示可与 Redisson 同名 API 互操作
- **契约与 Cluster**：wire 契约 17 → 24 组；`prefixName`/`suffixName` companion 与裸名 CRC16 同槽；增加可选 `REDIS_CLUSTER_ADDRS` cluster 冒烟
- **Java 互操作计数校准**：当前实际为 24 个 `TestJavaInterop_*` 测试函数（含复合场景）；新增 RFencedLock 共享 token/互斥、RFairLock 双向获取/释放与 RBinaryStream 原始字节双向验证；CI 增加 Temurin 25 + Maven + Redis 8 的 `java-interop` job
- **工程与文档**：新增 CONTRIBUTING、GetInto godoc 示例；README/COMPATIBILITY/演进方案同步，dashboard 空状态移除过时 `redi:lock:*` 文案

## v0.2.12 (2026-08-15)

- **RRingBuffer**：定容环形缓冲（源码复刻）—— `redisson_rb:{name}` 容量 SETNX + RPUSH/LPOP 溢出淘汰（批量 LTRIM、SetCapacity 即时裁剪，Lua 同 Java 原版）、ReadOldest/ReadNewest/RemainingCapacity
- **RShardedTopic**：Redis 7+ 分片 pub/sub（SSUBSCRIBE/SSPUBLISH + SHARDNUMSUB 计数）—— cluster 下消息不占总线广播；订阅生命周期遵循 AGENTS.md 陷阱清单（listener 自消费 ack + owner Close）
- **RFunction**：Redis Functions 门面（FUNCTION LOAD(REPLACE)/DELETE/LIST/FLUSH + FCALL/FCALL_RO）
- 三个结构均 Redis 原生协议，跨语言自动互通

## v0.2.11 (2026-08-15)

- **RTimeSeries**：时间序列（源码复刻 RedissonTimeSeries）—— 序列号 member（`redisson__ts_seq` 零填充 20 位，同刻多条目共存）、每条目 TTL（TTL 分支 score=截止时刻 vs 无 TTL 分支 now+100 年 +1，两个 Lua 变体）、label mark 字节（2/3）、Get/Range 惰性过滤过期、Size=ZCARD−过期（Java 同款惰性计数）、Remove/Delete
- **Java 互操作 22 → 23 组**：Go 写 → Java 精确时间戳读回；Java 写 → Go Range 解码 + 双方 size 一致
- wire 细节修正（对照 Java 源码）：`struct.unpack('BBc0Lc0Lc0')` 首返回值是外层 type 字节（4），label mark 在 label blob 首字节（Java 用 DECODE_LABEL 剥离，Go 在 Lua 内切片）

## v0.2.10 (2026-08-15)

- **RFencedLock**：栅栏令牌锁（源码复刻 RedissonFencedLock）—— `redisson_lock_token:{name}` 计数器，acquire Lua 原版（**重入也 INCR**，成功返回 token）；`TryLockAndGetToken`/`LockAndGetToken`（阻塞，pub/sub 唤醒）/`GetToken`；防锁过期脑裂的 fencing 模式
- **RMultiLock**：多锁全有或全无（RedissonMultiLock 编排）—— 顺序获取、等待预算、失败自动回滚已获取成员
- wire 契约 16 → **17 组**（FencedLock token key + 十进制格式）
- 决策记录：Redisson RTransaction 为快照回滚语义（非 MULTI/EXEC），不冒充实现（AGENTS.md 不做清单）

## v0.2.9 (2026-08-15)

- **wire 契约测试补齐至 16 组**（`wire_compat2_test.go`）：RWLock（mode 字段 + `{id}:write` 后缀）、Semaphore/Latch（裸名计数器 + 字面 channel 名 + 归零消息 "0"）、PermitExpirableSemaphore（`{name}:timeout` zset + 绝对到期 score）、RStream（field 名与值双 codec 编码）、ReliableTopic（field `m` + 每订阅者独立组 + timeout 活性）、LongAdder（topic/counter/semaphore 伴生键协同实测）、BitSet（MSB-first 原始字节）、LocalCachedMap（数据层 = RMap 格式）
- 动机：Java 互操作测试在 CI（无 JVM）skip，契约测试是 CI 上唯一的 wire 防线
- 修复测试竞态：pub/sub 断言前先消费订阅 ack（发布不得先于订阅注册）

## v0.2.8 (2026-08-15)

- **RLongAdder / RDoubleAdder**：高争用分布式计数器，源码复刻 Redisson BaseAdder 协议 —— Add 零网络本地缓冲；`Sum()` 发布 `1:<id>` 到 `{name}:adder-topic，各存活实例（**含 Java**）把本地总额 flush 进 `{name}:{id}:counter` 并释放 `{name}:{id}:semaphore` 栅栏，请求者收齐后 GETDEL 汇总（非破坏）；`Reset()` 广播 `0:<id>` 清全部缓冲
- **Java 互操作 21 → 22 组**：Go+Java 各持本地缓冲，任一方 Sum 收齐双方写入（100+23=123；+7→130 双向一致）
- 修复两个订阅生命周期 bug：就绪等待与消费流双消费者竞态（吞首条消息）；`PubSub.Receive` 不响应 ctx 取消导致 Destroy 死锁（owner 主动 Close 再 Wait）
- **AGENTS.md**：仓库协作约定（铁律/流程/测试约定/不做清单），沉淀全部踩坑经验
- 修复 example 自清理漏 companion 键（rate limiter 窗口跨运行残留）

## v0.2.7 (2026-08-15) — 限流器双重正确性修复 + 工程质量

### 修复（RRateLimiter，生产级隐患）
- **过期归还算术错误**：从 redi.py 移植的手写字节运算按大端读 permits 尾部、而写入按小端打包 —— 每个过期许可归还 **16777216** 而非 1（实测 `{name}:value` 灌到 16777210）。改用 Redisson 原版 `struct.pack('Bc0I',...)/struct.unpack('Bc0I',...)`（与 Java 字节级一致、读写自洽）
- **重复获取塌缩**：TryAcquire 传固定 client id → 相同 member 被 ZADD 去重覆盖，3 次获取只剩 1 条（2 个许可永久泄漏）。改为每次获取生成随机 16 字节 id（对齐 Java `generateIdArray()`）
- **AvailablePermits 池缩水**：裸 `ZRemRangeByScore` 删除过期许可但不归还池 → 读操作会永久缩小窗口。改用共享 purge Lua（带回池）
- 新增回归测试 `TestRRateLimiter_ExpiredPermitsReturnToPool`（过期后精确归还 rate 个）

### 工程质量
- **golangci-lint 清零**（13 项：errcheck 7 / staticcheck 3 / 死代码 3）并纳入 CI；checkout@v5 + setup-go@v6
- **godoc 示例测试**（example_test.go，5 个可运行示例：NewClient/RLock/RAtomicLong/RRateLimiter/RBatch，自带键清理可重跑）

## v0.2.6 (2026-08-15)

- **Codec Encode 快路径**：标量（string/int/float/bool）单次序列化直达，不再走 marshal→unmarshal→rewrap 三段管线（基准：字符串 103ns、int 63ns，原均 ~2.4µs，**~25x**）；复合值仍递归类型包装
- **嵌套 wire 实测**：深层复合结构（map>array>map>array 任意深度递归 @class/ArrayList 包装）经真实 Redisson 4.6.1 读取验证（第 21 组 Java 互操作用例）
- **RPriorityQueue**：ZSET 优先级队列（Offer 带分数/Poll 低分先出/Peek/PeekScore/Remove）

## v0.2.5 (2026-08-15)

- **RLocalCachedMap**：近端缓存 Map —— 本地命中零往返、写穿 Redis、Put/Remove/Clear 跨实例失效广播（channel `{name}:inval`，Go 内部协议）、本地容量上限（`SetLocalCacheLimit`）、`ClearLocalCache`/`CachedKeys`/`Destroy`
- 数据层完全 RMap wire 格式（Java 双向读写实测）；Java 失效协议（keyHash 二进制 + 更新日志 + Java 序列化）未复刻，COMPATIBILITY 诚实标注
- 修复同 client 多实例 instanceID 冲突（加实例级随机后缀）

## v0.2.4 (2026-08-15)

- **RPermitExpirableSemaphore**：每许可独立租约/到期（STRING 计数 + ZSET `{name}:timeout` + `redisson_sc` 唤醒）；Acquire 阻塞唤醒、Release 验证、UpdateLeaseTime/LeaseTime；过期回收在 acquire 与读路径惰性执行（Lua 原子）
- **RReliableTopic**：Stream 可靠主题 —— 源码验证 Redisson 语义为**每订阅者独立消费组**（广播；redi.py 单组多 consumer 实为负载均衡）、XADD field `m`、回调返回后才 XACK（崩溃重投递）、`{name}:timeout` 活性 zset + watchdog/3 刷新、晚订阅者从头重放
- Java 互操作 18 → **19 组**（PES：Java 租借/释放 ↔ Go 可见/获取共享池；ReliableTopic：Go 发布 → Java 监听收到 + 双语订阅组各收全量）
- 修复：`ZAddXX` 返回新增计数而非更新成功（UpdateLeaseTime 误判）

## v0.2.3 (2026-08-15)

- **RStream**：Redis Stream 全家 —— Add（含 MAXLEN 裁剪）/ReadRange/ReadReverse/Len/Trim(MAXLEN+MINID)/Remove/CreateGroup(BUSYGROUP 幂等)/DeleteGroup/CreateConsumer/RemoveConsumer/ReadGroup/PendingRange/Ack/Claim/AutoClaim；field 名与值均 codec 编码（Redisson RStream 同款）
- **修复 go-redis 陷阱**：`XReadGroup Block:0` = 永久阻塞（库内 `Block>=0` 恒发 BLOCK）→ block<=0 时内部传 -1 省略
- Java 互操作 17 → **18 组**（RStream：Java 写→Go 组读、跨语言 Ack 清 Pending、Go 写→Java 新组读全史）

## v0.2.2 (2026-08-15)

- **RHyperLogLog**：PFADD/PFCOUNT/PFMERGE + 联合计数/合并
- **RGeo**：GEOADD/GEOSEARCH/GEOPOS/GEODIST/GEOHASH + TryAdd（Lua 原子 NX；GEOSEARCH 替代 Redis 8 已移除的 GEORADIUS）
- **RBitSet**：原生位序（Java RedissonBitSet 一致，源码验证 toByteArray `1<<(7-i%8)`，无 redi.py 的位反转错误）；Length/Cardinality/BITOP And/Or/Xor/Not + 字节数组往返
- Java 互操作 14 → **17 组**（HLL 混合计数、Geo 双向 pos/dist、BitSet 双向位与 length）
- 规避 go-redis 坑：GeoSearchLocation 无 WITH* 标志时解析崩溃 → 恒带 WITHCOORD

## v0.2.1 (2026-08-15)

- **Java 直接互操作扩到 14 组用例**（新增 RWLock / RList / RScoredSortedSet / RLexSortedSet / RBucket / RMapCache / RDelayedQueue 双向）
- **RDelayedQueue 按 Redisson 4.6.1 真实布局重写**（Java 实测发现 redi.py 为旧格式）：`redisson_delay_queue_timeout:{name}` ZSET + `redisson_delay_queue:{name}` LIST 双结构、`Bc0Lc0` struct-packed member（长度前缀**小端 8 字节**）、原版迁移 Lua（unpack→RPUSH+LREM+ZREM）；Java↔Go 双向迁移实测通过

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

### 追加（同日第八轮，= v0.2.1）
- **Java 直接互操作扩到 14 组用例**（新增 RWLock / RList / RScoredSortedSet / RLexSortedSet / RBucket / RMapCache / RDelayedQueue 双向）
- **RDelayedQueue 按 Redisson 4.6.1 真实布局重写**（Java 实测发现 redi.py 为旧格式）：`redisson_delay_queue_timeout:{name}` ZSET + `redisson_delay_queue:{name}` LIST 双结构、`Bc0Lc0` struct-packed member（长度前缀**小端 8 字节**）、原版迁移 Lua（unpack→RPUSH+LREM+ZREM）；Java↔Go 双向迁移实测通过

## v0.1.0
初始版本：RLock/RMap/RList/RSet/RAtomicLong/RQueue + TUI dashboard。
