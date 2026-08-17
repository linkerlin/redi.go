# 兼容性矩阵（COMPATIBILITY）

> 与 Java Redisson（`JsonJacksonCodec`，无类型信息）及 redi.py 的互操作状态。
> wire 依据：Redisson 4.6.x 源码 + Java 实测 + redi.py 双向实测结论。
> Go 侧契约测试：`wire_compat_test.go` + `wire_compat2_test.go` + `rfairlock_test.go` + `rbinarystream_test.go`（**24 组，覆盖自定义 wire 结构的 key/channel/编码布局——CI 无 JVM 时由它们守护 wire**）；**redi.py 双向回归：`interop_redipy_test.go`；Java（Redisson 4.6.1）直接双向回归由单 JVM REPL 探针 `interop/java-probe/` 驱动，共 **48 个 `TestJavaInterop_*` 测试函数**（含 Topic / Script / Buckets / Keys / Set / Queue / Multimap / DoubleAdder 等）；CI 有独立 `java-interop` job**。

## 重要 wire 事实（Java 实测）

1. **JsonJacksonCodec default typing**：读裸 JSON 对象/数组报错（missing type id）。redi.go 编码端补齐：
   - map → `{"@class":"java.util.LinkedHashMap",...}`
   - slice → `["java.util.ArrayList",[...]]`
   - 超 int32 整数 → `["java.lang.Long",v]`
   - 解码端剥离以上全部（裸 JSON 也能读）。
   （redi.py 写裸对象时 Java 读会炸 —— 本库编码端已补齐该缺口。）

2. **DelayedQueue 双 LIST 布局（4.6.1 实测，redi.py 实现为旧版不兼容）**：
   - ZSET `redisson_delay_queue_timeout:{name}`：member = `struct.pack('Bc0Lc0', 8, randomId8B, encLen, encoded)`（**长度前缀小端 8 字节**，hex 实测确认）
   - LIST `redisson_delay_queue:{name}`：同一 packed member（迁移时 LREM 用）
   - 迁移 Lua：到期 member unpack 后 RPUSH 到目标 `{name}` + LREM + ZREM
   - ⚠ redi.py 的单 ZSET `redisson_delay_queue:{name}` 是 4.x 之前的格式，与 Java 4.6.1 **不互操作**（Java 实测 WRONGTYPE）

## ✅ 可互操作（wire 对齐）

| 结构 | Redis 布局 | 验证状态 |
|------|-----------|---------|
| RLock | HASH `{name}`，field=holder id，channel `redisson_lock__channel:{name}`（unlock 消息 0），renew/unlock Lua 同 Redisson | **Redisson 4.6.1 直接双向实测 ✅** |
| RSpinLock | 与 RLock 相同的 HASH/holder/watchdog wire；获取侧改为短退避自旋，不依赖 pub/sub 唤醒 | **Redisson 4.6.1 双向互斥实测 ✅** |
| RNonReentrantLock | RLock 同形 HASH，单 holder field 的计数恒为 1，channel `redisson_lock__channel:{name}`；同 holder 不允许再次获取 | **Redisson 4.6.1 双向互斥/拒绝重入实测 ✅** |
| RNonReentrantFairLock | RFairLock 同形 HASH/LIST/ZSET；同 holder 重入返回 `ErrLockReentrant`，其他 holder 保持 FIFO | RedissonNonReentrantFairLock 4.6.1 源码对照 + 拒绝重入/FIFO handoff 单元测试 ✅；尚无单独 Java 探针 |
| RFairLock | RLock HASH + LIST `redisson_lock_queue:{name}` + ZSET `redisson_lock_timeout:{name}`；等待者订阅 `redisson_lock__channel:{name}:{holderId}`，unlock 仅唤醒队首；try/acquire/unlock Lua 同 RedissonFairLock 4.6.1 | wire 契约 + FIFO 单元测试 + **Redisson 4.6.1 双向互斥/释放实测 ✅** |
| RReadWriteLock | 单 HASH + `mode` 字段（read/write/read-write），读 field=`{id}` 写 field=`{id}:write`，channel `redisson_rwlock:{name}`（读释放消息 1 / 写释放 0） | **Redisson 4.6.1 直接双向实测 ✅**（读写互斥、共享读、释放可见） |
| RSemaphore | STRING 计数器 + `redisson_sc:{name}` 唤醒 | **Redisson 4.6.1 直接实测 ✅** |
| RCountDownLatch | STRING 计数 + DECR/DEL/PUBLISH（channel `redisson_countdownlatch__channel__{name}`，消息 0/1） | **Redisson 4.6.1 countDown 唤醒 Go Await ✅** |
| RRateLimiter | HASH `{name}`（rate/interval/keepAliveTime/type 枚举序数）+ `{name}:value` + ZSET `{name}:permits`（member=`struct.pack('Bc0I', len, id16B, permits)`，**每次获取随机 id**——固定 id 会因 ZADD 去重塌缩；过期归还走 `struct.unpack`，勿用字节手算——endianness 曾致 16M 归还） | **Redisson 4.6.1 共享窗口实测 ✅** + 过期归还回归测试 |
| RMap / RList / RSet / RQueue / RDeque / RBlockingQueue | 裸名 + JSON 值编码（JsonJacksonCodec 互通，含 @class/Long/ArrayList 包装） | **Redisson 4.6.1 String/Long/嵌套对象双向实测 ✅** |
| RBoundedBlockingQueue | LIST `{name}` + 容量 STRING `redisson_bqs:{name}` + channel `redisson_sc:redisson_bqs:{name}`；Offer 原子 DECR+RPUSH，Poll 原子 LPOP+INCR+PUBLISH | Redisson 4.6.1 双向实测 ✅（capacity/offer/poll） |
| RScoredSortedSet | ZSET + JSON member | **Redisson 4.6.1 score/rank 双向实测 ✅** |
| RLexSortedSet | ZSET + 字典序操作，成员**裸存储**（跳过 codec，Redisson 特例）；含 rank/random 与正反向 rank/lex range | **Redisson 4.6.1 add/range 双向实测 ✅** |
| RBucket | 裸名 + JSON 值（毫秒 TTL） | **Redisson 4.6.1 双向实测 ✅** |
| RBinaryStream | 裸名 Redis STRING + 原始字节（`ByteArrayCodec`，不经过配置 Codec）；APPEND/GETRANGE/SETRANGE，缩短截断以 GETRANGE+SET 重写 | wire 契约 + **Redisson 4.6.1 set/get/channel 偏移写双向实测 ✅** |
| RMapCache | `<dQ` struct 打包值 + timeout/idle ZSET；容量配置 HASH `{name}:redisson_options`，LRU/LFU 排序 ZSET `redisson__map_cache__last_access__set:{name}`；Put/FastPut 原子容量淘汰并清理 companion；entry 事件走 `redisson_map_cache_{kind}:{name}` | wire 契约 + max-size surface 回归 + **Redisson 4.6.1 双向读写实测 ✅**；事件通道 Go 内消费，跨语言监听非目标 |
| RMapCacheNative | 普通 HASH 值（无 packed 头）+ `HPEXPIRE`/`HPEXPIREAT`/`HPTTL` 字段过期（Redis ≥7.4） | RedissonMapCacheNative 4.6.1 Lua 对照 + 单元测试（命令缺失自动 skip） |
| RSetMultimapCacheNative / RListMultimapCacheNative | RMultimap 布局；`ExpireKey` = `HPEXPIRE` 索引 field + `PEXPIRE` 集合键 | RedissonMultimapCacheNative 源码对照 + 单元测试（命令缺失自动 skip） |
| RBloomFilterNative / RCuckooFilter / RTopK / RTDigest / RGcra | Redis `BF.*` / `CF.*` / `TOPK.*` / `TDIGEST.*` / `GCRA`（元素经 codec 编码，除 TDigest/GCRA） | Redis 原生命令门面 + 冒烟测试（命令缺失自动 skip） |
| RDelayedQueue | ZSET `redisson_delay_queue_timeout:{name}` + LIST `redisson_delay_queue:{name}`（struct-packed member；Contains/Remove/ReadAll/Clear 均用 `struct.unpack` 并同步双结构） | **Redisson 4.6.1 双向迁移实测 ✅** + pending surface 回归 |
| RAtomicLong / RAtomicDouble | 裸名十进制字符串（StringCodec 语义） | wire 契约 + **AtomicLong/AtomicDouble：Redisson 4.6.1 双向实测 ✅** |
| RBloomFilter | 位图裸名 + `{name}:config` HASH，HighwayHash-128（Redisson 固定 KEY），Java 截断/半上取整公式 | **Redisson 4.6.1 位/公式双向实测 ✅** |
| RIdGenerator | `{name}` 计数 + `{name}:allocation` | wire 契约 ✅ |
| RSetCache | 单 ZSET（member=值，score=绝对过期时刻；无 TTL = MaxInt64）+ idle ZSET `redisson__idle__set:{name}`；ReadAll/随机/ContainsAll/RemoveAll/RetainAll 先清过期并同步 companion | wire 契约 + surface 回归 ✅ |
| RMultimap（Set/List） | HASH `{name}`（field=JSON key → 内部 ID）+ 集合 `{name}:{id}`；ID = HighwayHash-128 大端 + 无填充 base64（Java `Hash.hash128toBase64`） | wire 契约 + **redi.py 内部 ID 字节级 + 双向读写实测 ✅** |
| RSetMultimapCache / RListMultimapCache | RMultimap 布局 + ZSET `{name}:redisson_{set\|list}_multimap_ttl`（member=JSON key，score=绝对到期毫秒）；过期读取惰性删除 HASH/collection/ZSET | Redisson 4.6.1 Lua/命名对照 + set/list TTL/顺序/淘汰单元测试 ✅ |
| Map/Set/Multimap key 同步器 | `{objectName}:{Hash.hash128toBase64(codec(key))}:{lock\|fairlock\|rw_lock\|semaphore}`；所有 companion 保持同一 hash slot | RedissonObject.getLockByMapKey/getLockByValue 4.6.1 源码对照 + 名称契约测试 ✅ |
| RBlockingDeque | 双端阻塞消费（BLPOP/BRPOP） | 单元测试 ✅ |
| RTransferQueue | **非 Java 同名协议**：Go 为 LPOP→RPUSH 队列间原子迁移；Redisson RTransferQueue 依赖 RemoteService 消费者注册与直接交付，**不可互操作** | 单元测试 ✅（自有语义） |
| RHyperLogLog | PFADD/PFCOUNT/PFMERGE（codec 编码成员；sketch 为 Redis 原生） | **Redisson 4.6.1 混合计数实测 ✅** |
| RGeo | GEOADD（含 XX/批量）/GEOSEARCH/GEOSEARCHSTORE/GEOPOS/GEOHASH/GEODIST（codec 编码成员；Redis 8 兼容） | **Redisson 4.6.1 双向 pos/dist 实测 ✅** + bulk/store 回归 |
| RBitSet | 原生 GETBIT/SETBIT 位序（与 Java RedissonBitSet 一致，无位反转；MSB-first 字节数组） | **Redisson 4.6.1 双向位/cardinality/length 实测 ✅** |
| RStream | XADD/XRANGE/XREADGROUP/XPENDING/XACK/XCLAIM/XAUTOCLAIM；field 名与值均 codec 编码（Redisson RStream 同款）；消费组为 Redis 原生 | **Redisson 4.6.1 双向实测 ✅**（Java 写→Go 组读、跨语言 Ack、Go 写→Java 新组读全史） |
| RPermitExpirableSemaphore | STRING `{name}` 计数 + ZSET `{name}:timeout`（member=许可 ID，score=到期时刻）+ channel `redisson_sc:{name}`；过期回收在 acquire 与读路径惰性执行（Lua） | **Redisson 4.6.1 共享许可池实测 ✅**（Java 租借→Go 可见 0；Java 释放→Go 获取） |
| RReliableTopic | STREAM `{name}`（XADD field **`m`**）+ **每订阅者独立消费组**（组名=subscriberId，Java 语义：各组收全量；redi.py 单组多 consumer 实为负载均衡，语义错误）+ ZSET `{name}:timeout` 活性（watchdog/3 刷新）；回调返回后才 XACK（崩溃重投递） | **Redisson 4.6.1 实测 ✅**（Go 发布→Java 监听收到；Go/Java 订阅组各收全量） |
| RLocalCachedMap | 数据层 = RMap wire 格式（✅ 跨语言读写）；**失效广播为 Go 内部协议**（channel `{name}:inval`，JSON 消息）——Java 的 LocalCachedMapInvalidation（keyHash 二进制数组 + 更新日志 + Java 序列化对象）未复刻，Java 写入不会实时失效 Go 本地缓存（Go 读有 Redis 兜底） | 数据层 **Redisson 4.6.1 双向读写实测 ✅**；失效广播 Go↔Go ✅ / Java↔Go ❌（诚实标注） |
| RPriorityQueue | **非 Java 同名协议**：Go 为 ZSET+score 优先队列；Redisson RPriorityQueue 为 LIST+Comparator，**不可与 `getPriorityQueue()` 互操作**（可按 raw ZSET / RScoredSortedSet 族访问） | 单元测试 ✅（自有语义） |
| RPriorityBlockingQueue / RPriorityBlockingDeque / RPriorityDeque | ZSET + BZPOPMIN/BZPOPMAX / 双端非阻塞弹出；**非 Java Comparator 协议** | 单元测试 ✅（自有语义） |
| RArray | Redis 8.8+ ARRAY（ARSET/ARGET/ARINSERT/…）；codec 编码元素；命令缺失时测试 skip | 单元测试 ✅（命令可用时） |
| RClientSideCaching | **PARTIAL**：`Config.ClientSideCaching` 或 `GetClientSideCachingWithOptions` → go-redis RESP3 CLIENT TRACKING（standalone DB0，可配置 MaxEntries/MaxMemory/DrainInterval/MaxStaleness）；工厂转发；非 Java 读代理/EvictionPolicy | 失效回归（跨连接写后读可见）✅ |
| RLongAdder / RDoubleAdder | Redisson BaseAdder 协议（源码复刻）：channel `{name}:adder-topic`（消息 `1:<id>`=SUM / `0:<id>`=CLEAR，明文）+ flush 目标 `{name}:{id}:counter`（INCRBY/INCRBYFLOAT）+ 栅栏 `{name}:{id}:semaphore`（publish 返回订阅数 n，请求者 acquire n 后 GETDEL 汇总）；请求者自身订阅并响应，非破坏性 Sum | **Redisson 4.6.1 跨语言实测 ✅**（Go 加 100 + Java 加 23 → 双方 Sum 均 123；再 +7 → 双方 130，非破坏） |
| RFencedLock | RLock 布局 + `redisson_lock_token:{name}` 计数器；acquire Lua 同 Redisson 原版（`INCR` token —— **重入也递增**；成功返回 `{-1,token}`）；GetToken 为十进制 GET（StringCodec） | wire 契约 + **Redisson 4.6.1 跨语言 token/互斥实测 ✅** |
| RMultiLock | 纯客户端编排（成员即普通 RLock） | 单元测试 ✅（全有或全无 + 失败回滚） |
| RRedLock | 纯客户端编排；N 个独立 RLock 中获取 `floor(N/2)+1` 即成功，失败回滚已获取成员，Unlock 尝试全部成员 | RedissonRedLock 4.6.1 多数派/分摊等待源码对照 + 2/3 成功、1/3 回滚单元测试 ✅ |
| RTimeSeries | 源码复刻：ZSET `{name}`（score=时间戳，member=`struct.pack('BBc0Lc0Lc0',4,idLen,id,valLen,val,lblLen,lbl)`，id 来自 `redisson__ts_seq:{name}` 零填充 20 位序列）+ 过期 ZSET `redisson__ts_ttl:{name}`（TTL 分支 score=截止时刻；无 TTL 分支=now+100 年取 max 再 +1）；label blob=mark 字节(2 无/3 有)+label；Size=ZCARD−过期（惰性） | wire 契约 + **Redisson 4.6.1 双向实测 ✅**（Go 写→Java 精确时间戳读回；Java 写→Go Range 解码 + size 一致） |
| RRingBuffer | LIST `{name}` + 容量 STRING `redisson_rb:{name}`（SETNX，十进制）；溢出 RPUSH+LPOP（批量 LTRIM；SetCapacity 即时裁剪）—— Lua 同 Java 原版 | wire 契约 ✅ |
| RShardedTopic | Redis 7+ SSUBSCRIBE/SSPUBLISH（codec 编码消息；PUBSUB SHARDNUMSUB 计数） | Redis 原生协议，跨语言自动互通 ✅ |
| RFunction | FUNCTION LOAD(REPLACE)/DELETE/LIST/FLUSH + FCALL/FCALL_RO | Redis 原生命令，跨语言自动互通 ✅ |
| 嵌套复合值编码 | 递归类型包装（map→@class / slice→ArrayList 包装，任意深度） | **Redisson 4.6.1 深层嵌套（map>array>map>array）实测 ✅** |
| RKeys | DBSIZE/SCAN 迭代/模式删除（Del/Unlink）/Copy/Type/FlushDB | 单元测试 ✅ |
| RBuckets | MGET/MSET/MSETNX + 批量 TTL（pipeline） | 单元测试 ✅ |
| RScript | EVAL/EVALSHA/ScriptLoad/ScriptExists + ReturnType 转换 | 单元测试 ✅ |
| RBatch | 管道批处理（Map/Bucket/List/Set/Queue/Deque/AtomicLong/AtomicDouble/ScoredSortedSet），实测 ~7x | 单元测试 + 基准 ✅ |
| RTopic / RPatternTopic | 裸名 channel + JSON 消息 | 单元测试 ✅ |

运行双向回归（需本机 Python + `C:/GitHub/redi.py`，缺失自动 skip）：

```bash
go test -run TestInterop -v .        # Go ↔ redi.py（传递性对齐 Redisson）
go test -run TestJavaInterop -v .    # Go ↔ Java Redisson 4.6.1（直接；需 java+mvn，首次自动编译探针）
```

## 命名约定

- 工厂方法双命名并存：`GetRLock`/`GetLock`、`GetRMap`/`GetMap`……（Redisson 风格为后者）。
- `prefixName(prefix, name)` = `prefix:{name}`；`suffixName(name, suffix)` = `{name}:suffix`——花括号保证 cluster 同槽。

## RedissonClient 工厂对齐总表

> 一厂一态。详解与阶段见 [演进方案.md](演进方案.md)。状态：`WIRE_OK` 自定义 wire 可互操作；`NATIVE_OK` Redis 原生命令互通；`PARTIAL` 可用但不完整；`GO_ONLY` 同名不同协议；`REFUSE` 拒绝假实现；`PRO` 开源 UOE。

| 工厂 | 状态 | 备注 |
|------|------|------|
| getLock / getFairLock / getSpinLock / getNonReentrant* / getFencedLock / getReadWriteLock | WIRE_OK | Spin/NonReentrant/NonReentrantFair Java 双向实测 ✅；RWLock 续期 companion 与 Java 不完全同形，互斥 WIRE_OK |
| getMultiLock / getRedLock | WIRE_OK | 客户端编排；Go `GetMultiLock`/`NewMultiLock` |
| getMap / getList / getSet / getQueue / getDeque / getBlocking* / getBoundedBlockingQueue | WIRE_OK | Set / Queue Java 双向实测 ✅ |
| getMapCache / getMapCacheNative | WIRE_OK / NATIVE_OK | Native 需 Redis≥7.4；MapCacheNative Java 双向实测 ✅ |
| getSetCache / get*Multimap / get*MultimapCache / get*MultimapCacheNative | WIRE_OK / NATIVE_OK | Set/List Multimap(+Cache/Native) / SetCache Java 双向实测 ✅ |
| getScoredSortedSet / getLexSortedSet / getBucket / getBinaryStream | WIRE_OK | |
| getDelayedQueue / getAtomic* / getLongAdder / getDoubleAdder | WIRE_OK | LongAdder / DoubleAdder Java 双向实测 ✅ |
| getBloomFilter / getBloomFilterNative / getCuckooFilter / getTopK / getTDigest / getGcra | WIRE_OK / NATIVE_OK | |
| getRateLimiter / getSemaphore / getPermitExpirableSemaphore / getCountDownLatch | WIRE_OK | keepAlive 见 RateLimiter API |
| getTopic / getPatternTopic / getShardedTopic / getReliableTopic | WIRE_OK / NATIVE_OK | Topic / ShardedTopic / ReliableTopic Java 双向实测 ✅ |
| getStream / getGeo / getHyperLogLog / getBitSet / getTimeSeries / getRingBuffer | WIRE_OK / NATIVE_OK | RingBuffer Java 双向实测 ✅ |
| getIdGenerator / getKeys / getBuckets / getScript / createBatch / getFunction | WIRE_OK / NATIVE_OK | IdGenerator / Function / Keys / Buckets / Script Java 双向实测 ✅ |
| getLocalCachedMap | PARTIAL | 数据层 WIRE_OK；失效为 Go JSON（可用 Options 关近端） |
| getClientSideCaching | PARTIAL | go-redis TRACKING；非 Java EvictionPolicy |
| getArray | NATIVE_OK | Redis 8.8+；命令缺失 skip |
| getJsonBucket / getJsonBuckets | NATIVE_OK | RedisJSON；命令缺失 skip |
| getVectorSet | NATIVE_OK | Redis 8+ VSET；命令缺失 skip |
| getSearch | NATIVE_OK | RediSearch 核心子集；模块缺失 skip |
| getMaps | NATIVE_OK | 批量 DEL+HSET（HIMPORT 可后续优化） |
| getCircularBuffer | NATIVE_OK | P-post-4.6.1；ARRAY 环，≠ RingBuffer |
| getPriority* | GO_ONLY | ZSET+score，非 Comparator |
| getTransferQueue | GO_ONLY | 队列迁移；别名 `GetQueueTransfer` |
| getId / getConfig / getRedisNodes | NATIVE_OK | 运维薄封装 |
| getSortedSet | REFUSE | Comparator |
| createTransaction | REFUSE | 非 MULTI/EXEC |
| getRemoteService / getExecutorService / getLiveObjectService | REFUSE | |
| getReliableQueue / getLocalCachedMapCache / getReliablePubSubTopic / getBitVectorStore | PRO | |

## ⚠ 已知限制

| 项 | 说明 |
|----|------|
| RLock 的 holder id | 调用方传入；推荐 `Client.HolderID(threadID)` 生成 `uuid:threadId` 形态以对齐 Java field；Go 端不做线程绑定 |
| RMapCache 事件 | 事件**发布**走 Redisson 通道名；Go `AddListener` 消费同通道。跨语言监听**非**本库承诺目标 |
| Float 精度 | JSON float64 编码；RAtomicDouble 用 INCRBYFLOAT（17 位有效数字） |
| PER_CLIENT 限流 | key 带 Go 进程 id 后缀，跨语言语义等同（各客户端独立窗口），但 id 值与 Java 不同 |
| RWLock watchdog 续期结构 | Go 端续期用 `hexists(field)+pexpire`（自洽正确）；Java 另有 `{name}:{cid}:{tid}:rwlock_timeout:N`；互斥可互通，混部续期生命周期不完全同形 |
| MultimapCache 淘汰 | 默认惰性清理；可选 `StartAutoEviction` 后台清扫 |
| RClientSideCaching | PARTIAL：go-redis CSC；无 Java EvictionPolicy。Cluster/Sentinel 见实现注释 |
| LCM 失效 | Go 内部 JSON；Java 自定义二进制协议（非 Java Serialization）未移植；见 `LocalCachedMapOptions` |
| Priority* / TransferQueue | GO_ONLY，见总表 |
| PRO / REFUSE | 见总表与 [演进方案.md](演进方案.md) §3.4–3.5 |
| redi.py DelayedQueue | redi.py 为 4.x 前旧格式，与本库/Java 4.6.1 不互通 |
