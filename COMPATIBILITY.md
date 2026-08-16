# 兼容性矩阵（COMPATIBILITY）

> 与 Java Redisson（`JsonJacksonCodec`，无类型信息）及 redi.py 的互操作状态。
> wire 依据：Redisson 4.6.x 源码 + Java 实测 + redi.py 双向实测结论。
> Go 侧契约测试：`wire_compat_test.go`；**redi.py 双向回归：`interop_redipy_test.go`；Java（Redisson 4.6.1）直接双向回归：`interop_java_test.go` + `interop_java2_test.go`（单 JVM REPL 探针 `interop/java-probe/`，20 组用例全过）**。

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
| RReadWriteLock | 单 HASH + `mode` 字段（read/write/read-write），读 field=`{id}` 写 field=`{id}:write`，channel `redisson_rwlock:{name}`（读释放消息 1 / 写释放 0） | **Redisson 4.6.1 直接双向实测 ✅**（读写互斥、共享读、释放可见） |
| RSemaphore | STRING 计数器 + `redisson_sc:{name}` 唤醒 | **Redisson 4.6.1 直接实测 ✅** |
| RCountDownLatch | STRING 计数 + DECR/DEL/PUBLISH（channel `redisson_countdownlatch__channel__{name}`，消息 0/1） | **Redisson 4.6.1 countDown 唤醒 Go Await ✅** |
| RRateLimiter | HASH `{name}`（rate/interval/keepAliveTime/type 枚举序数）+ `{name}:value` + ZSET `{name}:permits`（member=0x10+16B UUID+LE uint32 permits） | **Redisson 4.6.1 共享窗口实测 ✅** |
| RMap / RList / RSet / RQueue / RDeque / RBlockingQueue | 裸名 + JSON 值编码（JsonJacksonCodec 互通，含 @class/Long/ArrayList 包装） | **Redisson 4.6.1 String/Long/嵌套对象双向实测 ✅** |
| RScoredSortedSet | ZSET + JSON member | **Redisson 4.6.1 score/rank 双向实测 ✅** |
| RLexSortedSet | ZSET + 字典序操作，成员**裸存储**（跳过 codec，Redisson 特例） | **Redisson 4.6.1 add/range 双向实测 ✅** |
| RBucket | 裸名 + JSON 值（毫秒 TTL） | **Redisson 4.6.1 双向实测 ✅** |
| RMapCache | `<dQ` struct 打包值 + ZSET `redisson__timeout__set:{name}` / `redisson__idle__set:{name}` + entry 事件（channel `redisson_map_cache_{kind}:{name}`，消息 = 8 字节 LE 长度前缀 + codec 段） | wire 契约 + **Redisson 4.6.1 双向读写实测 ✅**（事件通道仅 Go 端内消费） |
| RDelayedQueue | ZSET `redisson_delay_queue_timeout:{name}` + LIST `redisson_delay_queue:{name}`（struct-packed member + 迁移 Lua 同 Redisson 原版，见上文 wire 事实 2） | **Redisson 4.6.1 双向迁移实测 ✅** |
| RAtomicLong / RAtomicDouble | 裸名十进制字符串（StringCodec 语义） | wire 契约 + **redi.py 双向实测 ✅** |
| RBloomFilter | 位图裸名 + `{name}:config` HASH，HighwayHash-128（Redisson 固定 KEY），Java 截断/半上取整公式 | **Redisson 4.6.1 位/公式双向实测 ✅** |
| RIdGenerator | `{name}` 计数 + `{name}:allocation` | 单元测试 ✅ |
| RSetCache | 单 ZSET（member=值，score=绝对过期时刻；无 TTL = MaxInt64）+ idle ZSET | 单元测试 ✅ |
| RMultimap（Set/List） | HASH `{name}`（field=JSON key → 内部 ID）+ 集合 `{name}:{id}`；ID = HighwayHash-128 大端 + 无填充 base64（Java `Hash.hash128toBase64`） | **redi.py 内部 ID 字节级 + 双向读写实测 ✅** |
| RBlockingDeque | 双端阻塞消费（BLPOP/BRPOP） | 单元测试 ✅ |
| RTransferQueue | LPOP+RPUSH 单 Lua 原子迁移 | 单元测试 ✅ |
| RHyperLogLog | PFADD/PFCOUNT/PFMERGE（codec 编码成员；sketch 为 Redis 原生） | **Redisson 4.6.1 混合计数实测 ✅** |
| RGeo | GEOADD/GEOSEARCH/GEOPOS/GEODIST（codec 编码成员；GEOSEARCH 非 GEORADIUS，Redis 8 兼容） | **Redisson 4.6.1 双向 pos/dist 实测 ✅** |
| RBitSet | 原生 GETBIT/SETBIT 位序（与 Java RedissonBitSet 一致，无位反转；MSB-first 字节数组） | **Redisson 4.6.1 双向位/cardinality/length 实测 ✅** |
| RStream | XADD/XRANGE/XREADGROUP/XPENDING/XACK/XCLAIM/XAUTOCLAIM；field 名与值均 codec 编码（Redisson RStream 同款）；消费组为 Redis 原生 | **Redisson 4.6.1 双向实测 ✅**（Java 写→Go 组读、跨语言 Ack、Go 写→Java 新组读全史） |
| RPermitExpirableSemaphore | STRING `{name}` 计数 + ZSET `{name}:timeout`（member=许可 ID，score=到期时刻）+ channel `redisson_sc:{name}`；过期回收在 acquire 与读路径惰性执行（Lua） | **Redisson 4.6.1 共享许可池实测 ✅**（Java 租借→Go 可见 0；Java 释放→Go 获取） |
| RReliableTopic | STREAM `{name}`（XADD field **`m`**）+ **每订阅者独立消费组**（组名=subscriberId，Java 语义：各组收全量；redi.py 单组多 consumer 实为负载均衡，语义错误）+ ZSET `{name}:timeout` 活性（watchdog/3 刷新）；回调返回后才 XACK（崩溃重投递） | **Redisson 4.6.1 实测 ✅**（Go 发布→Java 监听收到；Go/Java 订阅组各收全量） |
| RLocalCachedMap | 数据层 = RMap wire 格式（✅ 跨语言读写）；**失效广播为 Go 内部协议**（channel `{name}:inval`，JSON 消息）——Java 的 LocalCachedMapInvalidation（keyHash 二进制数组 + 更新日志 + Java 序列化对象）未复刻，Java 写入不会实时失效 Go 本地缓存（Go 读有 Redis 兜底） | 数据层 **Redisson 4.6.1 双向读写实测 ✅**；失效广播 Go↔Go ✅ / Java↔Go ❌（诚实标注） |
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

## ⚠ 已知限制

| 项 | 说明 |
|----|------|
| RLock 的 holder id | 由调用方传入（建议 `uuid:threadId` 形态以对齐 Java field 格式）；Go 端不做线程绑定 |
| RRateLimiter keepAlive | 不清理过期 config / 不做 keepAlive 轮询 |
| RMapCache 事件 | 事件**发布**走 Redisson 通道（Java 监听端可收）；Go 端 `AddListener` 消费同通道，跨语言事件语义已由通道+格式锁定 |
| Float 精度 | JSON float64 编码；RAtomicDouble 用 INCRBYFLOAT（17 位有效数字） |
| PER_CLIENT 限流 | key 带 Go 进程 id 后缀，跨语言语义等同（各客户端独立窗口），但 id 值与 Java 不同 |
| RWLock watchdog 续期结构 | Go 端续期用 `hexists(field)+pexpire`（自洽正确）；Java 4.x 另有 `{name}:{cid}:{tid}:rwlock_timeout:1` 跟踪 key，Go 端未复刻（不影响互斥语义互操作） |
| RSortedSet（LIST+Comparator 版） | 未实现（勿用 ZSET 冒充，redi.py C7 教训） |
| redi.py DelayedQueue | redi.py 为 4.x 前旧格式（单 ZSET），与本库/Java 4.6.1 不互通；Go↔Java 已直接双向实测 |
