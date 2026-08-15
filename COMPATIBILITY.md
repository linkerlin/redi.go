# 兼容性矩阵（COMPATIBILITY）

> 与 Java Redisson（`JsonJacksonCodec`，无类型信息）及 redi.py 的互操作状态。
> wire 依据：Redisson 4.6.x 源码 + redi.py 经真实 Redisson 双向实测验证的结论（见 redi.py/兼容性优化演进方案.md §0.1）。
> Go 侧契约测试：`wire_compat_test.go`；**redi.py 双向回归：`interop_redipy_test.go`；Java（Redisson 4.6.1）直接双向回归：`interop_java_test.go`（单 JVM REPL 探针 `interop/java-probe/`，7 组用例全过）**。

## 重要 wire 事实（Java 实测）

Redisson `JsonJacksonCodec` 开启 default typing，**读取裸 JSON 对象/数组会报错（missing type id）**。因此 redi.go 编码端：
- map → `{"@class":"java.util.LinkedHashMap",...}`
- slice → `["java.util.ArrayList",[...]]`
- 超 int32 整数 → `["java.lang.Long",v]`
- 解码端剥离以上全部包装与 `@class`（裸 JSON 也能读，向后兼容）。
（redi.py 写裸对象时 Java 读会炸 —— 本库编码端已补齐该缺口。）

## ✅ 可互操作（wire 对齐）

| 结构 | Redis 布局 | 验证状态 |
|------|-----------|---------|
| RLock | HASH `{name}`，field=holder id，channel `redisson_lock__channel:{name}`（unlock 消息 0），renew/unlock Lua 同 Redisson | **Redisson 4.6.1 直接双向实测 ✅** |
| RReadWriteLock | 单 HASH + `mode` 字段（read/write/read-write），读 field=`{id}` 写 field=`{id}:write`，channel `redisson_rwlock:{name}`（读释放消息 1 / 写释放 0） | wire 契约测试 ✅ |
| RSemaphore | STRING 计数器 + `redisson_sc:{name}` 唤醒 | **Redisson 4.6.1 直接实测 ✅** |
| RCountDownLatch | STRING 计数 + DECR/DEL/PUBLISH（channel `redisson_countdownlatch__channel__{name}`，消息 0/1） | **Redisson 4.6.1 countDown 唤醒 Go Await ✅** |
| RRateLimiter | HASH `{name}`（rate/interval/keepAliveTime/type 枚举序数）+ `{name}:value` + ZSET `{name}:permits`（member=0x10+16B UUID+LE uint32 permits） | **Redisson 4.6.1 共享窗口实测 ✅** |
| RMap / RList / RSet / RQueue / RDeque / RBlockingQueue | 裸名 + JSON 值编码（JsonJacksonCodec 互通，含 @class/Long/ArrayList 包装） | **Redisson 4.6.1 String/Long/嵌套对象双向实测 ✅** |
| RMapCache | `<dQ` struct 打包值 + ZSET `redisson__timeout__set:{name}` / `redisson__idle__set:{name}` + entry 事件（channel `redisson_map_cache_{kind}:{name}`，消息 = 8 字节 LE 长度前缀 + codec 段） | wire 契约 + 事件二进制格式实测 ✅ |
| RDelayedQueue | ZSET `redisson_delay_queue:{name}`（score=交付时刻）+ channel `redisson_delay_queue_channel:{name}` + Redis TIME 时钟 | **redi.py 双向 ZSET 实测 ✅** |
| RAtomicLong / RAtomicDouble | 裸名十进制字符串（StringCodec 语义） | wire 契约 + **redi.py 双向实测 ✅** |
| RBucket | 裸名 + JSON 值（毫秒 TTL） | 单元测试 ✅ |
| RScoredSortedSet | ZSET + JSON member | 单元测试 ✅ |
| RBloomFilter | 位图裸名 + `{name}:config` HASH，HighwayHash-128（Redisson 固定 KEY），Java 截断/半上取整公式 | **Redisson 4.6.1 位/公式双向实测 ✅** |
| RIdGenerator | `{name}` 计数 + `{name}:allocation` | 单元测试 ✅ |
| RSetCache | 单 ZSET（member=值，score=绝对过期时刻；无 TTL = MaxInt64）+ idle ZSET | 单元测试 ✅ |
| RMultimap（Set/List） | HASH `{name}`（field=JSON key → 内部 ID）+ 集合 `{name}:{id}`；ID = HighwayHash-128 大端 + 无填充 base64（Java `Hash.hash128toBase64`） | **redi.py 内部 ID 字节级 + 双向读写实测 ✅** |
| RBlockingDeque | 双端阻塞消费（BLPOP/BRPOP） | 单元测试 ✅ |
| RLexSortedSet | ZSET + 字典序操作，成员**裸存储**（跳过 codec，Redisson 特例） | 单元测试 ✅ |
| RTransferQueue | LPOP+RPUSH 单 Lua 原子迁移 | 单元测试 ✅ |
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
| RReadWriteLock / RRateLimiter 无 watchdog/keepAlive | RW 锁 `ttl<=0` 时有 watchdog 续期（lease/3）；限流器不清理过期 config |
| RMapCache 事件监听 | `redisson_map_cache_{created,updated,removed,expired}:{name}` 通道暂未实现 |
| Float 精度 | JSON float64 编码；RAtomicDouble 用 INCRBYFLOAT（17 位有效数字） |
| PER_CLIENT 限流 | key 带 Go 进程 id 后缀，跨语言语义等同（各客户端独立窗口），但 id 值与 Java 不同 |
| RWLock watchdog 续期结构 | Go 端续期用 `hexists(field)+pexpire`（自洽正确）；Java 4.x 另有 `{name}:{cid}:{tid}:rwlock_timeout:1` 跟踪 key，Go 端未复刻（不影响互斥语义互操作） |
| RSortedSet（LIST+Comparator 版） | 未实现（勿用 ZSET 冒充，redi.py C7 教训） |
| 直接 Java 双向回归 | 当前经 redi.py（其格式已与 Redisson 4.6.1 双向实测）传递性验证；如需直接 Java 探针，可复用 `interop/redipy_probe.py` 模式对接 WireProbe |
