# redi.go

[中文](README.md) | [English](README_EN.md) | [Français](README_FR.md) | **日本語** | [한국어](README_KO.md)

Redisson を Pure Go で再現した Redis クライアントライブラリです。対象は **Redis 8.x**。
**Wire 形式（キー配置 / Lua アルゴリズム / 値エンコード）は Java Redisson（JsonJacksonCodec）および [redi.py](https://github.com/linkerlin/redi.py) と揃え、相互運用可能です** — 詳細は [COMPATIBILITY.md](COMPATIBILITY.md)。
**Go ↔ 実 Redisson 4.6.1 の双方向相互運用テスト**（36 個の `TestJavaInterop_*`）および Go ↔ redi.py 双方向回帰（Multimap 含む）を通過済みです。`Client.Get*` ファクトリは現在 **66 種のユニークな R\* 戻り値型**をカバーします。貢献方法は [CONTRIBUTING.md](CONTRIBUTING.md) を参照してください。

## 機能

| 機能 | 説明 |
|---|---|
| **RLock / RFairLock / RSpinLock / RNonReentrantLock / RNonReentrantFairLock** | 再入・公平 FIFO・スピン・再入禁止ロック；固定リースまたは watchdog |
| **RReadWriteLock** | 単一 HASH + `mode` フィールドの原子的読み書きロック（同一スレッド write→read 降格含む） |
| **RSemaphore / RCountDownLatch** | 分散セマフォ / カウントダウンラッチ（pub/sub 起床） |
| **RRateLimiter** | スライディングウィンドウ制限；設定は Redis 永続（プロセス横断）；Redisson 4.6 バイナリ形式 |
| **RMap / RMapCache / RMapCacheNative** | 分散 Hash；MapCache は entry TTL/maxIdle・packed 表面・容量淘汰；MapCacheNative は HPEXPIRE（≥7.4）；Map キーから Lock/FairLock/RWLock/Semaphore を派生可 |
| **RSetCache** | TTL/maxIdle 付き集合（単一 ZSET、乱択/一括表面、Redisson 形式） |
| **RMultimap / RMultimapCache / *CacheNative（Set/List）** | 1 キー多値；内部 ID = HighwayHash-128 big-endian base64；Cache はキー別 TTL；Native は HPEXPIRE+PEXPIRE |
| **RList / RSet / RQueue / RDeque / RBlockingQueue / RBlockingDeque / RBoundedBlockingQueue** | コレクション／キュー一式；有界ブロッキングキューは `redisson_bqs:{name}` 容量 companion |
| **RDelayedQueue** | 遅延キュー（timeout ZSET + packed LIST + バックグラウンド移行；待機項目の照会/削除/清空） |
| **RScoredSortedSet / RLexSortedSet** | スコア／辞書順集合（rank・乱択・先頭末尾 poll・正逆範囲；Lex メンバは生保存） |
| **RBucket** | オブジェクトバケツ（ミリ秒 TTL、CAS、GetAndDelete…） |
| **RBinaryStream** | 生バイトストリーム（APPEND/GETRANGE/SETRANGE、逐次ストリームと seek 可能チャネル；Codec 非経由） |
| **RAtomicLong / RAtomicDouble** | 原子カウンタ（十進文字列、CAS） |
| **RTopic / RPatternTopic** | Pub/Sub（パターン購読含む） |
| **RBloomFilter / RBloomFilterNative / RCuckooFilter / RTopK / RTDigest / RGcra** | 自前 Bloom / Redis BF.* / CF.* / TOPK.* / TDIGEST.* / GCRA（コマンド欠如時テスト skip） |
| **RIdGenerator** | ID 生成器（バッチ割当キャッシュ） |
| **RTransferQueue / GetQueueTransfer** | **GO_ONLY** キュー間原子移行（Java RemoteService TransferQueue ではない） |
| **RHyperLogLog / RGeo / RBitSet / RStream** | 基数推定 / 地理索引（一括 GEO、GEOSEARCHSTORE）/ 分散ビットマップ（Java ビット順）/ 分散ログ（消費者グループ、Pending、Claim/AutoClaim） |
| **RPermitExpirableSemaphore / RReliableTopic** | 許可単位リースのセマフォ / Stream ベース可靠トピック（購読者ごと消費者グループ + クラッシュ再配信） |
| **RLocalCachedMap** | ニアキャッシュ Map（write-through + Go 無効化；Java 混在時は `DisableNearCache`）；データ層は RMap wire |
| **RPriorityQueue / RPriorityBlockingQueue / RPriorityBlockingDeque / RPriorityDeque** | ZSET+score 優先キューおよびブロッキング／両端ラッパ（**非** Java Comparator 同名プロトコル） |
| **RArray / RCircularBuffer** | Redis 8.8+ ARRAY；CircularBuffer は安定スロット環（≠ RingBuffer）；欠如時 skip |
| **RJsonBucket / RVectorSet / RSearch / RMaps** | RedisJSON / Vector Set / RediSearch サブセット / 一括 HASH 取込（モジュール欠如時 skip） |
| **RClientSideCaching** | PARTIAL：go-redis RESP3 TRACKING（standalone DB0）；Java EvictionPolicy ではない |
| **RLongAdder / RDoubleAdder** | 高競合カウンタ：ローカル無ネットワーク蓄積、`Sum()` がインスタンス間（Java 含む）flush を協調；非破壊 |
| **RFencedLock / RMultiLock / RRedLock** | フェンストークンロック / 全有か全無のマルチロック / RedLock 厳格過半数（失敗時ロールバック） |
| **RTimeSeries** | 時系列（同一時刻複数エントリ、entry TTL、先頭末尾読取/poll、正逆範囲；Redisson wire 互換） |
| **RRingBuffer / RShardedTopic / RFunction** | 定容リングバッファ（溢時最古淘汰）/ Redis 7+ シャーディング pub/sub（SSUBSCRIBE）/ Redis Functions（FUNCTION/FCALL） |
| **RKeys / RBuckets / RScript / GetRedisNodes** | キースペース / 一括バケツ / Lua / トポロジ探査 |
| **RBatch** | パイプライン（`NewBatch()` → 構造書込をキュー → `Execute()` 1 往復；実測 **約 7×**） |
| **トポロジ** | single / cluster / sentinel（`redis.UniversalClient`） |
| **TUI Dashboard** | charmbracelet/bubbles ベースの端末モニタ |
| **整合ロードマップ** | [演进方案.md](演进方案.md) と [COMPATIBILITY.md](COMPATIBILITY.md) を参照 |

デフォルト値コーデックは `JSONCodec`：Redisson `JsonJacksonCodec` と相互運用（int32 超の整数は `["java.lang.Long",v]` でラップ；デコードは `@class` 型情報を除去）。`Config.Codec` で置換可。構造化読取は `RBucket` / `RMap` / `RList` / `RQueue` / `RDeque` / `RLocalCachedMap` の `GetInto` / `PeekInto` / `PollInto` / `Remove*Into`；`RMap` は `PutAll` / `FastPut*` / `Replace*` / `Keys` / `Values` も提供。

## 依存関係

- [redis/go-redis v9](https://github.com/redis/go-redis/v9) — Redis ドライバ（single/cluster/sentinel）
- [minio/highwayhash](https://github.com/minio/highwayhash) — RBloomFilter 用 Redisson 整列ハッシュ
- [charmbracelet/bubbles](https://github.com/charmbracelet/bubbles) — TUI コンポーネント（dashboard サブモジュールのみ）

## クイックスタート

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

    // --- RLock（ttl<=0 で watchdog 自動延長） ---
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

### レートリミッタ

```go
rl := client.GetRateLimiter("api-limiter")
rl.TrySetRate(ctx, redi.RateTypeOverall, 100, time.Second) // Redis に永続化
if ok, _ := rl.TryAcquire(ctx, 1); !ok {
    // 制限された
}
```

### RBatch（パイプライン）

```go
batch := client.NewBatch()
m := batch.GetMap("my-map")
for i := 0; i < 1000; i++ {
    _ = m.Put(ctx, fmt.Sprint(i), i) // キューイング；まだネットワーク往復なし
}
if err := batch.Execute(ctx); err != nil { // 全書込を 1 往復
    log.Fatal(err)
}
```

（バッチ束縛構造の読取は `Execute` 前はゼロ値 — 通常構造で読み戻してください。）

### Cluster / Sentinel

```go
cfg := redi.DefaultConfig()
cfg.Mode = redi.ModeCluster
cfg.Addrs = []string{"node1:7000", "node2:7001", "node3:7002"}

// または
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

3 つのタブ：**INFO**（サーバ指標）、**Locks**（保有者 / 再入回数 / TTL）、**Limiters**（制限設定と残許可）。`1/2/3` または `←/→` で切替。

## 開発

```bash
go test ./...          # ローカル Redis（localhost:6379）必要；なければ自動 skip
go test -race ./...    # CI フルモード
go test -run TestInterop -v .        # Go ↔ redi.py 双方向（Python + redi.py；欠如時 skip）
go test -run TestJavaInterop -v .    # Go ↔ Java Redisson 4.6.1 双方向（java+mvn；初回プローブ自動ビルド；CI に専用 job）
go test -bench . -benchtime 1s .     # ベンチマーク
go vet ./...
```

wire 形式契約は `wire_compat_test.go`；言語横断回帰は `interop_redipy_test.go`（redi.py 経由 — 実 Redisson と双方向検証済み — の推移的検証）。相互運用を壊す変更はここで失敗します。

## バージョン

[CHANGELOG.md](CHANGELOG.md) を参照。v0.2.0 以降、キーに私有 `redi:` 接頭辞は付きません（相互運用のための破壊的変更）。貢献フローは [CONTRIBUTING.md](CONTRIBUTING.md)。
