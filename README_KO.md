# redi.go

[中文](README.md) | [English](README_EN.md) | [Français](README_FR.md) | [日本語](README_JA.md) | **한국어**

Redisson을 Pure Go로 재현한 Redis 클라이언트 라이브러리입니다. 대상은 **Redis 8.x**입니다.
**Wire 형식(키 배치 / Lua 알고리즘 / 값 인코딩)은 Java Redisson(JsonJacksonCodec) 및 [redi.py](https://github.com/linkerlin/redi.py)와 맞춰 상호 운용 가능합니다** — [COMPATIBILITY.md](COMPATIBILITY.md) 참고.
**Go ↔ 실제 Redisson 4.6.1 양방향 상호 운용 테스트**(24개 `TestJavaInterop_*`)와 Go ↔ redi.py 양방향 회귀(Multimap 포함)를 통과했습니다. `Client.Get*` 팩토리는 현재 **66종의 고유 R\* 반환 타입**을 커버합니다. 기여 방법은 [CONTRIBUTING.md](CONTRIBUTING.md)를 보세요.

## 기능

| 기능 | 설명 |
|---|---|
| **RLock / RFairLock / RSpinLock / RNonReentrantLock / RNonReentrantFairLock** | 재진입·공정 FIFO·스핀·재진입 금지 락; 고정 리스 또는 watchdog |
| **RReadWriteLock** | 단일 HASH + `mode` 필드의 원자적 읽기/쓰기 락(동일 스레드 write→read 강등 포함) |
| **RSemaphore / RCountDownLatch** | 분산 세마포어 / 카운트다운 래치(pub/sub 기상) |
| **RRateLimiter** | 슬라이딩 윈도우 제한기; 설정은 Redis 영속(프로세스 간); Redisson 4.6 바이너리 형식 |
| **RMap / RMapCache / RMapCacheNative** | 분산 Hash; MapCache는 entry TTL/maxIdle·packed 표면·용량 축출; MapCacheNative는 HPEXPIRE(≥7.4); Map 키에서 Lock/FairLock/RWLock/Semaphore 파생 가능 |
| **RSetCache** | TTL/maxIdle 집합(단일 ZSET, 랜덤/일괄 표면, Redisson 형식) |
| **RMultimap / RMultimapCache / *CacheNative(Set/List)** | 한 키 다값; 내부 ID = HighwayHash-128 big-endian base64; Cache는 키별 TTL; Native는 HPEXPIRE+PEXPIRE |
| **RList / RSet / RQueue / RDeque / RBlockingQueue / RBlockingDeque / RBoundedBlockingQueue** | 컬렉션/큐 전체; 유계 블로킹 큐는 `redisson_bqs:{name}` 용량 companion |
| **RDelayedQueue** | 지연 큐(timeout ZSET + packed LIST + 백그라운드 이전; 대기 항목 조회/삭제/비우기) |
| **RScoredSortedSet / RLexSortedSet** | 점수/사전식 집합(rank, 랜덤, 앞뒤 poll, 정/역 범위; Lex 멤버는 원시 저장) |
| **RBucket** | 객체 버킷(밀리초 TTL, CAS, GetAndDelete…) |
| **RBinaryStream** | 원시 바이트 스트림(APPEND/GETRANGE/SETRANGE, 순차 스트림·seek 가능 채널; Codec 미경유) |
| **RAtomicLong / RAtomicDouble** | 원자 카운터(십진 문자열, CAS) |
| **RTopic / RPatternTopic** | Pub/Sub(패턴 구독 포함) |
| **RBloomFilter / RBloomFilterNative / RCuckooFilter / RTopK / RTDigest / RGcra** | 자체 Bloom / Redis BF.* / CF.* / TOPK.* / TDIGEST.* / GCRA(명령 없으면 테스트 skip) |
| **RIdGenerator** | ID 생성기(배치 할당 캐시) |
| **RTransferQueue / GetQueueTransfer** | **GO_ONLY** 큐 간 원자 이전(Java RemoteService TransferQueue 아님) |
| **RHyperLogLog / RGeo / RBitSet / RStream** | 카디널리티 / 지리 인덱스(일괄 GEO, GEOSEARCHSTORE) / 분산 비트맵(Java 비트 순서) / 분산 로그(소비자 그룹, Pending, Claim/AutoClaim) |
| **RPermitExpirableSemaphore / RReliableTopic** | 허가별 리스 세마포어 / Stream 기반 신뢰 토픽(구독자별 소비자 그룹 + 크래시 재전달) |
| **RLocalCachedMap** | 니어 캐시 Map(write-through + Go 무효화; Java 혼용 시 `DisableNearCache`); 데이터 계층 = RMap wire |
| **RPriorityQueue / RPriorityBlockingQueue / RPriorityBlockingDeque / RPriorityDeque** | ZSET+score 우선순위 큐 및 블로킹/양끝 래퍼(**비** Java Comparator 동명 프로토콜) |
| **RArray / RCircularBuffer** | Redis 8.8+ ARRAY; CircularBuffer는 안정 슬롯 링(≠ RingBuffer); 없으면 skip |
| **RJsonBucket / RVectorSet / RSearch / RMaps** | RedisJSON / Vector Set / RediSearch 부분 / 일괄 HASH 가져오기(모듈 없으면 skip) |
| **RClientSideCaching** | PARTIAL: go-redis RESP3 TRACKING(standalone DB0); Java EvictionPolicy 아님 |
| **RLongAdder / RDoubleAdder** | 고경합 카운터: 로컬 무네트워크 누적, `Sum()`이 인스턴스 간(Java 포함) flush 조율; 비파괴 |
| **RFencedLock / RMultiLock / RRedLock** | 펜스 토큰 락 / 전부 아니면 전무 멀티락 / RedLock 엄격 과반(실패 시 롤백) |
| **RTimeSeries** | 시계열(동일 시각 다중 엔트리, entry TTL, 앞뒤 읽기/poll, 정/역 범위; Redisson wire 호환) |
| **RRingBuffer / RShardedTopic / RFunction** | 고정 용량 링 버퍼(넘치면 최구 축출) / Redis 7+ 샤드 pub/sub(SSUBSCRIBE) / Redis Functions(FUNCTION/FCALL) |
| **RKeys / RBuckets / RScript / GetRedisNodes** | 키 공간 / 일괄 버킷 / Lua / 토폴로지 탐침 |
| **RBatch** | 파이프라인(`NewBatch()` → 구조체 쓰기 큐잉 → `Execute()` 1회 왕복; 실측 **약 7×**) |
| **토폴로지** | single / cluster / sentinel(`redis.UniversalClient`) |
| **TUI Dashboard** | charmbracelet/bubbles 기반 터미널 모니터 |
| **정렬 로드맵** | [演进方案.md](演进方案.md) 및 [COMPATIBILITY.md](COMPATIBILITY.md) 참고 |

기본 값 코덱은 `JSONCodec`: Redisson `JsonJacksonCodec`과 상호 운용(int32 초과 정수는 `["java.lang.Long",v]`로 래핑; 디코드 시 `@class` 제거). `Config.Codec`로 교체 가능. 구조화 읽기는 `RBucket` / `RMap` / `RList` / `RQueue` / `RDeque` / `RLocalCachedMap`의 `GetInto` / `PeekInto` / `PollInto` / `Remove*Into`; `RMap`은 `PutAll` / `FastPut*` / `Replace*` / `Keys` / `Values`도 제공.

## 의존성

- [redis/go-redis v9](https://github.com/redis/go-redis/v9) — Redis 드라이버(single/cluster/sentinel)
- [minio/highwayhash](https://github.com/minio/highwayhash) — RBloomFilter용 Redisson 정렬 해시
- [charmbracelet/bubbles](https://github.com/charmbracelet/bubbles) — TUI 컴포넌트(dashboard 서브모듈만)

## 빠른 시작

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

    // --- RLock(ttl<=0이면 watchdog 자동 연장) ---
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

### 속도 제한기

```go
rl := client.GetRateLimiter("api-limiter")
rl.TrySetRate(ctx, redi.RateTypeOverall, 100, time.Second) // Redis에 영속
if ok, _ := rl.TryAcquire(ctx, 1); !ok {
    // 제한됨
}
```

### RBatch(파이프라인)

```go
batch := client.NewBatch()
m := batch.GetMap("my-map")
for i := 0; i < 1000; i++ {
    _ = m.Put(ctx, fmt.Sprint(i), i) // 큐잉; 아직 네트워크 왕복 없음
}
if err := batch.Execute(ctx); err != nil { // 모든 쓰기를 1회 왕복
    log.Fatal(err)
}
```

(배치에 묶인 구조체 읽기는 `Execute` 전 제로값 — 일반 구조체로 다시 읽으세요.)

### Cluster / Sentinel

```go
cfg := redi.DefaultConfig()
cfg.Mode = redi.ModeCluster
cfg.Addrs = []string{"node1:7000", "node2:7001", "node3:7002"}

// 또는
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

세 탭: **INFO**(서버 지표), **Locks**(보유자 / 재진입 수 / TTL), **Limiters**(제한 설정과 남은 허가). `1/2/3` 또는 `←/→`로 전환.

## 개발

```bash
go test ./...          # 로컬 Redis(localhost:6379) 필요; 없으면 자동 skip
go test -race ./...    # CI 전체 모드
go test -run TestInterop -v .        # Go ↔ redi.py 양방향(Python + redi.py; 없으면 skip)
go test -run TestJavaInterop -v .    # Go ↔ Java Redisson 4.6.1 양방향(java+mvn; 최초 프로브 자동 빌드; CI 전용 job)
go test -bench . -benchtime 1s .     # 벤치마크
go vet ./...
```

wire 형식 계약은 `wire_compat_test.go`; 언어 간 회귀는 `interop_redipy_test.go`(redi.py 경유 — 실제 Redisson과 양방향 검증됨 — 추이적 검증). 상호 운용을 깨는 변경은 여기서 실패합니다.

## 버전

[CHANGELOG.md](CHANGELOG.md) 참고. v0.2.0부터 키에 사설 `redi:` 접두사가 없습니다(상호 운용을 위한 파괴적 변경). 기여 흐름: [CONTRIBUTING.md](CONTRIBUTING.md).
