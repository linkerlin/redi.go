# redi.go

[中文](README.md) | [English](README_EN.md) | **Français** | [日本語](README_JA.md) | [한국어](README_KO.md)

Bibliothèque cliente Redis en Pure Go calquée sur Redisson, ciblant **Redis 8.x**.
**Le format wire (disposition des clés / algorithmes Lua / encodage des valeurs) s’aligne sur Java Redisson (JsonJacksonCodec) et [redi.py](https://github.com/linkerlin/redi.py) pour l’interopérabilité** — voir [COMPATIBILITY.md](COMPATIBILITY.md).
Elle a passé les **tests d’interopérabilité bidirectionnelle Go ↔ Redisson 4.6.1 réel** (33 fonctions `TestJavaInterop_*`) et la régression bidirectionnelle Go ↔ redi.py (dont Multimap). Les usines `Client.Get*` couvrent actuellement **66 types de retour R\* uniques**. Voir [CONTRIBUTING.md](CONTRIBUTING.md) pour contribuer.

## Fonctionnalités

| Fonctionnalité | Description |
|---|---|
| **RLock / RFairLock / RSpinLock / RNonReentrantLock / RNonReentrantFairLock** | Verrous réentrants, FIFO équitable, spin et non réentrants ; bail fixe ou watchdog |
| **RReadWriteLock** | Verrou lecture/écriture atomique sur un seul HASH + champ `mode` (dont rétrogradation write→read même thread) |
| **RSemaphore / RCountDownLatch** | Sémaphore / barre de décompte distribués avec réveil pub/sub |
| **RRateLimiter** | Limiteur à fenêtre glissante ; config persistée dans Redis (inter-processus) ; format binaire Redisson 4.6 |
| **RMap / RMapCache / RMapCacheNative** | Hash distribué ; MapCache gère TTL/maxIdle d’entrée, surface packed et éviction par taille ; MapCacheNative utilise HPEXPIRE (≥7.4) ; une clé de map peut dériver Lock/FairLock/RWLock/Semaphore |
| **RSetCache** | Ensemble avec TTL/maxIdle (un seul ZSET, surface aléatoire/bulk, format Redisson) |
| **RMultimap / RMultimapCache / *CacheNative (Set/List)** | Une clé, plusieurs valeurs ; ID interne = HighwayHash-128 big-endian base64 ; Cache = TTL par clé ; Native = HPEXPIRE+PEXPIRE |
| **RList / RSet / RQueue / RDeque / RBlockingQueue / RBlockingDeque / RBoundedBlockingQueue** | Famille collections/files ; file bornée via companion `redisson_bqs:{name}` |
| **RDelayedQueue** | File différée (ZSET timeout + LIST packed + transfert arrière-plan ; requête/suppression/vidage des items en attente) |
| **RScoredSortedSet / RLexSortedSet** | Ensembles scorés / lexicographiques (rang, aléatoire, poll début/fin, plages ; membres Lex bruts) |
| **RBucket** | Seau d’objets (TTL ms, CAS, GetAndDelete, …) |
| **RBinaryStream** | Flux d’octets bruts (APPEND/GETRANGE/SETRANGE, flux séquentiels et canal seekable ; hors Codec) |
| **RAtomicLong / RAtomicDouble** | Compteurs atomiques (chaînes décimales, CAS) |
| **RTopic / RPatternTopic** | Pub/sub (dont abonnement par motif) |
| **RBloomFilter / RBloomFilterNative / RCuckooFilter / RTopK / RTDigest / RGcra** | Bloom classique / Redis BF.* / CF.* / TOPK.* / TDIGEST.* / GCRA (tests skip si commandes absentes) |
| **RIdGenerator** | Générateur d’ID (cache d’allocation par lots) |
| **RTransferQueue / GetQueueTransfer** | **GO_ONLY** migration atomique entre files (pas le TransferQueue RemoteService Java) |
| **RHyperLogLog / RGeo / RBitSet / RStream** | Cardinalité / geo (GEO bulk, GEOSEARCHSTORE) / bitmap (ordre de bits Java) / journal (groupes, Pending, Claim/AutoClaim) |
| **RPermitExpirableSemaphore / RReliableTopic** | Sémaphore à bail par permis / topic fiable Stream (groupe par abonné + redélivrance après crash) |
| **RLocalCachedMap** | Near cache (write-through + invalidation Go ; `DisableNearCache` en mix Java) ; couche données = wire RMap |
| **RPriorityQueue / RPriorityBlockingQueue / RPriorityBlockingDeque / RPriorityDeque** | Files prioritaires ZSET+score et wrappers bloquants/double-ended (**pas** le protocole Comparator Java homonyme) |
| **RArray / RCircularBuffer** | ARRAY Redis 8.8+ ; CircularBuffer à slots stables (≠ RingBuffer) ; skip si absent |
| **RJsonBucket / RVectorSet / RSearch / RMaps** | RedisJSON / Vector Set / sous-ensemble RediSearch / import HASH bulk (skip si modules absents) |
| **RClientSideCaching** | PARTIAL : TRACKING RESP3 go-redis (standalone DB0) ; pas EvictionPolicy Java |
| **RLongAdder / RDoubleAdder** | Compteurs haute contention : accumulation locale sans réseau ; `Sum()` coordonne le flush entre instances (dont Java) ; non destructif |
| **RFencedLock / RMultiLock / RRedLock** | Verrou à jeton fenced / multi-lock tout-ou-rien / RedLock majorité stricte (rollback en échec) |
| **RTimeSeries** | Séries temporelles (plusieurs entrées par timestamp, TTL d’entrée, lecture/poll début/fin, plages ; wire Redisson) |
| **RRingBuffer / RShardedTopic / RFunction** | Buffer annulaire à capacité fixe (évince le plus ancien) / pub/sub shardé Redis 7+ (SSUBSCRIBE) / Redis Functions (FUNCTION/FCALL) |
| **RKeys / RBuckets / RScript / GetRedisNodes** | Espace de clés / seaux bulk / Lua / sonde de topologie |
| **RBatch** | Pipeline (`NewBatch()` → enqueue → `Execute()` en un round-trip ; **~7×** mesuré) |
| **Topologies** | single / cluster / sentinel (`redis.UniversalClient`) |
| **TUI Dashboard** | Panneau terminal basé sur charmbracelet/bubbles |
| **Feuille de route** | Voir [演进方案.md](演进方案.md) et [COMPATIBILITY.md](COMPATIBILITY.md) |

Le codec par défaut est `JSONCodec` : interopérable avec `JsonJacksonCodec` Redisson (entiers > int32 encapsulés en `["java.lang.Long",v]` ; décodage retire le typage `@class`). Remplaçable via `Config.Codec`. Lectures structurées : `GetInto` / `PeekInto` / `PollInto` / `Remove*Into` sur `RBucket` / `RMap` / `RList` / `RQueue` / `RDeque` / `RLocalCachedMap` ; `RMap` expose aussi `PutAll` / `FastPut*` / `Replace*` / `Keys` / `Values`.

## Dépendances

- [redis/go-redis v9](https://github.com/redis/go-redis/v9) — pilote Redis (single/cluster/sentinel)
- [minio/highwayhash](https://github.com/minio/highwayhash) — hash aligné Redisson pour RBloomFilter
- [charmbracelet/bubbles](https://github.com/charmbracelet/bubbles) — composants TUI (sous-module dashboard uniquement)

## Démarrage rapide

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

    // --- RLock (ttl<=0 active le renouvellement watchdog) ---
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

### Limiteur de débit

```go
rl := client.GetRateLimiter("api-limiter")
rl.TrySetRate(ctx, redi.RateTypeOverall, 100, time.Second) // persisté dans Redis
if ok, _ := rl.TryAcquire(ctx, 1); !ok {
    // limité
}
```

### RBatch (pipeline)

```go
batch := client.NewBatch()
m := batch.GetMap("my-map")
for i := 0; i < 1000; i++ {
    _ = m.Put(ctx, fmt.Sprint(i), i) // en file ; pas encore de round-trip réseau
}
if err := batch.Execute(ctx); err != nil { // un round-trip pour toutes les écritures
    log.Fatal(err)
}
```

(Les lectures sur structures liées au batch renvoient des zéros avant `Execute` — relisez via une structure normale.)

### Cluster / Sentinel

```go
cfg := redi.DefaultConfig()
cfg.Mode = redi.ModeCluster
cfg.Addrs = []string{"node1:7000", "node2:7001", "node3:7002"}

// ou
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

Trois onglets : **INFO** (métriques serveur), **Locks** (détenteurs / réentrées / TTL), **Limiters** (config et permis restants). Basculer avec `1/2/3` ou `←/→`.

## Développement

```bash
go test ./...          # Redis local (localhost:6379) requis ; skip auto sans Redis
go test -race ./...    # mode CI complet
go test -run TestInterop -v .        # interop Go ↔ redi.py (Python + redi.py ; skip si absent)
go test -run TestJavaInterop -v .    # interop Go ↔ Redisson 4.6.1 (java+mvn ; sonde compilée au 1er run ; job CI dédié)
go test -bench . -benchtime 1s .     # benchmarks
go vet ./...
```

Contrats wire dans `wire_compat_test.go` ; régression cross-langage dans `interop_redipy_test.go` (via redi.py, déjà vérifié bidirectionnellement contre Redisson réel). Tout changement cassant l’interop échoue ici.

## Versions

Voir [CHANGELOG.md](CHANGELOG.md). Depuis v0.2.0, les clés n’utilisent plus le préfixe privé `redi:` (changement cassant pour l’interop). Processus de contribution : [CONTRIBUTING.md](CONTRIBUTING.md).
