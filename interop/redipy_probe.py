"""redi.go <-> redi.py wire interop probe.

Usage: python redipy_probe.py <command> [args...]
Each command prints one JSON line on stdout. Errors exit non-zero.

redipy's wire formats were themselves verified bidirectionally against real
Java Redisson 4.6.1, so passing these probes transitively validates the Go
implementation against Redisson.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.environ.get("REDIPY_HOME", "C:/GitHub/redi.py"))

import redis  # noqa: E402
from redipy import (  # noqa: E402
    RAtomicLong,
    RBloomFilter,
    RCountDownLatch,
    RDelayedQueue,
    RListMultimap,
    RLock,
    RMap,
    RRateLimiter,
    RSemaphore,
    RSetMultimap,
)

HOST = os.environ.get("REDIS_HOST", "localhost")
PORT = int(os.environ.get("REDIS_PORT", "6379"))

r = redis.Redis(host=HOST, port=PORT, decode_responses=True)


def emit(obj: dict) -> None:
    print(json.dumps(obj), flush=True)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: redipy_probe.py <command> [args...]")
    cmd = sys.argv[1]
    a = sys.argv[2:]

    if cmd == "ping":
        r.ping()
        emit({"ok": True})

    elif cmd == "lock_hold":
        # Acquire, report, hold until stdin closes, then release in this
        # same process (the lock field embeds this process's thread id).
        name, hold_id = a[0], a[1]
        lock = RLock(r, name, client_id=hold_id)
        acquired = lock.try_lock(wait_time_ms=0, lease_time_ms=60000)
        emit({"acquired": acquired})
        if acquired:
            sys.stdin.read()
            lock.unlock()
            emit({"released": True})

    elif cmd == "lock_acquire":
        # One-shot: acquire and immediately release within this process.
        name, hold_id = a[0], a[1]
        lock = RLock(r, name, client_id=hold_id)
        acquired = lock.try_lock(wait_time_ms=0, lease_time_ms=60000)
        if acquired:
            lock.unlock()
        emit({"acquired": acquired})

    elif cmd == "map_put":
        m = RMap(r, a[0])
        m.put(a[1], json.loads(a[2]))
        emit({"ok": True})

    elif cmd == "map_get":
        m = RMap(r, a[0])
        emit({"value": m.get(a[1])})

    elif cmd == "along_add":
        n = RAtomicLong(r, a[0]).add_and_get(int(a[1]))
        emit({"value": n})

    elif cmd == "along_get":
        emit({"value": RAtomicLong(r, a[0]).get()})

    elif cmd == "bloom_init":
        f = RBloomFilter(r, a[0])
        emit({"ok": f.try_init(int(a[1]), float(a[2]))})

    elif cmd == "bloom_add":
        emit({"added": RBloomFilter(r, a[0]).add(json.loads(a[1]))})

    elif cmd == "bloom_contains":
        emit({"contains": RBloomFilter(r, a[0]).contains(json.loads(a[1]))})

    elif cmd == "rate_set":
        rl = RRateLimiter(r, a[0])
        emit({"ok": rl.try_set_rate(int(a[1]), int(a[2]))})

    elif cmd == "rate_try":
        emit({"acquired": RRateLimiter(r, a[0]).try_acquire(int(a[1]))})

    elif cmd == "sem_set":
        emit({"ok": RSemaphore(r, a[0]).try_set_permits(int(a[1]))})

    elif cmd == "sem_try":
        emit({"acquired": RSemaphore(r, a[0]).try_acquire(int(a[1]))})

    elif cmd == "sem_release":
        RSemaphore(r, a[0]).release(int(a[1]))
        emit({"ok": True})

    elif cmd == "latch_set":
        emit({"ok": RCountDownLatch(r, a[0]).try_set_count(int(a[1]))})

    elif cmd == "latch_count_down":
        RCountDownLatch(r, a[0]).count_down()
        emit({"ok": True})

    elif cmd == "latch_count":
        emit({"count": RCountDownLatch(r, a[0]).get_count()})

    elif cmd == "dq_offer":
        RDelayedQueue(r, a[0]).offer(json.loads(a[1]), int(a[2]))
        emit({"ok": True})

    elif cmd == "dq_delayed_size":
        size = r.zcard(f"redisson_delay_queue:{{{a[0]}}}")
        emit({"size": size})

    elif cmd == "setmm_put":
        m = RSetMultimap(r, a[0])
        emit({"ok": m.put(json.loads(a[1]), json.loads(a[2]))})

    elif cmd == "setmm_get":
        m = RSetMultimap(r, a[0])
        emit({"values": sorted(m.get(json.loads(a[1])))})

    elif cmd == "setmm_internal_id":
        # Deterministic Hash.hash128toBase64 of the codec-encoded key.
        m = RSetMultimap(r, a[0])
        emit({"id": m._internal_id(json.loads(a[1]))})

    elif cmd == "listmm_put":
        m = RListMultimap(r, a[0])
        emit({"ok": m.put(json.loads(a[1]), json.loads(a[2]))})

    elif cmd == "listmm_get":
        m = RListMultimap(r, a[0])
        emit({"values": m.get(json.loads(a[1]))})

    else:
        raise SystemExit(f"unknown command: {cmd}")


if __name__ == "__main__":
    main()
