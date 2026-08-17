# Contributing to redi.go

## Before you change code

1. Read [AGENTS.md](AGENTS.md) (wire 铁律、不做清单、测试约定).
2. For any structure that stores Redis keys / Lua / codec values: start from
   Java Redisson 4.6.x sources under `C:/GitHub/redisson` (or upstream),
   **not** from redi.py alone.
3. Keep docs in sync: `README.md` / `README_EN.md`, `COMPATIBILITY.md`,
   `CHANGELOG.md`, and the execution status block in `演进方案.md`.

## Local checks

```bash
go vet ./...
go test -race ./... -count=1 -timeout 600s   # needs Redis :6379
golangci-lint run ./... --timeout 300s
```

Optional interop (skipped when tools are missing):

```bash
go test -run TestInterop -v .         # Python + redi.py
go test -run TestJavaInterop -v .     # Java 25 + Maven; builds interop/java-probe
```

CI runs the Go race suite and a dedicated `java-interop` job.

## Adding a structure

1. Embed `rObject`; encode via `c.codec`; Lua via `redis.NewScript`.
2. Companion keys through `prefixName` / `suffixName` (cluster hash-tags).
3. Factory on `Client` (Redisson name + alias if useful).
4. Tests: unit + wire contract in `wire_compat*_test.go` when layout is
   custom + Java probe command + `TestJavaInterop_*` when mutually readable.
5. Register the row in `COMPATIBILITY.md`.

## Wire changes

Any change to key names, channel names, member packing, or codec wrappers
must update or add a `TestWire_*` assertion. CI has no JVM on the main
`test` job — wire contracts are the everyday interop guardrail.

## API stability

`v0.2.x` is pre-1.0: breaking changes are allowed when needed for Redisson
wire correctness, and must be called out in `CHANGELOG.md`. Prefer additive
APIs (`GetInto`, `HolderID`) over silent semantic drift.
