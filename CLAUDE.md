# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build / quality
make vet          # go vet ./...
make fmt          # gofmt -w .
make lint         # golangci-lint run

# Tests
make test         # all tests, race detector (unit only — no Redis needed)
make test-unit    # subset: root + singleflight + inmemory + key packages
make test-redis   # integration tests (requires Redis); set REDIS_ADDR if non-default
make bench        # benchmarks

# Single test
go test -race -run TestFoo .

# Redis for integration tests
make docker-redis   # start redis:7 on :6379
make stop-redis     # stop and remove the container
REDIS_ADDR=127.0.0.1:6379 go test -race -count=1 -tags integration ./...
```

Integration tests are gated with the `integration` build tag, so `go test ./...` (no tag) always runs without Redis.

## Architecture

The library deduplicates concurrent identical requests via two pluggable interfaces:

- **`Coordinator`** (`coordinator.go`) — decides which concurrent caller is the "original" and blocks all duplicates. Returns `ErrWaitCompleted` to signal that a waiter should fetch the result from the store rather than use the return value.
- **`ResultStore`** (`store.go`) — holds the result transiently so in-flight duplicate waiters on other instances (`coordinator/redis`) can fetch it. Not a cache: sequential requests after the in-flight group completes always run `fn` again. Returns `ErrNotFound` for absent/expired keys.

**`Deduplicator.Do`** (`dedup.go`) wires them together:
1. `coordinator.Run` — acquire the lock and call `fn` (or block if another caller already holds it). Requests arriving after the in-flight group completes always become new originals and run `fn` again — there is no result cache between flights.
2. Inside the coordinator closure: call `fn`, then `store.Set` the result so in-flight duplicate waiters on other instances (Redis coordinator) can fetch it. `store.Set` uses a detached context so a cancelled caller cannot prevent the write.
3. On `ErrWaitCompleted` (returned by Redis coordinator for waiters): fetch the result from the store using a detached context (caller context may already be cancelled).

### Implementations

| Package | Type | Mechanism |
|---|---|---|
| `coordinator/singleflight` | `Coordinator` | `golang.org/x/sync/singleflight` — in-process only |
| `coordinator/redis` | `Coordinator` | Redis SET NX leader election + Pub/Sub done-channel + periodic polling fallback + lock renewal goroutine (every `LockTTL/3`) |
| `store/inmemory` | `ResultStore` | `sync.RWMutex` map with TTL |
| `store/redis` | `ResultStore` | Redis `SET EX` with JSON serialisation |

The Redis coordinator uses Lua CAS scripts for atomic lock extension and release to prevent accidental deletion by a different owner.

### Key building (`key/`)

`key.FromParts`, `key.FromMap`, and `key.FromJSON` all produce a stable SHA-256 hex string. Use these to build the deduplication key from request parameters.

### Extending

Implement `Coordinator` or `ResultStore` interfaces to plug in any backend. The interfaces are small — one method each. See `coordinator.go` and `store.go` for the contracts.
