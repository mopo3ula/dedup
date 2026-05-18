# dedup

Transparent request deduplication for Go services.
When multiple identical requests with the same deduplication key arrive
concurrently, only one request becomes the **original** and executes the
handler. All duplicates block and receive **the same result** the moment the
original finishes. Later requests execute the handler again after the
coordinator's in-flight window has closed; completed results are only a
short-lived duplicate hand-off, not a long-lived response cache.

## Features

- **Transport-agnostic** — works with HTTP, gRPC, message queues, or any other transport.
- **Two coordination strategies** — swap with a one-line change:
    - `coordinator/singleflight` — in-process, zero external dependencies.
    - `coordinator/redis` — distributed across multiple instances.
- **Two result stores** — also swappable independently:
    - `store/inmemory` — in-process, zero external dependencies.
    - `store/redis` — shared across all instances.
- **Extensible** — implement `Coordinator` or `ResultStore` to plug in any backend.
- **Safe** — deep-copies are returned to callers; stored hand-off data is never mutated.

## Installation

```bash
go get github.com/mopo3ula/dedup
```

## Quick start

### Single process (no external dependencies)

```go
package main

import (
    "time"

    "github.com/mopo3ula/dedup"
    sfcoord "github.com/mopo3ula/dedup/coordinator/singleflight"
    memstore "github.com/mopo3ula/dedup/store/inmemory"
)

func main() {
    d, err := dedup.New(
        memstore.New(),
        sfcoord.New(),
        &dedup.Options{ResultTTL: 30 * time.Second},
    )
    if err != nil {
        panic(err)
    }

    _ = d // use in your handlers
}
```

### Multi-instance (shared Redis)

```go
package main

import (
    "time"

    "github.com/mopo3ula/dedup"
    rediscoord "github.com/mopo3ula/dedup/coordinator/redis"
    redistore "github.com/mopo3ula/dedup/store/redis"
    goredis "github.com/redis/go-redis/v9"
)

func main() {
    rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})

    d, err := dedup.New(
        redistore.New(rdb, "myapp:result:"),
        rediscoord.New(rdb, &rediscoord.Options{Prefix: "myapp"}),
        &dedup.Options{ResultTTL: 30 * time.Second},
    )
    if err != nil {
        panic(err)
    }

    _ = d // use in your handlers
}
```
Both variants expose **the same API**. Switching is a one-line change in
the initialisation code; all callers of `d.Do(...)` remain untouched.

## Usage

```go
package handlers

import (
    "context"
    "time"

    "github.com/mopo3ula/dedup"
    "github.com/mopo3ula/dedup/key"
)

// Build a stable key from whatever uniquely identifies the request.
func handle(ctx context.Context, d *dedup.Deduplicator, userID, action string, requestBody []byte) ([]byte, error) {
    k := key.FromParts(userID, action, string(requestBody))

    env, err := d.Do(ctx, k, func(ctx context.Context) (*dedup.Envelope, error) {
        // This function is called at most once per key while a call is in-flight.
        result, err := callExpensiveUpstream(ctx)
        if err != nil {
            return nil, err
        }
        return &dedup.Envelope{
            Payload: result,
            Meta:    map[string]string{"content-type": "application/json"},
        }, nil
    })
    if err != nil {
        return nil, err
    }
    // env.Payload contains the result — identical for all concurrent duplicates.
    return env.Payload, nil
}

// callExpensiveUpstream is a placeholder for the real business call.
func callExpensiveUpstream(ctx context.Context) ([]byte, error) {
    time.Sleep(50 * time.Millisecond)
    return []byte(`{"ok":true}`), nil
}
```

## Package layout

```
github.com/mopo3ula/dedup
├── dedup.go                   Deduplicator, Options, Do
├── envelope.go                Envelope (transport-agnostic response container)
├── coordinator.go             Coordinator interface + ErrWaitCompleted
├── store.go                   ResultStore interface + ErrNotFound
├── coordinator/
│   ├── singleflight/          In-process coordinator (golang.org/x/sync/singleflight)
│   └── redis/                 Distributed coordinator (Redis SET NX + Pub/Sub)
├── store/
│   ├── inmemory/              In-process store (sync.RWMutex + TTL)
│   └── redis/                 Redis store (JSON serialisation)
└── key/                       Key-building helpers (SHA-256)
```

## How it works

```
Request A ──► Do(ctx, key, fn) ──► Coordinator.Run ──► fn() executes ──► ResultStore.Set ──► respond
Request B ──► Do(ctx, key, fn) ──► Coordinator.Run ──► blocks ──────────────────────────► respond (same Envelope)
Request C ──► Do(ctx, key, fn) ──► Coordinator.Run ──► fn() executes again ─────────────► respond (new Envelope)
```

1. **A** acquires the coordinator lock and runs `fn`.
2. **B** arrives while **A** is running — blocks in the coordinator.
3. **A** finishes → stores result in `ResultStore` → marks completion → notifies **B**.
4. **B** wakes up, reads result from `ResultStore`, returns the same `Envelope`.
5. **C** arrives after the coordinator's in-flight/completion window has closed, so it becomes a new original and runs `fn` again.

`ResultStore` is only a short-lived hand-off channel for duplicate waiters.
`ResultTTL` controls how long that hand-off remains readable; it is not a
long-lived response-cache window. With the Redis coordinator, keep `ResultTTL`
longer than `coordinator/redis.Options.CompletionTTL` so callers suppressed by
the post-completion burst window can still read the stored result.

Correctness does not rely on artificial sleeps or millisecond-sized gaps between
requests: callers that arrive nanoseconds apart are coordinated by the
coordinator's lock/singleflight primitive, not by timestamp comparison. The
Redis coordinator additionally keeps a short completed marker after a successful
original finishes. That marker closes the race where a very fast handler releases
the active lock before all members of the same request burst have reached Redis.

## Guarantees and limits

For a given deduplication key, `dedup` coalesces concurrent calls as follows:

- **Single process:** `coordinator/singleflight` makes one in-flight caller run
  `fn`; duplicate callers in the same process wait for that result.
- **Multiple instances:** `coordinator/redis` uses Redis `SET NX` as a shared
  distributed lock, so all instances that use the same Redis client namespace
  compete for one original caller.
- **After success:** a later call is a new original after the coordinator's
  in-flight window has closed, even before `ResultTTL` expires. For Redis, the
  default post-completion burst window is `CompletionTTL` (`100ms`).
- **Nanosecond-close arrivals:** if five requests with the same key enter
  `Do` at effectively the same time, one becomes the original and the other
  four wait/read the stored result; the handler is not selected by comparing
  timestamps.

The guarantee depends on these operational assumptions:

- All duplicates must use the same stable key for the same logical request.
- Multi-instance deployments must share the same Redis coordinator prefix and
  Redis-backed result store prefix.
- `coordinator/redis.Options.LockTTL` is a renewable Redis lease, not a hard
  limit on handler runtime. It must be long enough to survive short Redis
  hiccups and scheduler pauses between renewals. If the process crashes or the
  lease cannot be renewed until it expires, another caller can acquire the lock
  and execute `fn` again.
- `coordinator/redis.Options.CompletionTTL` keeps a completed marker briefly
  after Redis originals finish. Increase it if same-burst callers routinely
  arrive at Redis after very fast handlers finish; keep `dedup.Options.ResultTTL`
  greater than this value.
- If the original returns an error, its error is propagated to waiters and no
  successful result is stored for hand-off.
- If a process crashes or loses Redis connectivity mid-flight, the lock TTL is
  the recovery mechanism; use an idempotent business operation when you need
  end-to-end exactly-once side effects.

## Extending

### Custom coordinator

```go
package custom

import "github.com/mopo3ula/dedup"

type MyCoordinator struct{}

func (c *MyCoordinator) Run(
    ctx context.Context,
    key string,
    fn func(context.Context) (*dedup.Envelope, error),
) (*dedup.Envelope, error) {
    // your coordination logic
    return fn(ctx)
}
```

### Custom store

```go
package custom

import (
    "context"
    "time"

    "github.com/mopo3ula/dedup"
)

type MyStore struct{}

func (s *MyStore) Get(ctx context.Context, key string) (*dedup.Envelope, error) {
    // return dedup.ErrNotFound if absent or expired
    return nil, dedup.ErrNotFound
}

func (s *MyStore) Set(ctx context.Context, key string, value *dedup.Envelope, ttl time.Duration) error {
    // persist value
    return nil
}
```

## Key helpers

```go
package example

import (
    "fmt"

    "github.com/mopo3ula/dedup/key"
)

func examples() {
    k1 := key.FromParts("POST", "/api/pay", "{...body...}") // variadic parts → SHA-256
    fmt.Println(k1)

    k2 := key.FromMap(map[string]string{"uid": "1", "op": "pay"}) // sorted map → SHA-256
    fmt.Println(k2)

    // FromJSON may return a non-deterministic error if the value contains unsupported types.
    if k3, err := key.FromJSON(map[string]any{"a": 1}); err == nil {
        fmt.Println(k3)
    }
}
```

## Configuration

| Option      | Default    | Description                                           |
|-------------|------------|-------------------------------------------------------|
| `ResultTTL` | `30s`      | How long a completed result is kept in `ResultStore` for in-flight duplicate waiters; not a response-cache TTL. |
| `Now`       | `time.Now` | Clock override — useful in tests.                     |

Redis coordinator options (`coordinator/redis.Options`):

| Option     | Default   | Description                                                            |
|------------|-----------|------------------------------------------------------------------------|
| `LockTTL`       | `5s`      | Redis lock lease duration. The original renews it while `fn` runs; choose a value long enough for short Redis hiccups and scheduler pauses. |
| `WaitStep`      | `20ms`    | Polling interval for the fallback lock/completion check.               |
| `Prefix`        | `"dedup"` | Redis key namespace.                                                   |
| `CompletionTTL` | `100ms`   | Short post-completion window that suppresses second originals from the same near-simultaneous burst; keep `ResultTTL` greater than this. |

## Requirements

- Go 1.24+
- Redis 7+ (only for `coordinator/redis` and `store/redis`)

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

