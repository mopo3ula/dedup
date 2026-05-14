# dedup

Transparent request deduplication for Go services.
When multiple identical requests arrive concurrently, only the **first** one
is executed. All duplicates block and receive **the same result** the moment
the original finishes. Subsequent requests within the TTL window are answered
**instantly from cache** — the handler is never called again.

## Features

- **Transport-agnostic** — works with HTTP, gRPC, message queues, or any other transport.
- **Two coordination strategies** — swap with a one-line change:
    - `coordinator/singleflight` — in-process, zero external dependencies.
    - `coordinator/redis` — distributed across multiple instances.
- **Two result stores** — also swappable independently:
    - `store/inmemory` — in-process, zero external dependencies.
    - `store/redis` — shared across all instances.
- **Extensible** — implement `Coordinator` or `ResultStore` to plug in any backend.
- **Safe** — deep-copies are returned to callers; cached data is never mutated.

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
    d := dedup.New(
        memstore.New(),
        sfcoord.New(),
        &dedup.Options{ResultTTL: 30 * time.Second},
    )

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

    d := dedup.New(
        redistore.New(rdb, "myapp:result:"),
        rediscoord.New(rdb, &rediscoord.Options{Prefix: "myapp"}),
        &dedup.Options{ResultTTL: 30 * time.Second},
    )

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
        // This function is called at most once per key per ResultTTL window.
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
Request C ──► Do(ctx, key, fn) ──► ResultStore.Get ─────────────────────────────────── respond instantly (cached)
```

1. **A** acquires the coordinator lock and runs `fn`.
2. **B** arrives while **A** is running — blocks in the coordinator.
3. **A** finishes → stores result in `ResultStore` → releases lock → notifies **B**.
4. **B** wakes up, reads result from `ResultStore`, returns the same `Envelope`.
5. **C** arrives after **A** finishes → `ResultStore.Get` returns immediately (within TTL).

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
| `ResultTTL` | `30s`      | How long a completed result is kept in `ResultStore`. |
| `Now`       | `time.Now` | Clock override — useful in tests.                     |

Redis coordinator options (`coordinator/redis.Options`):

| Option     | Default   | Description                                                            |
|------------|-----------|------------------------------------------------------------------------|
| `LockTTL`  | `5s`      | Max time the distributed lock is held. Must exceed worst-case latency. |
| `WaitStep` | `20ms`    | Polling interval for the fallback lock-exists check.                   |
| `Prefix`   | `"dedup"` | Redis key namespace.                                                   |

## Requirements

- Go 1.24+
- Redis 7+ (only for `coordinator/redis` and `store/redis`)

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

