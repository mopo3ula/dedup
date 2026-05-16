// Package dedup provides transparent request deduplication for any transport.
//
// # Overview
//
// When multiple identical requests with the same deduplication key arrive
// concurrently, one caller becomes the "original" and executes the handler.
// Duplicates block until the original finishes and receive the same [Envelope].
// Subsequent requests within the ResultTTL window are served instantly from the
// [ResultStore] without invoking the handler again.
//
// # Guarantees and limits
//
// Deduplication is based on the key and the selected coordinator, not on wall
// clock timestamp comparison. Calls that arrive nanoseconds apart with the same
// key are coalesced by the in-process singleflight group or by the Redis SET NX
// distributed lock. In multi-instance deployments, every instance must share
// the same Redis coordinator namespace and result-store namespace.
//
// For the Redis coordinator, [coordinator/redis.Options.LockTTL] is a renewable
// Redis lease, not a hard limit on handler runtime. It must be long enough to
// survive short Redis hiccups and scheduler pauses between renewals. If the
// process crashes or the lease cannot be renewed until it expires, another
// caller can acquire the lock and execute the handler again. The package
// deduplicates request handling; it does not by itself provide end-to-end
// exactly-once side effects across process crashes, Redis outages, or
// non-idempotent upstream operations.
//
// # Core abstractions
//
//   - [Coordinator] – decides which call is the "original" and blocks duplicates.
//   - [ResultStore] – persists completed results for fast fan-out.
//   - [Envelope] – transport-agnostic container for the response payload and metadata.
//   - [Deduplicator] – wires the above together; the single entry point via [Deduplicator.Do].
//
// # Built-in implementations
//
// Coordinators:
//   - [coordinator/singleflight] – in-process, based on golang.org/x/sync/singleflight.
//     Use for single-instance deployments.
//   - [coordinator/redis] – distributed, Redis SET NX + Pub/Sub.
//     Use for horizontally-scaled deployments.
//
// Stores:
//   - [store/inmemory] – in-process map with TTL eviction. No external dependencies.
//   - [store/redis]    – Redis-backed, JSON-serialised results.
//
// # Quick start
//
// Single process (no external dependencies):
//
//	d, err := dedup.New(
//	    inmemory.New(),
//	    singleflight.New(),
//	    &dedup.Options{ResultTTL: 30 * time.Second},
//	)
//	if err != nil {
//	    return err
//	}
//
// Multi-instance (shared Redis):
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
//	d, err := dedup.New(
//	    redistore.New(rdb, "myapp:result:"),
//	    rediscoord.New(rdb, nil),
//	    &dedup.Options{ResultTTL: 30 * time.Second},
//	)
//	if err != nil {
//	    return err
//	}
//
// Both variants expose the same [Deduplicator.Do] API — switching is a one-line
// change in the initialisation code.
//
// # Integration
//
// Wrap your handler with [Deduplicator.Do] and build the dedup key from
// whatever uniquely identifies a request (URL, body hash, user ID, etc.):
//
//	env, err := d.Do(ctx, key, func(ctx context.Context) (*dedup.Envelope, error) {
//	    result, err := callUpstream(ctx, req)
//	    if err != nil {
//	        return nil, err
//	    }
//	    return &dedup.Envelope{Payload: result}, nil
//	})
//
// Use [key.FromParts] or [key.FromJSON] to derive stable, collision-resistant keys.
// [key.FromJSON] returns an error when a value cannot be marshalled by encoding/json
// (for example, when it contains chan or func fields), so callers should handle it
// explicitly.
package dedup
