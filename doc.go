// Package dedup provides transparent request deduplication for any transport.
//
// # Overview
//
// When multiple identical requests arrive concurrently, only the first one
// (the "original") is executed. All duplicates block until the original
// finishes and receive exactly the same [Envelope]. Subsequent requests
// within the ResultTTL window are served instantly from the [ResultStore]
// without invoking the handler again.
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
//	d := dedup.New(
//	    inmemory.New(),
//	    singleflight.New(),
//	    &dedup.Options{ResultTTL: 30 * time.Second},
//	)
//
// Multi-instance (shared Redis):
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
//	d := dedup.New(
//	    redistore.New(rdb, "myapp:result:"),
//	    rediscoord.New(rdb, nil),
//	    &dedup.Options{ResultTTL: 30 * time.Second},
//	)
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
package dedup
