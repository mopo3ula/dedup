# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

### Changed

- **Breaking behaviour**: results are no longer cached between in-flight groups.
  Requests arriving after the in-flight group completes always execute the handler
  again as a new original. Previously, results were cached in `ResultStore` for
  `ResultTTL` (default 30 s) and sequential requests within that window were
  served without calling the handler.
- `ResultStore` now acts as a transient channel for passing the result to
  in-flight duplicate waiters on other instances (`coordinator/redis`); it is not
  a cache for future requests.
- `EventCacheHit`, `EventCacheMiss`, and `EventInnerCacheHit` are kept as
  exported constants for backwards compatibility but are no longer emitted.

### Added

- Core `Deduplicator` with `Do(ctx, key, fn)` API.
- `Coordinator` interface with two built-in implementations:
    - `coordinator/singleflight` – in-process, no external dependencies.
    - `coordinator/redis` – distributed via Redis SET NX + Pub/Sub.
- `ResultStore` interface with two built-in implementations:
    - `store/inmemory` – in-process map with TTL eviction.
    - `store/redis` – Redis-backed, JSON-serialised results.
- `key` package with `FromParts`, `FromJSON`, `FromMap` helpers.
- Full test suite including concurrency and benchmark tests.

