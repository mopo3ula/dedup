# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

### Added

- Core `Deduplicator` with `Do(ctx, key, fn)` API.
- `Coordinator` interface with two built-in implementations:
    - `coordinator/singleflight` – in-process, no external dependencies.
    - `coordinator/redis` – distributed via Redis SET NX + Pub/Sub.
- `ResultStore` interface with two built-in implementations:
    - `store/inmemory` – in-process map with TTL eviction.
    - `store/redis` – Redis-backed, JSON-serialised results.
- `key` package with `FromParts`, `FromJSON`, `FromMap` helpers.
- Full test suite including concurrency, cache-hit, TTL-expiry, and benchmark tests.

