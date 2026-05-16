package dedup

import (
	"context"
	"errors"
	"time"
)

// EventKind identifies the type of internal event emitted by [Deduplicator].
type EventKind string

const (
	// EventCacheHit is fired when the fast-path cache lookup returns a result.
	EventCacheHit EventKind = "cache_hit"
	// EventCacheMiss is fired when the fast-path cache lookup finds nothing.
	EventCacheMiss EventKind = "cache_miss"
	// EventOriginal is fired when this caller becomes the original executor.
	// fn will be called next.
	EventOriginal EventKind = "original"
	// EventInnerCacheHit is fired when the inner (post-lock) cache check
	// finds a previously stored result, so fn is skipped.
	EventInnerCacheHit EventKind = "inner_cache_hit"
	// EventDuplicate is fired when this caller was a duplicate waiter and the
	// original has now completed.
	EventDuplicate EventKind = "duplicate"
)

// Event carries information about a single internal decision made by
// [Deduplicator.Do]. Attach a handler via [Options.OnEvent] to observe
// cache hits, coordinator decisions, and fn invocations.
type Event struct {
	Kind EventKind
	Key  string
	// Err is non-nil only when Kind == EventDuplicate and the original call
	// failed (the error has already been returned to the caller; this field
	// is informational only).
	Err error
}

// Options configures [Deduplicator] behaviour.
type Options struct {
	// ResultTTL controls how long a completed result is kept in [ResultStore].
	// Requests arriving within this window after the original finishes are
	// served from cache without re-executing the handler.
	// Default: 30s.
	ResultTTL time.Duration

	// Now overrides the wall clock used to stamp [Envelope.CreatedAt] and to
	// calculate store TTLs. Useful in tests. Default: time.Now.
	Now func() time.Time

	// OnEvent, if non-nil, is called synchronously for every notable internal
	// decision: cache hits/misses, coordinator role (original vs duplicate),
	// and inner-cache hits that skip fn. Useful for metrics and debugging.
	// The callback must not block for long; it runs in the caller's goroutine.
	OnEvent func(e Event)
}

func (o *Options) withDefaults() Options {
	opt := Options{
		ResultTTL: 30 * time.Second,
		Now:       time.Now,
	}
	if o == nil {
		return opt
	}
	if o.ResultTTL > 0 {
		opt.ResultTTL = o.ResultTTL
	}
	if o.Now != nil {
		opt.Now = o.Now
	}
	if o.OnEvent != nil {
		opt.OnEvent = o.OnEvent
	}
	return opt
}

// Deduplicator deduplicates concurrent identical requests.
//
// Create one with [New] and call [Deduplicator.Do] from your request handler.
// The zero value is not usable; always use [New].
type Deduplicator struct {
	store       ResultStore
	coordinator Coordinator
	opt         Options
}

// New creates a [Deduplicator] backed by the provided [ResultStore] and
// [Coordinator]. opt may be nil; defaults are used in that case.
//
// Typical combinations:
//
//	// Single-process deployment
//	dedup.New(inmemory.New(), singleflight.New(), nil)
//
//	// Multi-instance deployment (Redis required)
//	dedup.New(redistore.New(rdb, "prefix:"), rediscoord.New(rdb, nil), nil)
func New(store ResultStore, coordinator Coordinator, opt *Options) *Deduplicator {
	return &Deduplicator{
		store:       store,
		coordinator: coordinator,
		opt:         opt.withDefaults(),
	}
}

// Do executes fn at most once per key within the [Options.ResultTTL] window.
//
// Behaviour:
//   - If a result for key is already cached in [ResultStore], it is returned
//     immediately without calling fn.
//   - If an identical key is currently in-flight, Do blocks until the original
//     finishes and then returns the same [Envelope].
//   - Otherwise fn is executed as the "original", its result is stored in
//     [ResultStore], and returned.
//
// fn receives a context derived from ctx that preserves ctx values while
// ignoring ctx cancellation. Cancelling ctx interrupts duplicate waiters but
// does not abort an already-running original.
//
// The returned [Envelope] is always a fresh deep copy; callers may mutate it
// freely without affecting cached data.
func (d *Deduplicator) Do(
	ctx context.Context,
	key string,
	fn func(context.Context) (*Envelope, error),
) (*Envelope, error) {
	if key == "" {
		return nil, errors.New("dedup: key is empty")
	}

	// Fast path: result already in store.
	if cached, err := d.store.Get(ctx, key); err == nil {
		if d.opt.OnEvent != nil {
			d.opt.OnEvent(Event{
				Kind: EventCacheHit,
				Key:  key,
			})
		}
		return cached.clone(), nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	if d.opt.OnEvent != nil {
		d.opt.OnEvent(Event{
			Kind: EventCacheMiss,
			Key:  key,
		})
	}

	result, err := d.coordinator.Run(ctx, key, func(execCtx context.Context) (*Envelope, error) {
		// Store operations use a detached context so that a cancelled or
		// expired caller context (e.g. gRPC deadline) does not prevent the
		// result from being written to the store. A failed store.Set would
		// release the coordinator lock without caching the result, allowing
		// the next caller to become the new original and execute fn again.
		storeCtx := context.WithoutCancel(execCtx)

		// Another goroutine may have stored the result while we were acquiring
		// the coordinator lock; avoid redundant fn calls.
		if cached, cacheErr := d.store.Get(storeCtx, key); cacheErr == nil {
			if d.opt.OnEvent != nil {
				d.opt.OnEvent(Event{
					Kind: EventInnerCacheHit,
					Key:  key,
				})
			}
			return cached.clone(), nil
		} else if !errors.Is(cacheErr, ErrNotFound) {
			return nil, cacheErr
		}

		if d.opt.OnEvent != nil {
			d.opt.OnEvent(Event{
				Kind: EventOriginal,
				Key:  key,
			})
		}

		res, callErr := fn(execCtx)
		if callErr != nil {
			return nil, callErr
		}
		if res == nil {
			return nil, errors.New("dedup: fn returned nil envelope")
		}

		res = res.clone()
		res.CreatedAt = d.opt.Now().UTC()
		if setErr := d.store.Set(storeCtx, key, res, d.opt.ResultTTL); setErr != nil {
			return nil, setErr
		}
		return res, nil
	})

	if err == nil {
		return result.clone(), nil
	}
	if !errors.Is(err, ErrWaitCompleted) {
		return nil, err
	}

	// We were a duplicate waiter; the original has stored the result.
	// Use a detached context: the caller's context may have been cancelled
	// by the time we reach this point, but the result is already in the
	// store so the read must succeed regardless.
	cached, fetchErr := d.store.Get(context.WithoutCancel(ctx), key)
	if fetchErr != nil {
		return nil, fetchErr
	}
	if d.opt.OnEvent != nil {
		d.opt.OnEvent(Event{Kind: EventDuplicate, Key: key})
	}
	return cached.clone(), nil
}
