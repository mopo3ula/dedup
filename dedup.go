package dedup

import (
	"context"
	"errors"
	"reflect"
	"time"
)

// EventKind identifies the type of internal event emitted by [Deduplicator].
type EventKind string

const (
	// EventCacheHit is kept for backwards compatibility but is no longer emitted.
	//
	// Deprecated: dedup no longer caches results beyond the in-flight window.
	EventCacheHit EventKind = "cache_hit"
	// EventCacheMiss is kept for backwards compatibility but is no longer emitted.
	//
	// Deprecated: dedup no longer caches results beyond the in-flight window.
	EventCacheMiss EventKind = "cache_miss"
	// EventOriginal is fired when this caller becomes the original executor.
	// fn will be called next.
	EventOriginal EventKind = "original"
	// EventInnerCacheHit is kept for backwards compatibility but is no longer emitted.
	//
	// Deprecated: dedup no longer caches results beyond the in-flight window.
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
	// The store is used by coordinators (such as [coordinator/redis]) to share
	// the result with in-flight duplicate waiters across instances. Requests
	// arriving after the in-flight group completes always re-execute the handler.
	// Default: 30s.
	ResultTTL time.Duration

	// Now overrides the wall clock used to stamp [Envelope.CreatedAt] and to
	// calculate store TTLs. Useful in tests. Default: time.Now.
	Now func() time.Time

	// OnEvent, if non-nil, is called synchronously for every notable internal
	// decision: coordinator role (original vs duplicate). Useful for metrics
	// and debugging. The callback must not block for long; it runs in the
	// caller's goroutine.
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
// The zero value is not usable; always use [New] or [MustNew].
type Deduplicator struct {
	store       ResultStore
	coordinator Coordinator
	opt         Options
}

var (
	// ErrNilResultStore is returned by [New] when store is nil.
	ErrNilResultStore = errors.New("dedup: nil ResultStore")
	// ErrNilCoordinator is returned by [New] when coordinator is nil.
	ErrNilCoordinator = errors.New("dedup: nil Coordinator")
	// ErrNilHandler is returned by [Deduplicator.Do] when fn is nil.
	ErrNilHandler = errors.New("dedup: nil handler")
)

// New creates a [Deduplicator] backed by the provided [ResultStore] and
// [Coordinator]. opt may be nil; defaults are used in that case.
// It returns an error if store or coordinator is nil.
//
// Typical combinations:
//
//	// Single-process deployment
//	d, err := dedup.New(inmemory.New(), singleflight.New(), nil)
//
//	// Multi-instance deployment (Redis required)
//	d, err := dedup.New(redistore.New(rdb, "prefix:"), rediscoord.New(rdb, nil), nil)
func New(store ResultStore, coordinator Coordinator, opt *Options) (*Deduplicator, error) {
	if isNilDependency(store) {
		return nil, ErrNilResultStore
	}
	if isNilDependency(coordinator) {
		return nil, ErrNilCoordinator
	}

	return &Deduplicator{
		store:       store,
		coordinator: coordinator,
		opt:         opt.withDefaults(),
	}, nil
}

// MustNew is like [New], but panics if the deduplicator cannot be created.
// Use MustNew only when nil dependencies are a programmer error that should
// stop the process during startup.
func MustNew(store ResultStore, coordinator Coordinator, opt *Options) *Deduplicator {
	d, err := New(store, coordinator, opt)
	if err != nil {
		panic(err)
	}
	return d
}

// Do deduplicates concurrent calls with the same key.
// It returns an error if key is empty or fn is nil.
//
// Behaviour:
//   - If an identical key is currently in-flight, Do blocks until the original
//     finishes and then returns the same [Envelope].
//   - Otherwise fn is executed as the "original" and its result is returned
//     to all concurrent waiters.
//   - Requests that arrive after the in-flight group completes always call fn
//     again as a new original.
//
// fn receives a context derived from ctx that preserves ctx values while
// ignoring ctx cancellation. Cancelling ctx interrupts duplicate waiters but
// does not abort an already-running original.
//
// The returned [Envelope] is always a fresh deep copy; callers may mutate it
// freely without affecting other callers.
func (d *Deduplicator) Do(
	ctx context.Context,
	key string,
	fn func(context.Context) (*Envelope, error),
) (*Envelope, error) {
	if fn == nil {
		return nil, ErrNilHandler
	}
	if key == "" {
		return nil, errors.New("dedup: key is empty")
	}

	result, err := d.coordinator.Run(ctx, key, func(execCtx context.Context) (*Envelope, error) {
		if d.opt.OnEvent != nil {
			d.opt.OnEvent(Event{Kind: EventOriginal, Key: key})
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

		// Store the result so that in-flight duplicate waiters on other
		// instances (e.g. coordinator/redis) can fetch it. A detached context
		// is used so that a cancelled caller context (e.g. gRPC deadline) does
		// not prevent the write.
		storeCtx := context.WithoutCancel(execCtx)
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

	// ErrWaitCompleted: we were a duplicate waiter and the original has stored
	// the result. Use a detached context — the caller's context may have been
	// cancelled by the time we reach this point.
	cached, fetchErr := d.store.Get(context.WithoutCancel(ctx), key)
	if fetchErr != nil {
		return nil, fetchErr
	}
	if d.opt.OnEvent != nil {
		d.opt.OnEvent(Event{Kind: EventDuplicate, Key: key})
	}
	return cached.clone(), nil
}

func isNilDependency(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
