package dedup_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mopo3ula/dedup"
	sfcoord "github.com/mopo3ula/dedup/coordinator/singleflight"
	memstore "github.com/mopo3ula/dedup/store/inmemory"
)

// newTestDeduplicator creates a Deduplicator with in-process implementations.
func newTestDeduplicator(ttl time.Duration) *dedup.Deduplicator {
	return dedup.MustNew(memstore.New(), sfcoord.New(), &dedup.Options{ResultTTL: ttl})
}

func TestNewReturnsErrorOnNilStore(t *testing.T) {
	d, err := dedup.New(nil, sfcoord.New(), nil)
	if d != nil {
		t.Fatalf("deduplicator = %#v, want nil", d)
	}
	if !errors.Is(err, dedup.ErrNilResultStore) {
		t.Fatalf("error = %v, want %v", err, dedup.ErrNilResultStore)
	}
}

func TestNewReturnsErrorOnNilCoordinator(t *testing.T) {
	d, err := dedup.New(memstore.New(), nil, nil)
	if d != nil {
		t.Fatalf("deduplicator = %#v, want nil", d)
	}
	if !errors.Is(err, dedup.ErrNilCoordinator) {
		t.Fatalf("error = %v, want %v", err, dedup.ErrNilCoordinator)
	}
}

func TestNewReturnsErrorOnTypedNilDependencies(t *testing.T) {
	var store *memstore.Store
	d, err := dedup.New(store, sfcoord.New(), nil)
	if d != nil {
		t.Fatalf("deduplicator = %#v, want nil", d)
	}
	if !errors.Is(err, dedup.ErrNilResultStore) {
		t.Fatalf("error = %v, want %v", err, dedup.ErrNilResultStore)
	}

	var coordinator *sfcoord.Coordinator
	d, err = dedup.New(memstore.New(), coordinator, nil)
	if d != nil {
		t.Fatalf("deduplicator = %#v, want nil", d)
	}
	if !errors.Is(err, dedup.ErrNilCoordinator) {
		t.Fatalf("error = %v, want %v", err, dedup.ErrNilCoordinator)
	}
}

func TestMustNewPanicsOnNilDependency(t *testing.T) {
	assertPanicError(t, dedup.ErrNilResultStore, func() {
		dedup.MustNew(nil, sfcoord.New(), nil)
	})
}

func TestNilHandlerReturnsMeaningfulError(t *testing.T) {
	d := newTestDeduplicator(time.Second)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Do panicked: %v", r)
		}
	}()

	_, err := d.Do(context.Background(), "nil-handler", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, dedup.ErrNilHandler) {
		t.Fatalf("error = %v, want %v", err, dedup.ErrNilHandler)
	}
}

func assertPanicError(t *testing.T, want error, fn func()) {
	t.Helper()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic %v, got nil", want)
		}
		got, ok := r.(error)
		if !ok {
			t.Fatalf("panic = %#v, want error %v", r, want)
		}
		if !errors.Is(got, want) {
			t.Fatalf("panic = %v, want %v", got, want)
		}
	}()

	fn()
}

// TestDeduplicatesConcurrentCalls verifies that when N goroutines call Do with
// the same key simultaneously, the handler is executed exactly once and every
// caller receives the same result.
func TestDeduplicatesConcurrentCalls(t *testing.T) {
	d := newTestDeduplicator(time.Second)

	var originCalls int64
	handler := func(ctx context.Context) (*dedup.Envelope, error) {
		n := atomic.AddInt64(&originCalls, 1)
		time.Sleep(120 * time.Millisecond)
		return &dedup.Envelope{Payload: []byte{byte(n)}}, nil
	}

	const n = 16
	results := make([]byte, n)

	var wg sync.WaitGroup
	wg.Add(n)
	start := time.Now()
	for i := range n {
		go func(idx int) {
			defer wg.Done()
			env, err := d.Do(context.Background(), "same-key", handler)
			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", idx, err)
				return
			}
			results[idx] = env.Payload[0]
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if got := atomic.LoadInt64(&originCalls); got != 1 {
		t.Fatalf("handler called %d times, want exactly 1", got)
	}
	for i, b := range results {
		if b != 1 {
			t.Fatalf("result[%d] = %d, want 1", i, b)
		}
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("deduplication added too much overhead: %v", elapsed)
	}
}

// TestDeduplicatesNanosecondCloseCalls verifies that correctness does not
// depend on requests being separated by scheduler-scale delays. A duplicate
// that starts one nanosecond after the original still waits for the original
// and receives its result instead of executing the handler again.
func TestDeduplicatesNanosecondCloseCalls(t *testing.T) {
	d := newTestDeduplicator(time.Second)

	var originCalls int64
	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context) (*dedup.Envelope, error) {
		if atomic.AddInt64(&originCalls, 1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return &dedup.Envelope{Payload: []byte("ok")}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	type callResult struct {
		payload string
		err     error
	}
	results := make(chan callResult, 2)

	go func() {
		env, err := d.Do(context.Background(), "nanosecond-key", handler)
		if err != nil {
			results <- callResult{err: err}
			return
		}
		results <- callResult{payload: string(env.Payload)}
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	time.Sleep(time.Nanosecond)

	go func() {
		env, err := d.Do(context.Background(), "nanosecond-key", handler)
		if err != nil {
			results <- callResult{err: err}
			return
		}
		results <- callResult{payload: string(env.Payload)}
	}()

	close(release)

	for range 2 {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("unexpected error: %v", res.err)
			}
			if res.payload != "ok" {
				t.Fatalf("payload = %q, want ok", res.payload)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for calls")
		}
	}

	if got := atomic.LoadInt64(&originCalls); got != 1 {
		t.Fatalf("handler called %d times, want exactly 1", got)
	}
}

// TestCachedResultReturnedImmediately verifies that a second call with the
// same key after the original finishes is served from cache without calling
// the handler again and returns in well under 1 ms.
func TestCachedResultReturnedImmediately(t *testing.T) {
	d := newTestDeduplicator(time.Second)
	var originCalls int64

	handler := func(ctx context.Context) (*dedup.Envelope, error) {
		atomic.AddInt64(&originCalls, 1)
		time.Sleep(80 * time.Millisecond)
		return &dedup.Envelope{Payload: []byte("hello")}, nil
	}

	if _, err := d.Do(context.Background(), "k", handler); err != nil {
		t.Fatalf("first call: %v", err)
	}

	start := time.Now()
	env, err := d.Do(context.Background(), "k", handler)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	elapsed := time.Since(start)

	if string(env.Payload) != "hello" {
		t.Fatalf("payload = %q, want \"hello\"", env.Payload)
	}
	if got := atomic.LoadInt64(&originCalls); got != 1 {
		t.Fatalf("handler called %d times after cache hit, want 1", got)
	}
	if elapsed > 5*time.Millisecond {
		t.Fatalf("cached response took %v, want < 5ms", elapsed)
	}
}

// TestExpiredCacheCallsHandlerAgain verifies that after ResultTTL elapses the
// handler is invoked again on the next call.
func TestExpiredCacheCallsHandlerAgain(t *testing.T) {
	d := newTestDeduplicator(50 * time.Millisecond)
	var originCalls int64

	handler := func(ctx context.Context) (*dedup.Envelope, error) {
		atomic.AddInt64(&originCalls, 1)
		return &dedup.Envelope{Payload: []byte("v")}, nil
	}

	if _, err := d.Do(context.Background(), "k2", handler); err != nil {
		t.Fatalf("first call: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // let TTL expire

	if _, err := d.Do(context.Background(), "k2", handler); err != nil {
		t.Fatalf("second call: %v", err)
	}

	if got := atomic.LoadInt64(&originCalls); got != 2 {
		t.Fatalf("handler called %d times, want 2 (TTL expired)", got)
	}
}

// TestEmptyKeyReturnsError verifies that an empty key is rejected.
func TestEmptyKeyReturnsError(t *testing.T) {
	d := newTestDeduplicator(time.Second)
	_, err := d.Do(context.Background(), "", func(ctx context.Context) (*dedup.Envelope, error) {
		return &dedup.Envelope{}, nil
	})
	if err == nil {
		t.Fatal("expected error for empty key, got nil")
	}
}

// TestNilEnvelopeWithoutErrorReturnsMeaningfulError verifies that fn returning
// (nil, nil) does not panic and produces an explicit validation error.
func TestNilEnvelopeWithoutErrorReturnsMeaningfulError(t *testing.T) {
	d := newTestDeduplicator(time.Second)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Do panicked: %v", r)
		}
	}()

	_, err := d.Do(context.Background(), "nil-envelope", func(ctx context.Context) (*dedup.Envelope, error) {
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	wantErr := errors.New("dedup: fn returned nil envelope")
	if !errors.Is(err, wantErr) && err.Error() != wantErr.Error() {
		t.Fatalf("error = %q, want %q", err.Error(), wantErr.Error())
	}
}

// TestStoreSetWithCancelledContext verifies that cancelling the caller's
// context after fn returns does not prevent the result from being cached.
// Before the fix, store.Set used execCtx (the caller's context); if that
// context was cancelled between fn returning and store.Set running, the
// result was never cached and the next caller would execute fn again —
// reproducing the bug where a gRPC deadline fired mid-operation.
func TestStoreSetWithCancelledContext(t *testing.T) {
	d := newTestDeduplicator(time.Second)
	var calls int64

	ctx, cancel := context.WithCancel(context.Background())

	// First call: context is cancelled inside fn().
	// The singleflight coordinator may return context.Canceled to this caller,
	// but fn itself must still complete and cache the result.
	_, _ = d.Do(ctx, "cancel-key", func(_ context.Context) (*dedup.Envelope, error) {
		atomic.AddInt64(&calls, 1)
		cancel() // simulate: gRPC deadline fires while fn is still running
		return &dedup.Envelope{Payload: []byte("ok")}, nil
	})

	// Give the background goroutine (singleflight) time to finish storing.
	time.Sleep(20 * time.Millisecond)

	// Second call with a fresh context: must hit the cache — fn must NOT run again.
	env, err := d.Do(context.Background(), "cancel-key", func(_ context.Context) (*dedup.Envelope, error) {
		atomic.AddInt64(&calls, 1)
		return &dedup.Envelope{Payload: []byte("second")}, nil
	})
	if err != nil {
		t.Fatalf("second Do: unexpected error: %v", err)
	}
	if string(env.Payload) != "ok" {
		t.Fatalf("payload = %q, want \"ok\" (cached result)", string(env.Payload))
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("fn called %d times, want 1 (result must be cached despite context cancellation)", got)
	}
}

// already cached in the in-memory store.
func BenchmarkHotKeyCached(b *testing.B) {
	d := newTestDeduplicator(time.Minute)
	handler := func(ctx context.Context) (*dedup.Envelope, error) {
		return &dedup.Envelope{Payload: []byte("ok")}, nil
	}
	if _, err := d.Do(context.Background(), "bench-key", handler); err != nil {
		b.Fatalf("warmup: %v", err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := d.Do(context.Background(), "bench-key", handler); err != nil {
				b.Fatalf("do: %v", err)
			}
		}
	})
}
