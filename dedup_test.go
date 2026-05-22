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
// that starts while the original is in-flight still waits for the original
// and receives its result instead of executing the handler again.
func TestDeduplicatesNanosecondCloseCalls(t *testing.T) {
	d := newTestDeduplicator(time.Second)

	var calls int64
	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context) (*dedup.Envelope, error) {
		if atomic.AddInt64(&calls, 1) == 1 {
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

	go func() {
		env, err := d.Do(context.Background(), "nanosecond-key", handler)
		if err != nil {
			results <- callResult{err: err}
			return
		}
		results <- callResult{payload: string(env.Payload)}
	}()

	// Give goroutine 2 time to enter d.Do and block in the coordinator before
	// the original's fn returns.
	time.Sleep(5 * time.Millisecond)
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

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("handler called %d times, want exactly 1", got)
	}
}

// TestSequentialCallsEachExecuteHandler verifies that requests arriving after
// the in-flight group completes are not served from any cache — each becomes
// a new original and the handler is executed every time.
func TestSequentialCallsEachExecuteHandler(t *testing.T) {
	d := newTestDeduplicator(time.Second)
	var calls int64

	handler := func(ctx context.Context) (*dedup.Envelope, error) {
		n := atomic.AddInt64(&calls, 1)
		return &dedup.Envelope{Payload: []byte{byte(n)}}, nil
	}

	for i := range 3 {
		env, err := d.Do(context.Background(), "seq-key", handler)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if want := byte(i + 1); env.Payload[0] != want {
			t.Fatalf("call %d: payload = %d, want %d", i, env.Payload[0], want)
		}
	}

	if got := atomic.LoadInt64(&calls); got != 3 {
		t.Fatalf("handler called %d times, want 3", got)
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

func BenchmarkInflightDedup(b *testing.B) {
	d := newTestDeduplicator(time.Minute)
	handler := func(ctx context.Context) (*dedup.Envelope, error) {
		return &dedup.Envelope{Payload: []byte("ok")}, nil
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
