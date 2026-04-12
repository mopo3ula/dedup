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
	return dedup.New(memstore.New(), sfcoord.New(), &dedup.Options{ResultTTL: ttl})
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

// BenchmarkHotKeyCached measures the overhead of serving a result that is
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
