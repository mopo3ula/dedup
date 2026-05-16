package singleflight

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mopo3ula/dedup"
)

type contextKey string

func TestRunPassesContextValuesToOriginal(t *testing.T) {
	coord := New()
	key := contextKey("request-id")
	ctx := context.WithValue(context.Background(), key, "abc-123")

	_, err := coord.Run(ctx, "value-key", func(execCtx context.Context) (*dedup.Envelope, error) {
		if got := execCtx.Value(key); got != "abc-123" {
			t.Fatalf("context value = %v, want abc-123", got)
		}
		return &dedup.Envelope{Payload: []byte("ok")}, nil
	})
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
}

func TestWaiterCancellationDoesNotCancelAlreadyRunningOriginal(t *testing.T) {
	coord := New()

	started := make(chan struct{})
	releaseOriginal := make(chan struct{})
	originalDone := make(chan error, 1)
	var duplicateCalls int64

	go func() {
		_, err := coord.Run(context.Background(), "shared-key", func(execCtx context.Context) (*dedup.Envelope, error) {
			close(started)
			select {
			case <-releaseOriginal:
				return &dedup.Envelope{Payload: []byte("original")}, nil
			case <-execCtx.Done():
				return nil, execCtx.Err()
			}
		})
		originalDone <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("original did not start")
	}

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterStarted := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterStarted)
		_, err := coord.Run(waiterCtx, "shared-key", func(context.Context) (*dedup.Envelope, error) {
			atomic.AddInt64(&duplicateCalls, 1)
			return &dedup.Envelope{Payload: []byte("duplicate")}, nil
		})
		waiterDone <- err
	}()

	select {
	case <-waiterStarted:
	case <-time.After(time.Second):
		t.Fatal("waiter did not start")
	}
	time.Sleep(10 * time.Millisecond)
	cancelWaiter()

	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not return after cancellation")
	}

	select {
	case err := <-originalDone:
		t.Fatalf("original finished before release with error: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseOriginal)

	select {
	case err := <-originalDone:
		if err != nil {
			t.Fatalf("original error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("original did not finish after release")
	}

	if got := atomic.LoadInt64(&duplicateCalls); got != 0 {
		t.Fatalf("duplicate fn called %d times, want 0", got)
	}
}
