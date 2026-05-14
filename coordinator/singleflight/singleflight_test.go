package singleflight

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mopo3ula/dedup"
)

type contextKey struct{}

func TestRunPassesContextValuesToOriginal(t *testing.T) {
	coord := New()
	ctx := context.WithValue(context.Background(), contextKey{}, "trace-id")

	_, err := coord.Run(ctx, "context-values", func(execCtx context.Context) (*dedup.Envelope, error) {
		if got := execCtx.Value(contextKey{}); got != "trace-id" {
			t.Fatalf("context value = %v, want trace-id", got)
		}
		return &dedup.Envelope{Payload: []byte("ok")}, nil
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestRunIgnoresCallerCancellationForOriginal(t *testing.T) {
	coord := New()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	fnDone := make(chan error, 1)
	runDone := make(chan error, 1)

	go func() {
		_, err := coord.Run(ctx, "cancellation", func(execCtx context.Context) (*dedup.Envelope, error) {
			close(started)
			<-release
			select {
			case <-execCtx.Done():
				fnDone <- execCtx.Err()
			default:
				fnDone <- nil
			}
			return &dedup.Envelope{Payload: []byte("ok")}, nil
		})
		runDone <- err
	}()

	<-started
	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after caller cancellation")
	}

	close(release)

	select {
	case err := <-fnDone:
		if err != nil {
			t.Fatalf("original context was canceled: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("original function did not finish")
	}
}
