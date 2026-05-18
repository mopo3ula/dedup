//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mopo3ula/dedup"
	redisstore "github.com/mopo3ula/dedup/store/redis"
	goredis "github.com/redis/go-redis/v9"
)

type contextKey string

func newIntegrationClient(t *testing.T) goredis.UniversalClient {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("Redis is unavailable at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestRunPassesContextValuesToOriginal(t *testing.T) {
	client := newIntegrationClient(t)
	prefix := fmt.Sprintf("dedup-test:%d:value", time.Now().UnixNano())
	coord := New(client, &Options{Prefix: prefix})

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
	client := newIntegrationClient(t)
	prefix := fmt.Sprintf("dedup-test:%d:cancel", time.Now().UnixNano())
	original := New(client, &Options{Prefix: prefix, WaitStep: 5 * time.Millisecond})
	lockMissed := make(chan struct{})
	waiter := New(client, &Options{
		Prefix:   prefix,
		WaitStep: 5 * time.Millisecond,
		OnLockMissed: func(string, string) {
			close(lockMissed)
		},
	})

	started := make(chan struct{})
	releaseOriginal := make(chan struct{})
	originalDone := make(chan error, 1)
	var duplicateCalls int64

	go func() {
		_, err := original.Run(context.Background(), "shared-key", func(execCtx context.Context) (*dedup.Envelope, error) {
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
	waiterDone := make(chan error, 1)
	go func() {
		_, err := waiter.Run(waiterCtx, "shared-key", func(context.Context) (*dedup.Envelope, error) {
			atomic.AddInt64(&duplicateCalls, 1)
			return &dedup.Envelope{Payload: []byte("duplicate")}, nil
		})
		waiterDone <- err
	}()

	select {
	case <-lockMissed:
	case <-time.After(time.Second):
		t.Fatal("waiter did not miss the original lock")
	}
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

func TestRunReturnsLockLostAndDoesNotPublishCompletionWhenRenewalLosesOwnership(t *testing.T) {
	client := newIntegrationClient(t)
	prefix := fmt.Sprintf("dedup-test:%d:lock-lost", time.Now().UnixNano())
	const key = "shared-key"

	lockTTL := 90 * time.Millisecond
	coord := New(client, &Options{
		Prefix:   prefix,
		LockTTL:  lockTTL,
		WaitStep: 5 * time.Millisecond,
	})

	ctx := context.Background()
	lockKey := coord.keyPrefix + key
	doneCh := coord.chanPrefix + key
	pubsub := client.Subscribe(ctx, doneCh)
	defer pubsub.Close() //nolint:errcheck
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe %q: %v", doneCh, err)
	}

	started := make(chan struct{})
	finish := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		_, err := coord.Run(ctx, key, func(context.Context) (*dedup.Envelope, error) {
			close(started)
			<-finish
			return &dedup.Envelope{Payload: []byte("unsafe")}, nil
		})
		runDone <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("original did not start")
	}

	if err := client.Set(ctx, lockKey, "replacement-token", time.Second).Err(); err != nil {
		t.Fatalf("replace lock token: %v", err)
	}

	// Give the renewer enough time to run at least once after the token was
	// replaced. Its next CAS extension must observe a miss and record lock loss.
	time.Sleep(lockTTL)
	close(finish)

	select {
	case err := <-runDone:
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("Run error = %v, want %v", err, ErrLockLost)
		}
	case <-time.After(time.Second):
		t.Fatal("original did not return after release")
	}

	select {
	case msg := <-pubsub.Channel():
		t.Fatalf("unexpected completion publication after lock loss: %q", msg.Payload)
	case <-time.After(2 * lockTTL):
	}
}

func TestDeduplicatorWithRedisRunsSequentialCallAgain(t *testing.T) {
	client := newIntegrationClient(t)
	prefix := fmt.Sprintf("dedup-test:%d:sequential", time.Now().UnixNano())
	coord := New(client, &Options{Prefix: prefix, WaitStep: 5 * time.Millisecond})
	store := redisstore.New(client, prefix+":result:")
	d := dedup.MustNew(store, coord, &dedup.Options{ResultTTL: time.Minute})

	var calls int64
	handler := func(context.Context) (*dedup.Envelope, error) {
		n := atomic.AddInt64(&calls, 1)
		return &dedup.Envelope{Payload: []byte{byte('0' + n)}}, nil
	}

	env, err := d.Do(context.Background(), "shared-key", handler)
	if err != nil {
		t.Fatalf("first Do: %v", err)
	}
	if string(env.Payload) != "1" {
		t.Fatalf("first payload = %q, want 1", env.Payload)
	}

	env, err = d.Do(context.Background(), "shared-key", handler)
	if err != nil {
		t.Fatalf("second Do: %v", err)
	}
	if string(env.Payload) != "2" {
		t.Fatalf("second payload = %q, want 2", env.Payload)
	}
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("handler called %d times, want 2", got)
	}
}
