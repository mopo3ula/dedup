//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mopo3ula/dedup"
	goredis "github.com/redis/go-redis/v9"
)

func TestRunReportsLostLockAndWaitersDoNotTreatItAsSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := integrationRedisClient(t, ctx)
	prefix := fmt.Sprintf("dedup-test:%d", time.Now().UnixNano())
	key := "lost-lock"
	lockTTL := 300 * time.Millisecond

	originalAcquired := make(chan struct{})
	duplicateMissed := make(chan struct{})
	fnStarted := make(chan struct{})
	finishFn := make(chan struct{})

	original := New(client, &Options{
		LockTTL:  lockTTL,
		WaitStep: 10 * time.Millisecond,
		Prefix:   prefix,
		OnLockAcquired: func(string, string) {
			closeOnce(originalAcquired)
		},
	})
	duplicate := New(client, &Options{
		LockTTL:  lockTTL,
		WaitStep: 10 * time.Millisecond,
		Prefix:   prefix,
		OnLockMissed: func(string, string) {
			closeOnce(duplicateMissed)
		},
	})

	t.Cleanup(func() {
		_ = client.Del(context.Background(), prefix+":lock:"+key, prefix+":done:"+key, prefix+":error:"+key).Err()
	})

	originalErr := make(chan error, 1)
	go func() {
		_, err := original.Run(ctx, key, func(context.Context) (*dedup.Envelope, error) {
			closeOnce(fnStarted)
			<-finishFn
			return &dedup.Envelope{Payload: []byte("unsafe-success")}, nil
		})
		originalErr <- err
	}()

	waitFor(t, ctx, originalAcquired, "original to acquire lock")
	waitFor(t, ctx, fnStarted, "original function to start")

	duplicateErr := make(chan error, 1)
	go func() {
		_, err := duplicate.Run(ctx, key, func(context.Context) (*dedup.Envelope, error) {
			return nil, errors.New("duplicate unexpectedly executed fn")
		})
		duplicateErr <- err
	}()
	waitFor(t, ctx, duplicateMissed, "duplicate to miss the lock")

	lockKey := prefix + ":lock:" + key
	if err := client.Set(ctx, lockKey, "stolen-token", 5*time.Second).Err(); err != nil {
		t.Fatalf("replace lock token: %v", err)
	}

	// Renewal runs every lockTTL/3. Give it enough time to observe that the
	// stored token no longer belongs to the original before fn returns.
	time.Sleep(lockTTL/2 + 50*time.Millisecond)
	close(finishFn)

	if err := receiveErr(t, ctx, originalErr, "original Run"); !errors.Is(err, ErrLockLost) {
		t.Fatalf("original Run error = %v, want ErrLockLost", err)
	}
	if err := receiveErr(t, ctx, duplicateErr, "duplicate Run"); !errors.Is(err, ErrLockLost) {
		t.Fatalf("duplicate Run error = %v, want ErrLockLost", err)
	} else if errors.Is(err, dedup.ErrWaitCompleted) {
		t.Fatalf("duplicate Run error = %v, must not be ordinary successful completion", err)
	}
}

func integrationRedisClient(t *testing.T, ctx context.Context) *goredis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("Redis integration test requires Redis at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func waitFor(t *testing.T, ctx context.Context, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", what, ctx.Err())
	}
}

func receiveErr(t *testing.T, ctx context.Context, ch <-chan error, what string) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", what, ctx.Err())
		return nil
	}
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
