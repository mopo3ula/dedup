// Package redis provides a distributed [dedup.Coordinator] backed by Redis.
//
// It uses SET NX with a TTL for leader election and Redis Pub/Sub to notify
// waiting duplicates the moment the original finishes. A periodic polling
// fallback ensures correctness even if a Pub/Sub message is lost.
//
// Use this coordinator when your service runs as multiple instances that share
// a Redis cluster. For single-process deployments [coordinator/singleflight]
// is simpler and has no external dependencies.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mopo3ula/dedup"
	goredis "github.com/redis/go-redis/v9"
)

// Options configures the Redis coordinator.
type Options struct {
	// LockTTL is the distributed lock lease duration. The coordinator renews
	// the lease while the original handler is running, so this value only needs
	// to cover short Redis/network hiccups between renewals.
	// Default: 5s.
	LockTTL time.Duration

	// WaitStep is the interval at which duplicate waiters poll Redis as a
	// fallback in case the Pub/Sub notification is lost.
	// Default: 20ms.
	WaitStep time.Duration

	// Prefix is prepended to all Redis keys created by this coordinator.
	// Default: "dedup".
	Prefix string
}

// Coordinator is a distributed [dedup.Coordinator] that uses Redis SET NX +
// Pub/Sub to coordinate across multiple service instances.
type Coordinator struct {
	client     goredis.UniversalClient
	lockTTL    time.Duration
	waitStep   time.Duration
	keyPrefix  string
	chanPrefix string
	errPrefix  string
}

// New creates a Coordinator using the provided Redis client.
// opt may be nil; defaults are used in that case.
func New(client goredis.UniversalClient, opt *Options) *Coordinator {
	cfg := Options{
		LockTTL:  5 * time.Second,
		WaitStep: 20 * time.Millisecond,
		Prefix:   "dedup",
	}
	if opt != nil {
		if opt.LockTTL > 0 {
			cfg.LockTTL = opt.LockTTL
		}
		if opt.WaitStep > 0 {
			cfg.WaitStep = opt.WaitStep
		}
		if opt.Prefix != "" {
			cfg.Prefix = opt.Prefix
		}
	}
	return &Coordinator{
		client:     client,
		lockTTL:    cfg.LockTTL,
		waitStep:   cfg.WaitStep,
		keyPrefix:  fmt.Sprintf("%s:lock:", cfg.Prefix),
		chanPrefix: fmt.Sprintf("%s:done:", cfg.Prefix),
		errPrefix:  fmt.Sprintf("%s:error:", cfg.Prefix),
	}
}

// Run implements [dedup.Coordinator].
//
// The first caller that acquires SET NX on the lock key becomes the original
// and executes fn. All other concurrent callers subscribe to the done channel
// and block. When the original finishes it publishes to the done channel and
// deletes the lock (via a Lua script to prevent accidental deletion by a
// different owner). If the original failed, waiters return the original error;
// otherwise they return [dedup.ErrWaitCompleted] so that [dedup.Deduplicator]
// can fetch the result from [dedup.ResultStore].
func (c *Coordinator) Run(
	ctx context.Context,
	key string,
	fn func(context.Context) (*dedup.Envelope, error),
) (env *dedup.Envelope, err error) {
	lockKey := c.keyPrefix + key
	doneCh := c.chanPrefix + key
	errKey := c.errPrefix + key
	token := fmt.Sprintf("%d", time.Now().UnixNano())

	acquired, err := c.client.SetNX(ctx, lockKey, token, c.lockTTL).Result()
	if err != nil {
		return nil, err
	}

	if acquired {
		// This instance is the original.
		stopRenew := c.renewLock(lockKey, token)
		defer func() {
			stopRenew()
			bg := context.Background()
			if err != nil {
				_ = c.client.Set(bg, errKey, err.Error(), c.lockTTL).Err()
			} else {
				_ = c.client.Del(bg, errKey).Err()
			}
			// Notify all waiters before releasing the lock, so the next
			// generation of callers cannot consume this completion signal.
			_ = c.client.Publish(bg, doneCh, "done").Err()
			// Release lock only if we still own it (Lua CAS).
			const releaseLua = `if redis.call("GET",KEYS[1])==ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`
			_ = c.client.Eval(bg, releaseLua, []string{lockKey}, token).Err()
		}()
		return fn(ctx)
	}

	// This instance is a duplicate: subscribe and wait.
	pubsub := c.client.Subscribe(ctx, doneCh)
	defer pubsub.Close() //nolint:errcheck

	// Receive confirms the subscription is active before we start waiting.
	if _, err = pubsub.Receive(ctx); err != nil {
		return nil, err
	}

	ch := pubsub.Channel()
	ticker := time.NewTicker(c.waitStep)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-ch:
			// Pub/Sub notification: original finished.
			return c.completed(ctx, errKey)

		case <-ticker.C:
			// Fallback poll: if the lock is gone the original has finished
			// or the lock was lost after renewal stopped.
			exists, existsErr := c.client.Exists(ctx, lockKey).Result()
			if existsErr != nil {
				return nil, existsErr
			}
			if exists == 0 {
				return c.completed(ctx, errKey)
			}
		}
	}
}

func (c *Coordinator) renewLock(lockKey, token string) func() {
	done := make(chan struct{})
	interval := c.lockTTL / 2
	if interval <= 0 {
		interval = c.lockTTL
	}
	if interval <= 0 {
		interval = time.Second
	}

	ttlMillis := c.lockTTL.Milliseconds()
	if ttlMillis < 1 {
		ttlMillis = 1
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		const renewLua = `if redis.call("GET",KEYS[1])==ARGV[1] then return redis.call("PEXPIRE",KEYS[1],ARGV[2]) else return 0 end`
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = c.client.Eval(context.Background(), renewLua, []string{lockKey}, token, ttlMillis).Err()
			}
		}
	}()

	return func() { close(done) }
}

func (c *Coordinator) completed(ctx context.Context, errKey string) (*dedup.Envelope, error) {
	originalErr, err := c.client.Get(ctx, errKey).Result()
	if err == nil {
		return nil, errors.New(originalErr)
	}
	if errors.Is(err, goredis.Nil) {
		return nil, dedup.ErrWaitCompleted
	}
	return nil, err
}

// Compile-time interface check.
var _ dedup.Coordinator = (*Coordinator)(nil)
