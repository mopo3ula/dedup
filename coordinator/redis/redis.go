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
	"fmt"
	"time"

	"github.com/mopo3ula/dedup"
	goredis "github.com/redis/go-redis/v9"
)

// Options configures the Redis coordinator.
type Options struct {
	// LockTTL is the maximum time the distributed lock is held. If the
	// original handler runs longer than this the lock may expire and a second
	// instance could start executing. Keep this value comfortably above your
	// expected handler latency.
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
	}
}

// Run implements [dedup.Coordinator].
//
// The first caller that acquires SET NX on the lock key becomes the original
// and executes fn. All other concurrent callers subscribe to the done channel
// and block. When the original finishes it deletes the lock (via a Lua script
// to prevent accidental deletion by a different owner) and publishes to the
// done channel. Waiters then return [dedup.ErrWaitCompleted] so that
// [dedup.Deduplicator] can fetch the result from [dedup.ResultStore].
func (c *Coordinator) Run(
	ctx context.Context,
	key string,
	fn func(context.Context) (*dedup.Envelope, error),
) (*dedup.Envelope, error) {
	lockKey := c.keyPrefix + key
	doneCh := c.chanPrefix + key
	token := fmt.Sprintf("%d", time.Now().UnixNano())

	acquired, err := c.client.SetNX(ctx, lockKey, token, c.lockTTL).Result()
	if err != nil {
		return nil, err
	}

	if acquired {
		// This instance is the original.
		defer func() {
			// Release lock only if we still own it (Lua CAS).
			const releaseLua = `if redis.call("GET",KEYS[1])==ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`
			_ = c.client.Eval(context.Background(), releaseLua, []string{lockKey}, token).Err()
			// Notify all waiters.
			_ = c.client.Publish(context.Background(), doneCh, "done").Err()
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
			return nil, dedup.ErrWaitCompleted

		case <-ticker.C:
			// Fallback poll: if the lock is gone the original has finished
			// (lock TTL expired or it was released normally).
			exists, existsErr := c.client.Exists(ctx, lockKey).Result()
			if existsErr != nil {
				return nil, existsErr
			}
			if exists == 0 {
				return nil, dedup.ErrWaitCompleted
			}
		}
	}
}

// Compile-time interface check.
var _ dedup.Coordinator = (*Coordinator)(nil)
