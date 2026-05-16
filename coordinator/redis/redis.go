// Package redis provides a distributed [dedup.Coordinator] backed by Redis.
//
// It uses SET NX with a renewable TTL for leader election and Redis Pub/Sub to
// notify waiting duplicates the moment the original finishes. A check
// immediately after subscription plus a periodic polling fallback prevent
// waiters from depending on a single Pub/Sub notification.
//
// Use this coordinator when your service runs as multiple instances that share
// a Redis cluster. For single-process deployments [coordinator/singleflight]
// is simpler and has no external dependencies.
package redis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mopo3ula/dedup"
	goredis "github.com/redis/go-redis/v9"
)

// Options configures the Redis coordinator.
type Options struct {
	// LockTTL is the Redis lock lease duration. The original refreshes the lease
	// while fn is running; if the process crashes or Redis cannot be reached long
	// enough for the lease to expire, another instance may acquire the lock and
	// execute the same handler for the same key. Keep this value comfortably
	// above short Redis hiccups and scheduler pauses.
	// Default: 5s.
	LockTTL time.Duration

	// WaitStep is the interval at which duplicate waiters poll Redis as a
	// fallback in case the Pub/Sub notification is lost.
	// Default: 20ms.
	WaitStep time.Duration

	// Prefix is prepended to all Redis keys created by this coordinator.
	// Default: "dedup".
	Prefix string

	// OnLockAcquired, if non-nil, is called when this caller wins the SET NX
	// election and becomes the original (fn will execute next). The coordinatorID
	// is a unique string that identifies this [Coordinator] instance — useful to
	// detect unexpected multiple-instance scenarios. Useful for debugging and
	// metrics (e.g. count how many times the original runs per key).
	OnLockAcquired func(key, coordinatorID string)

	// OnLockMissed, if non-nil, is called when SET NX fails — this caller is a
	// duplicate and will wait for the original to finish.
	OnLockMissed func(key, coordinatorID string)
}

// ErrLockLost is returned by [Coordinator.Run] when the original executor
// detects that it no longer owns its Redis lock while fn is running. In that
// state the returned result is unsafe to publish because another executor may
// have acquired the same key and started a newer generation of work.
var ErrLockLost = errors.New("dedup redis: lock ownership lost")

// Coordinator is a distributed [dedup.Coordinator] that uses Redis SET NX +
// Pub/Sub to coordinate across multiple service instances.
type Coordinator struct {
	client         goredis.UniversalClient
	lockTTL        time.Duration
	waitStep       time.Duration
	keyPrefix      string
	chanPrefix     string
	errPrefix      string
	onLockAcquired func(key, coordinatorID string)
	onLockMissed   func(key, coordinatorID string)
}

// New creates a Coordinator using the provided Redis client.
// opt may be nil; defaults are used in that case.
func New(client goredis.UniversalClient, opt *Options) *Coordinator {
	cfg := Options{
		LockTTL:  5 * time.Second,
		WaitStep: 20 * time.Millisecond,
		Prefix:   "dedup",
	}
	var onAcquired, onMissed func(key, coordinatorID string)
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
		onAcquired = opt.OnLockAcquired
		onMissed = opt.OnLockMissed
	}
	return &Coordinator{
		client:         client,
		lockTTL:        cfg.LockTTL,
		waitStep:       cfg.WaitStep,
		keyPrefix:      fmt.Sprintf("%s:lock:", cfg.Prefix),
		chanPrefix:     fmt.Sprintf("%s:done:", cfg.Prefix),
		errPrefix:      fmt.Sprintf("%s:error:", cfg.Prefix),
		onLockAcquired: onAcquired,
		onLockMissed:   onMissed,
	}
}

// Run implements [dedup.Coordinator].
//
// The first caller that acquires SET NX on the lock key becomes the original
// and executes fn with a detached context that preserves ctx values but ignores
// ctx cancellation. While fn is running, the original periodically refreshes the
// lock lease as long as it still owns the lock. All other concurrent callers
// subscribe to the done channel and block. When the original finishes it
// publishes to the done channel and deletes the lock (via a Lua script to
// prevent accidental deletion by a different owner). If lock renewal detects
// that another owner has replaced or removed the lock before fn returns, Run
// returns [ErrLockLost] and does not publish a normal completion signal. If
// the original failed, waiters return the original error; otherwise they return
// [dedup.ErrWaitCompleted] so that [dedup.Deduplicator] can fetch the result
// from [dedup.ResultStore].
func (c *Coordinator) Run(
	ctx context.Context,
	key string,
	fn func(context.Context) (*dedup.Envelope, error),
) (env *dedup.Envelope, err error) {
	lockKey := c.keyPrefix + key
	doneCh := c.chanPrefix + key
	errKey := c.errPrefix + key
	token := fmt.Sprintf("%p:%d", c, time.Now().UnixNano())

	acquired, err := c.client.SetNX(ctx, lockKey, token, c.lockTTL).Result()
	if err != nil {
		return nil, err
	}

	if acquired {
		// This instance is the original.
		renewal := c.renewLock(lockKey, token)
		defer func() {
			lockLost := renewal.stop()
			if lockLost {
				err = ErrLockLost
				return
			}

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
			_ = c.releaseLock(bg, lockKey, token)
		}()
		if c.onLockAcquired != nil {
			c.onLockAcquired(key, token)
		}
		env, err = fn(context.WithoutCancel(ctx))
		return env, err
	}

	// This instance is a duplicate: subscribe and wait.
	if c.onLockMissed != nil {
		c.onLockMissed(key, fmt.Sprintf("%p", c))
	}
	pubsub := c.client.Subscribe(ctx, doneCh)
	defer pubsub.Close() //nolint:errcheck

	// Receive confirms the subscription is active before we start waiting.
	if _, err = pubsub.Receive(ctx); err != nil {
		return nil, err
	}

	// The original may finish in the tiny window between our failed SET NX
	// attempt and the moment the subscription becomes active. Pub/Sub would not
	// replay that already-published message, so check the lock immediately after
	// subscribing before falling back to ticker-based polling. This keeps
	// nanosecond-close duplicate arrivals from waiting for WaitStep just because
	// they missed the notification.
	finished, err := c.originalFinished(ctx, lockKey)
	if err != nil {
		return nil, err
	}
	if finished {
		return c.completed(ctx, errKey)
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
			// (lock TTL expired or it was released normally).
			finished, existsErr := c.originalFinished(ctx, lockKey)
			if existsErr != nil {
				return nil, existsErr
			}
			if finished {
				return c.completed(ctx, errKey)
			}
		}
	}
}

type lockRenewal struct {
	cancel context.CancelFunc
	done   chan struct{}
	lost   atomic.Bool
}

func (r *lockRenewal) stop() bool {
	r.cancel()
	<-r.done
	return r.lost.Load()
}

type extendLockResult int

const (
	extendLockSucceeded extendLockResult = iota
	extendLockLost
)

func (c *Coordinator) renewLock(lockKey, token string) *lockRenewal {
	ctx, cancel := context.WithCancel(context.Background())
	renewal := &lockRenewal{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	interval := c.lockTTL / 3
	if interval <= 0 {
		interval = c.lockTTL
	}

	go func() {
		defer close(renewal.done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := c.extendLock(ctx, lockKey, token)
				if err != nil {
					continue
				}
				if result == extendLockLost {
					renewal.lost.Store(true)
					return
				}
			}
		}
	}()

	return renewal
}

func (c *Coordinator) extendLock(ctx context.Context, lockKey, token string) (extendLockResult, error) {
	const extendLua = `if redis.call("GET",KEYS[1])==ARGV[1] then return redis.call("PEXPIRE",KEYS[1],ARGV[2]) else return 0 end`
	extended, err := c.client.Eval(ctx, extendLua, []string{lockKey}, token, c.lockTTLMillis()).Int()
	if err != nil {
		return extendLockLost, err
	}
	if extended == 1 {
		return extendLockSucceeded, nil
	}
	return extendLockLost, nil
}

func (c *Coordinator) lockTTLMillis() int64 {
	millis := c.lockTTL.Milliseconds()
	if millis <= 0 {
		return 1
	}
	return millis
}

func (c *Coordinator) releaseLock(ctx context.Context, lockKey, token string) error {
	const releaseLua = `if redis.call("GET",KEYS[1])==ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`
	return c.client.Eval(ctx, releaseLua, []string{lockKey}, token).Err()
}

func (c *Coordinator) originalFinished(ctx context.Context, lockKey string) (bool, error) {
	exists, err := c.client.Exists(ctx, lockKey).Result()
	if err != nil {
		return false, err
	}
	return exists == 0, nil
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
