// Package singleflight provides an in-process [dedup.Coordinator] built on
// top of [golang.org/x/sync/singleflight].
//
// It is suitable for single-instance deployments where all requests are
// handled by the same process. For multi-instance (horizontally-scaled)
// deployments use [coordinator/redis] instead.
package singleflight

import (
	"context"

	"github.com/mopo3ula/dedup"
	"golang.org/x/sync/singleflight"
)

// Coordinator is an in-process [dedup.Coordinator] backed by
// [singleflight.Group]. All duplicate calls with the same key within a single
// process block until the original completes and receive the same result.
type Coordinator struct {
	group singleflight.Group
}

// New returns a ready-to-use in-process Coordinator.
func New() *Coordinator {
	return &Coordinator{}
}

// Run implements [dedup.Coordinator].
//
// If a call for key is already in-flight, Run waits for either:
//   - the shared singleflight result, or
//   - ctx.Done() for this local caller.
//
// If ctx is canceled first, Run returns ctx.Err() for this caller only.
// The in-flight original call continues running independently with ctx values
// preserved but cancellation ignored, and other waiters can still receive its
// result. The [dedup.Deduplicator] layer handles fan-out, so the shared bool
// from singleflight is intentionally ignored.
func (c *Coordinator) Run(
	ctx context.Context,
	key string,
	fn func(context.Context) (*dedup.Envelope, error),
) (*dedup.Envelope, error) {
	execCtx := context.WithoutCancel(ctx)
	resultCh := c.group.DoChan(key, func() (any, error) {
		return fn(execCtx)
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Val.(*dedup.Envelope), nil
	}
}

// Compile-time interface check.
var _ dedup.Coordinator = (*Coordinator)(nil)
