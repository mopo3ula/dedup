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
// If a call for key is already in-flight, Run blocks until it completes and
// returns the same value. The third return value of singleflight (shared bool)
// is intentionally ignored; the [dedup.Deduplicator] layer handles fan-out.
func (c *Coordinator) Run(
	ctx context.Context,
	key string,
	fn func(context.Context) (*dedup.Envelope, error),
) (*dedup.Envelope, error) {
	v, err, _ := c.group.Do(key, func() (any, error) {
		return fn(ctx)
	})
	if err != nil {
		return nil, err
	}
	return v.(*dedup.Envelope), nil
}

// Compile-time interface check.
var _ dedup.Coordinator = (*Coordinator)(nil)
