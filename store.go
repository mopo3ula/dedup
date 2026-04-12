package dedup

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by [ResultStore.Get] when no result is stored for
// the given key (either it never existed or its TTL has elapsed).
var ErrNotFound = errors.New("dedup: result not found")

// ResultStore persists completed request results so that duplicates arriving
// after the original finishes can be served instantly from cache.
//
// Implementations must:
//   - Enforce TTL: [Get] must return [ErrNotFound] for expired entries.
//   - Be safe for concurrent use.
//   - Return [ErrNotFound] (not nil, not another error) for missing keys.
type ResultStore interface {
	// Get returns the [Envelope] stored for key, or [ErrNotFound] if absent
	// or expired.
	Get(ctx context.Context, key string) (*Envelope, error)

	// Set stores value under key, expiring it after ttl.
	Set(ctx context.Context, key string, value *Envelope, ttl time.Duration) error
}
