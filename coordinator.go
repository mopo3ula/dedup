package dedup

import (
	"context"
	"errors"
)

// ErrWaitCompleted is returned by a [Coordinator] implementation to signal
// that the current caller was a duplicate waiter and the original has now
// finished. [Deduplicator.Do] intercepts this sentinel and reads the result
// from [ResultStore] instead of using the returned value.
var ErrWaitCompleted = errors.New("dedup: duplicate waited for original")

// Coordinator decides which concurrent call for a given key is the "original"
// and blocks all duplicates until it completes.
//
// Contract that every implementation must satisfy:
//
//   - Exactly one caller of [Run] per key executes fn (the "original") with a
//     context that preserves the caller's context values but ignores caller
//     cancellation.
//   - All other concurrent callers with the same key (the "duplicates") block
//     until the original returns.
//   - On success the coordinator may either return the same [Envelope] value
//     to duplicates or return [ErrWaitCompleted] to indicate that they should
//     read the result from [ResultStore].
//   - If fn returns an error it must be propagated to all waiters.
//   - Implementations must be safe for concurrent use.
type Coordinator interface {
	Run(ctx context.Context, key string, fn func(context.Context) (*Envelope, error)) (*Envelope, error)
}
