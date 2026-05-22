// Package inmemory provides an in-process [dedup.ResultStore] backed by a
// plain Go map with TTL-based expiration.
//
// All operations are O(1). Expired entries are evicted lazily on [Store.Get].
// There is no background goroutine, so the store is safe to abandon without
// an explicit shutdown step.
//
// Use this store together with [coordinator/singleflight] for single-process
// deployments. For multi-instance deployments use [store/redis] so that all
// instances can share results with in-flight duplicate waiters.
package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/mopo3ula/dedup"
)

// Store is an in-process [dedup.ResultStore].
// The zero value is not usable; create one with [New].
type Store struct {
	mu    sync.RWMutex
	items map[string]item
	now   func() time.Time
}

type item struct {
	value  *dedup.Envelope
	expire time.Time
}

// New returns a ready-to-use in-memory Store.
func New() *Store {
	return &Store{
		items: make(map[string]item),
		now:   time.Now,
	}
}

// Get implements [dedup.ResultStore].
// Returns [dedup.ErrNotFound] if the key does not exist or has expired.
func (s *Store) Get(_ context.Context, key string) (*dedup.Envelope, error) {
	s.mu.RLock()
	it, ok := s.items[key]
	s.mu.RUnlock()

	if !ok {
		return nil, dedup.ErrNotFound
	}
	if s.now().After(it.expire) {
		s.mu.Lock()
		delete(s.items, key)
		s.mu.Unlock()
		return nil, dedup.ErrNotFound
	}
	return it.value, nil
}

// Set implements [dedup.ResultStore].
func (s *Store) Set(_ context.Context, key string, value *dedup.Envelope, ttl time.Duration) error {
	s.mu.Lock()
	s.items[key] = item{value: value, expire: s.now().Add(ttl)}
	s.mu.Unlock()
	return nil
}

// Compile-time interface check.
var _ dedup.ResultStore = (*Store)(nil)
