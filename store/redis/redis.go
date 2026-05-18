// Package redis provides a [dedup.ResultStore] backed by Redis.
//
// Results are JSON-serialised and stored with the given TTL. Use this store
// together with [coordinator/redis] so that all service instances share the
// same completed-result cache.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mopo3ula/dedup"
	goredis "github.com/redis/go-redis/v9"
)

// Store is a Redis-backed [dedup.ResultStore].
// The zero value is not usable; create one with [New].
type Store struct {
	client goredis.UniversalClient
	prefix string
}

// New creates a Store using the provided Redis client.
// prefix is prepended to every key (e.g. "myapp:result:"). An empty prefix
// defaults to "dedup:result:".
func New(client goredis.UniversalClient, prefix string) *Store {
	if prefix == "" {
		prefix = "dedup:result:"
	}
	return &Store{client: client, prefix: prefix}
}

// Get implements [dedup.ResultStore].
// Returns [dedup.ErrNotFound] when the key is absent or expired.
func (s *Store) Get(ctx context.Context, key string) (*dedup.Envelope, error) {
	raw, err := s.client.Get(ctx, s.prefix+key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, dedup.ErrNotFound
		}
		return nil, err
	}
	var env dedup.Envelope
	if err = json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("redis store: decode envelope: %w", err)
	}
	return &env, nil
}

// Set implements [dedup.ResultStore].
func (s *Store) Set(ctx context.Context, key string, value *dedup.Envelope, ttl time.Duration) error {
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("redis store: encode envelope: %w", err)
	}
	return s.client.Set(ctx, s.prefix+key, b, ttl).Err()
}

// Compile-time interface check.
var _ dedup.ResultStore = (*Store)(nil)
