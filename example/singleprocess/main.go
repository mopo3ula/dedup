// Example of using dedup in a single-process environment.
//
// Uses an in-memory result store and a singleflight coordinator.
// All goroutines within the same process that call Do with the same key
// concurrently will receive the same result without re-invoking the handler.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mopo3ula/dedup"
	"github.com/mopo3ula/dedup/coordinator/singleflight"
	"github.com/mopo3ula/dedup/store/inmemory"
)

// expensiveQuery simulates a slow request to a database or external API.
func expensiveQuery(ctx context.Context, id string) (*dedup.Envelope, error) {
	fmt.Printf("[handler] executing query for id=%s\n", id)
	// Simulate network / DB latency.
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	type Response struct {
		ID    string `json:"id"`
		Value int    `json:"value"`
	}
	body, _ := json.Marshal(Response{ID: id, Value: 42})

	return &dedup.Envelope{
		Payload: body,
		Meta:    map[string]string{"content-type": "application/json"},
	}, nil
}

func main() {
	// Create a Deduplicator with an in-memory store and singleflight coordinator.
	// ResultTTL=5s means repeated requests within 5 seconds return the cached
	// result without invoking the handler again.
	d, err := dedup.New(
		inmemory.New(),
		singleflight.New(),
		&dedup.Options{ResultTTL: 5 * time.Second},
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// --- Example 1: concurrent requests with the same key ---
	fmt.Println("=== Example 1: 5 concurrent requests with key 'user:1' ===")
	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			env, err := d.Do(ctx, "user:1", func(ctx context.Context) (*dedup.Envelope, error) {
				return expensiveQuery(ctx, "1")
			})
			if err != nil {
				fmt.Printf("  goroutine %d: error: %v\n", n, err)
				return
			}
			fmt.Printf("  goroutine %d: payload=%s createdAt=%s\n",
				n, env.Payload, env.CreatedAt.Format(time.RFC3339Nano))
		}(i)
	}
	wg.Wait()

	// --- Example 2: repeated request is served from cache ---
	fmt.Println("\n=== Example 2: repeated request (should be served from cache) ===")
	env, err := d.Do(ctx, "user:1", func(ctx context.Context) (*dedup.Envelope, error) {
		return expensiveQuery(ctx, "1")
	})
	if err != nil {
		fmt.Printf("error: %v\n", err)
	} else {
		fmt.Printf("cached result: payload=%s\n", env.Payload)
	}

	// --- Example 3: different key is executed independently ---
	fmt.Println("\n=== Example 3: request with new key 'user:2' ===")
	env, err = d.Do(ctx, "user:2", func(ctx context.Context) (*dedup.Envelope, error) {
		return expensiveQuery(ctx, "2")
	})
	if err != nil {
		fmt.Printf("error: %v\n", err)
	} else {
		fmt.Printf("result: payload=%s\n", env.Payload)
	}

	// --- Example 4: context cancellation ---
	fmt.Println("\n=== Example 4: context cancelled while waiting ===")
	cancelCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	_, err = d.Do(cancelCtx, "user:slow", func(ctx context.Context) (*dedup.Envelope, error) {
		return expensiveQuery(ctx, "slow") // takes 100ms, timeout is 10ms
	})
	fmt.Printf("expected timeout error: %v\n", err)
}
