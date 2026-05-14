// Example of using dedup in a distributed (multi-instance) environment.
//
// Uses a Redis result store and a Redis coordinator.
// Multiple service instances connected to the same Redis will execute the
// handler exactly once, sharing the result across all instances.
//
// Requires Redis running on localhost:6379.
// Run: go run ./example/multiinstance
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mopo3ula/dedup"
	rediscoord "github.com/mopo3ula/dedup/coordinator/redis"
	redistore "github.com/mopo3ula/dedup/store/redis"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	// Connect to Redis.
	rdb := goredis.NewClient(&goredis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(fmt.Sprintf("failed to connect to Redis: %v", err))
	}

	// Create a Deduplicator with a Redis store and Redis coordinator.
	// All application instances sharing the same Redis will share the result
	// cache and the distributed lock.
	d := dedup.New(
		redistore.New(rdb, "myapp:result:"),
		rediscoord.New(rdb, &rediscoord.Options{
			LockTTL: 10 * time.Second, // maximum expected handler duration
			Prefix:  "myapp",
		}),
		&dedup.Options{ResultTTL: 30 * time.Second},
	)

	// --- Example 1: concurrent requests with the same key ---
	fmt.Println("=== Example 1: 5 concurrent requests with key 'order:42' ===")
	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			env, err := d.Do(ctx, "order:42", func(ctx context.Context) (*dedup.Envelope, error) {
				return processOrder(ctx, "42")
			})
			if err != nil {
				fmt.Printf("  goroutine %d: error: %v\n", n, err)
				return
			}
			fmt.Printf("  goroutine %d: payload=%s\n", n, env.Payload)
		}(i)
	}
	wg.Wait()

	// --- Example 2: repeated request is served from Redis cache ---
	fmt.Println("\n=== Example 2: repeated request (served from Redis cache) ===")
	env, err := d.Do(ctx, "order:42", func(ctx context.Context) (*dedup.Envelope, error) {
		return processOrder(ctx, "42")
	})
	if err != nil {
		fmt.Printf("error: %v\n", err)
	} else {
		fmt.Printf("from Redis cache: payload=%s createdAt=%s\n",
			env.Payload, env.CreatedAt.Format(time.RFC3339))
	}

	// Clean up test keys.
	rdb.Del(ctx, "myapp:result:order:42")
}

// processOrder simulates order processing — a slow idempotent operation.
func processOrder(ctx context.Context, orderID string) (*dedup.Envelope, error) {
	fmt.Printf("[handler] processing order id=%s\n", orderID)
	select {
	case <-time.After(200 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	type OrderResult struct {
		OrderID string `json:"order_id"`
		Status  string `json:"status"`
	}
	body, _ := json.Marshal(OrderResult{OrderID: orderID, Status: "processed"})

	return &dedup.Envelope{
		Payload: body,
		Meta:    map[string]string{"content-type": "application/json"},
	}, nil
}
