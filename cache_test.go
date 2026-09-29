package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Concurrent Gets for the same key must share one loader call and all
// observe the same value from the authoritative source.
func TestMergedConcurrentLoad(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	c := New(Options{Loader: func(ctx context.Context, key string) (any, error) {
		calls.Add(1)
		<-release
		return "v-" + key, nil
	}})

	const n = 32
	var wg sync.WaitGroup
	results := make([]any, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := c.Get(context.Background(), "k")
			if err != nil {
				t.Errorf("Get: %v", err)
			}
			results[i] = v
		}(i)
	}
	// Wait until the single load is in flight, then release it.
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("loader called %d times, want 1", got)
	}
	for i, v := range results {
		if v != "v-k" {
			t.Fatalf("waiter %d got %v", i, v)
		}
	}
	if s := c.Stats(); s.Hits+s.Misses != n || s.Loads != 1 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

// A load started before Invalidate must not overwrite the newer state.
func TestStaleLoadCannotOverwrite(t *testing.T) {
	var calls atomic.Int64
	block := make(chan struct{})
	c := New(Options{Loader: func(ctx context.Context, key string) (any, error) {
		n := calls.Add(1)
		if n == 1 {
			<-block // first load: hold it so we can invalidate mid-flight
			return "old", nil
		}
		return "new", nil
	}})

	first := make(chan any, 1)
	go func() {
		v, _ := c.Get(context.Background(), "k")
		first <- v
	}()
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	c.Invalidate("k")
	close(block)
	if v := <-first; v != "old" { // the in-flight waiter still gets its result
		t.Fatalf("first waiter got %v", v)
	}
	// The stale result was dropped; the next Get reloads from source.
	v, err := c.Get(context.Background(), "k")
	if err != nil || v != "new" {
		t.Fatalf("after invalidate got %v, %v", v, err)
	}
	if s := c.Stats(); s.StaleDrops != 1 || s.Loads != 2 {
		t.Fatalf("unexpected stats: %+v", s)
	}
	if ver := c.Version("k"); ver != 1 {
		t.Fatalf("version = %d, want 1", ver)
	}
}

// Cancelling one waiter must not cancel the shared load for others.
func TestWaiterCancelDoesNotCancelSharedLoad(t *testing.T) {
	release := make(chan struct{})
	c := New(Options{Loader: func(ctx context.Context, key string) (any, error) {
		select {
		case <-release:
			return "shared", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}})

	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := c.Get(ctx, "k")
		cancelled <- err
	}()
	<-started
	// Give the first waiter a moment to register, then add a second.
	time.Sleep(20 * time.Millisecond)
	second := make(chan any, 1)
	go func() {
		v, err := c.Get(context.Background(), "k")
		if err != nil {
			second <- err
			return
		}
		second <- v
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-cancelled; err != context.Canceled {
		t.Fatalf("cancelled waiter err = %v", err)
	}
	close(release)
	if v := <-second; v != "shared" {
		t.Fatalf("second waiter got %v", v)
	}
}

// Capacity pressure must evict LRU entries observably.
func TestCapacityEviction(t *testing.T) {
	var mu sync.Mutex
	evicted := map[string]any{}
	c := New(Options{
		Capacity: 2,
		Loader: func(ctx context.Context, key string) (any, error) {
			return "val-" + key, nil
		},
		OnEvict: func(key string, e Entry) {
			mu.Lock()
			evicted[key] = e.Value
			mu.Unlock()
		},
	})
	ctx := context.Background()
	mustGet := func(k string) {
		if _, err := c.Get(ctx, k); err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
	}
	mustGet("a")
	mustGet("b")
	mustGet("a") // refresh a; b becomes LRU
	mustGet("c") // evicts b

	if c.Len() != 2 {
		t.Fatalf("len = %d, want 2", c.Len())
	}
	mu.Lock()
	if len(evicted) != 1 || evicted["b"] != "val-b" {
		t.Fatalf("evicted = %v", evicted)
	}
	mu.Unlock()
	if s := c.Stats(); s.Evictions != 1 {
		t.Fatalf("evictions = %d", s.Evictions)
	}
}

// With AllowStale, an expired entry is served while a refresh reloads.
func TestStaleWhileRevalidate(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	c := New(Options{
		TTL:        30 * time.Millisecond,
		AllowStale: true,
		Loader: func(ctx context.Context, key string) (any, error) {
			n := calls.Add(1)
			if n > 1 {
				<-release
			}
			return fmt.Sprintf("gen-%d", n), nil
		},
	})
	ctx := context.Background()

	if v, _ := c.Get(ctx, "k"); v != "gen-1" {
		t.Fatalf("initial = %v", v)
	}
	time.Sleep(50 * time.Millisecond) // let the entry expire
	v, err := c.Get(ctx, "k")
	if err != nil || v != "gen-1" {
		t.Fatalf("stale read = %v, %v", v, err)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := c.Get(ctx, "k"); v == "gen-2" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if v, _ := c.Get(ctx, "k"); v != "gen-2" {
		t.Fatalf("refreshed = %v", v)
	}
	if s := c.Stats(); s.StaleHits == 0 {
		t.Fatalf("no stale hits recorded: %+v", s)
	}
}

// A swapped-in Store implementation observes identical results.
func TestAlternateStoreSameResults(t *testing.T) {
	run := func(store Store) (any, Stats) {
		var calls atomic.Int64
		c := New(Options{
			Store:    store,
			Capacity: 1,
			Loader: func(ctx context.Context, key string) (any, error) {
				calls.Add(1)
				return "x-" + key, nil
			},
		})
		ctx := context.Background()
		c.Get(ctx, "a")
		c.Get(ctx, "b") // evicts a
		v, _ := c.Get(ctx, "b")
		return v, c.Stats()
	}
	v1, s1 := run(nil)
	v2, s2 := run(NewMapStore())
	if v1 != v2 || s1 != s2 {
		t.Fatalf("results differ: %v/%+v vs %v/%+v", v1, s1, v2, s2)
	}
}

// Hammer the cache with concurrent loads, invalidations and cancels.
func TestConcurrentLoadInvalidateCancel(t *testing.T) {
	var calls atomic.Int64
	c := New(Options{
		Capacity: 8,
		Loader: func(ctx context.Context, key string) (any, error) {
			calls.Add(1)
			time.Sleep(time.Millisecond)
			return calls.Load(), nil
		},
	})
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i%8)
			ctx := context.Background()
			if i%3 == 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(i%5)*time.Millisecond)
				defer cancel()
			}
			for j := 0; j < 20; j++ {
				if i%7 == 0 {
					c.Invalidate(key)
				}
				c.Get(ctx, key) //nolint:errcheck
			}
		}(i)
	}
	wg.Wait()
	if c.Len() > 8 {
		t.Fatalf("len = %d exceeds capacity", c.Len())
	}
}
