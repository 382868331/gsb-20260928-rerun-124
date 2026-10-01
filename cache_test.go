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
	c := New(Options{Loader: func(ctx context.Context, key string) (Result, error) {
		calls.Add(1)
		<-release
		return Result{Value: "v-" + key}, nil
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
	c := New(Options{Loader: func(ctx context.Context, key string) (Result, error) {
		n := calls.Add(1)
		if n == 1 {
			<-block // first load: hold it so we can invalidate mid-flight
			return Result{Value: "old"}, nil
		}
		return Result{Value: "new"}, nil
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
	c := New(Options{Loader: func(ctx context.Context, key string) (Result, error) {
		select {
		case <-release:
			return Result{Value: "shared"}, nil
		case <-ctx.Done():
			return Result{}, ctx.Err()
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
		Loader: func(ctx context.Context, key string) (Result, error) {
			return Result{Value: "val-" + key}, nil
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
		Loader: func(ctx context.Context, key string) (Result, error) {
			n := calls.Add(1)
			if n > 1 {
				<-release
			}
			return Result{Value: fmt.Sprintf("gen-%d", n)}, nil
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
			Loader: func(ctx context.Context, key string) (Result, error) {
				calls.Add(1)
				return Result{Value: "x-" + key}, nil
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
		Loader: func(ctx context.Context, key string) (Result, error) {
			calls.Add(1)
			time.Sleep(time.Millisecond)
			return Result{Value: calls.Load()}, nil
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

// A load that finishes with an older source version than the one already
// known must be dropped: local generation counts invalidations, it does
// not order source data. Here the source itself regresses (fallback to
// an older business version) while a refresh is in flight.
func TestSourceRegressCannotOverwriteNewer(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	c := New(Options{
		TTL:        30 * time.Millisecond,
		AllowStale: true,
		Loader: func(ctx context.Context, key string) (Result, error) {
			n := calls.Add(1)
			if n == 1 {
				return Result{Value: "new-data", Version: 5}, nil
			}
			<-release
			return Result{Value: "old-data", Version: 3}, nil // source fallback
		},
	})
	ctx := context.Background()

	if v, _ := c.Get(ctx, "k"); v != "new-data" {
		t.Fatalf("initial = %v", v)
	}
	time.Sleep(50 * time.Millisecond)             // expire
	if v, _ := c.Get(ctx, "k"); v != "new-data" { // stale read, refresh starts
		t.Fatalf("stale read = %v", v)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Stats().VersionDrops == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s := c.Stats(); s.VersionDrops != 1 {
		t.Fatalf("VersionDrops = %d, want 1 (%+v)", s.VersionDrops, s)
	}
	if v, _ := c.Get(ctx, "k"); v != "new-data" {
		t.Fatalf("after regressed refresh = %v, want new-data", v)
	}
	if sv := c.SourceVersion("k"); sv != 5 {
		t.Fatalf("SourceVersion = %d, want 5", sv)
	}
	var foundDrop, foundStore bool
	for _, ev := range c.Events() {
		if ev.Key != "k" {
			continue
		}
		if ev.Kind == EventDropped && ev.Reason == "superseded-by-newer-source-version" && ev.Version == 3 {
			foundDrop = true
		}
		if ev.Kind == EventStored && ev.Version == 5 {
			foundStore = true
		}
	}
	if !foundDrop || !foundStore {
		t.Fatalf("events missing drop/store records: %v", c.Events())
	}
}

// A load started before an invalidation stays in flight for its own
// waiters, but a later Get starts a fresh load at the new generation;
// whichever source version is newer wins, independent of completion order.
func TestNewerSourceWinsRegardlessOfCompletionOrder(t *testing.T) {
	var calls atomic.Int64
	slow := make(chan struct{})
	c := New(Options{Loader: func(ctx context.Context, key string) (Result, error) {
		n := calls.Add(1)
		if n == 1 {
			<-slow // first load: old source version, finishes last
			return Result{Value: "v1", Version: 1}, nil
		}
		return Result{Value: "v2", Version: 2}, nil
	}})
	ctx := context.Background()

	first := make(chan any, 1)
	go func() {
		v, _ := c.Get(ctx, "k")
		first <- v
	}()
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	c.Invalidate("k") // first load is now stale; a new Get reloads
	second := make(chan any, 1)
	go func() {
		v, _ := c.Get(ctx, "k")
		second <- v
	}()
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if v := <-second; v != "v2" { // second load commits first, version 2
		t.Fatalf("second got %v", v)
	}
	close(slow)
	if v := <-first; v != "v1" { // first waiter still gets its own result
		t.Fatalf("first waiter got %v", v)
	}
	if v, _ := c.Get(ctx, "k"); v != "v2" {
		t.Fatalf("final = %v, want v2", v)
	}
	if s := c.Stats(); s.StaleDrops != 1 || s.Loads != 2 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

// Invalidating a dependency invalidates everything computed from it.
func TestDependencyInvalidatePropagates(t *testing.T) {
	var sumCalls atomic.Int64
	c := New(Options{Loader: func(ctx context.Context, key string) (Result, error) {
		switch key {
		case "a":
			return Result{Value: "A", Version: 1}, nil
		case "b":
			return Result{Value: "B", Version: 1}, nil
		default: // "sum" is computed from a and b
			n := sumCalls.Add(1)
			return Result{Value: fmt.Sprintf("sum-%d", n), Version: uint64(n), Deps: []string{"a", "b"}}, nil
		}
	}})
	ctx := context.Background()
	for _, k := range []string{"a", "b", "sum"} {
		if _, err := c.Get(ctx, k); err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
	}
	c.Invalidate("a")
	if got := c.Version("sum"); got != 1 {
		t.Fatalf("sum generation = %d, want 1 (propagated)", got)
	}
	if c.Len() != 1 { // only b remains
		t.Fatalf("len = %d, want 1", c.Len())
	}
	if v, _ := c.Get(ctx, "sum"); v != "sum-2" {
		t.Fatalf("reloaded sum = %v, want sum-2", v)
	}
	var propagated bool
	for _, ev := range c.Events() {
		if ev.Key == "sum" && ev.Kind == EventInvalidated && ev.Reason == "dependency-invalidate" {
			propagated = true
		}
	}
	if !propagated {
		t.Fatalf("no dependency-invalidate event for sum: %v", c.Events())
	}
}

// When a reload reports different dependencies, stale reverse edges must
// not keep propagating invalidations from former dependencies.
func TestDependencyChangeStopsOldPropagation(t *testing.T) {
	var sumCalls atomic.Int64
	c := New(Options{Loader: func(ctx context.Context, key string) (Result, error) {
		if key == "sum" {
			n := sumCalls.Add(1)
			dep := "a"
			if n > 1 {
				dep = "b" // second load no longer depends on a
			}
			return Result{Value: fmt.Sprintf("sum-%d", n), Version: uint64(n), Deps: []string{dep}}, nil
		}
		return Result{Value: key, Version: 1}, nil
	}})
	ctx := context.Background()
	c.Get(ctx, "sum") //nolint:errcheck // depends on a
	c.Invalidate("sum")
	c.Get(ctx, "sum") //nolint:errcheck // now depends on b
	if got := c.Version("sum"); got != 1 {
		t.Fatalf("sum generation = %d, want 1", got)
	}
	c.Invalidate("a") // must NOT propagate to sum anymore
	if got := c.Version("sum"); got != 1 {
		t.Fatalf("sum generation after old dep invalidate = %d, want 1", got)
	}
	c.Invalidate("b") // must propagate
	if got := c.Version("sum"); got != 2 {
		t.Fatalf("sum generation after new dep invalidate = %d, want 2", got)
	}
}

// Dependency cycles must not cause infinite invalidation loops.
func TestDependencyCycleTerminates(t *testing.T) {
	c := New(Options{Loader: func(ctx context.Context, key string) (Result, error) {
		if key == "a" {
			return Result{Value: "a", Version: 1, Deps: []string{"b"}}, nil
		}
		return Result{Value: "b", Version: 1, Deps: []string{"a"}}, nil
	}})
	ctx := context.Background()
	c.Get(ctx, "a") //nolint:errcheck
	c.Get(ctx, "b") //nolint:errcheck
	done := make(chan struct{})
	go func() {
		c.Invalidate("a")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Invalidate on a dependency cycle did not terminate")
	}
	if c.Version("a") != 1 || c.Version("b") != 1 {
		t.Fatalf("versions a=%d b=%d, want 1/1", c.Version("a"), c.Version("b"))
	}
}

// Concurrent refreshes across dependency-linked keys must settle on the
// newest source versions, with every drop and update recorded.
func TestConcurrentRefreshWithDeps(t *testing.T) {
	var src atomic.Uint64
	var dropEvents, storeEvents atomic.Uint64
	c := New(Options{
		AllowStale: true,
		TTL:        time.Millisecond,
		OnEvent: func(ev Event) {
			switch ev.Kind {
			case EventDropped:
				dropEvents.Add(1)
			case EventStored:
				storeEvents.Add(1)
			}
		},
		Loader: func(ctx context.Context, key string) (Result, error) {
			v := src.Add(1)
			if key == "derived" {
				return Result{Value: fmt.Sprintf("derived-%d", v), Version: v, Deps: []string{"base"}}, nil
			}
			return Result{Value: fmt.Sprintf("base-%d", v), Version: v}, nil
		},
	})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "base"
			if i%2 == 0 {
				key = "derived"
			}
			for j := 0; j < 25; j++ {
				if j%5 == 0 {
					c.Invalidate("base")
				}
				c.Refresh(ctx, key)
				c.Get(ctx, key) //nolint:errcheck
			}
		}(i)
	}
	wg.Wait()
	// Every committed entry must carry the highest source version known
	// for its key: no older read may have overwritten a newer one.
	for _, key := range []string{"base", "derived"} {
		c.mu.Lock()
		e, ok := c.store.Get(key)
		c.mu.Unlock()
		if ok && e.Version != c.SourceVersion(key) {
			t.Fatalf("%s: stored version %d != known source version %d",
				key, e.Version, c.SourceVersion(key))
		}
	}
	st := c.Stats()
	if drops := dropEvents.Load(); drops != st.StaleDrops+st.VersionDrops {
		t.Fatalf("drop events %d != stats %d", drops, st.StaleDrops+st.VersionDrops)
	}
	if storeEvents.Load() == 0 {
		t.Fatal("no store events recorded")
	}
}
