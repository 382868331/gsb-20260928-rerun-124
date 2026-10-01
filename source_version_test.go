package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type eventLog struct {
	mu     sync.Mutex
	events []Event
}

func (l *eventLog) add(e Event) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *eventLog) count(op EventOp, reason string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.events {
		if e.Op == op && e.Reason == reason {
			n++
		}
	}
	return n
}

// A source read of an older business version that finishes late must not
// overwrite the newer known result, even though no local invalidation
// happened in between.
func TestOlderSourceReadCannotOverwrite(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	log := &eventLog{}
	c := New(Options{
		TTL:        30 * time.Millisecond,
		AllowStale: true,
		OnEvent:    log.add,
		SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
			n := calls.Add(1)
			switch n {
			case 1:
				return SourceResult{Value: "v10", Version: 10}, nil
			case 2:
				<-release // slow stale replica read
				return SourceResult{Value: "v7", Version: 7}, nil
			default:
				return SourceResult{Value: "v11", Version: 11}, nil
			}
		},
	})
	ctx := context.Background()

	if v, _ := c.Get(ctx, "k"); v != "v10" {
		t.Fatalf("initial = %v", v)
	}
	time.Sleep(50 * time.Millisecond) // expire the entry
	// Stale read: serves v10 and starts the slow background refresh.
	if v, _ := c.Get(ctx, "k"); v != "v10" {
		t.Fatalf("stale read = %v", v)
	}
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	// Wait for the dropped refresh to finish, then force a real reload.
	deadline = time.Now().Add(2 * time.Second)
	for c.Stats().OldSourceDrops == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := c.SourceVersion("k"); got != 10 {
		t.Fatalf("source version regressed to %d", got)
	}
	time.Sleep(50 * time.Millisecond) // expire again
	c.Refresh(ctx, "k")
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := c.Get(ctx, "k"); v == "v11" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if v, _ := c.Get(ctx, "k"); v != "v11" {
		t.Fatalf("final = %v, want v11", v)
	}
	if s := c.Stats(); s.OldSourceDrops != 1 {
		t.Fatalf("OldSourceDrops = %d, want 1 (%+v)", s.OldSourceDrops, s)
	}
	if n := log.count(EventDropped, "older-source-version"); n != 1 {
		t.Fatalf("drop events = %d, want 1", n)
	}
}

// Under concurrent refreshes the final stored value must be the one with
// the highest source version, regardless of completion order.
func TestConcurrentRefreshKeepsNewestSource(t *testing.T) {
	ctx := context.Background()
	// Let a slow load of an older source version run while a newer
	// version becomes known through a later load.
	gate := make(chan struct{})
	var calls atomic.Int64
	c2 := New(Options{
		SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
			n := calls.Add(1)
			if n == 1 {
				<-gate
				return SourceResult{Value: "old-read", Version: 5}, nil
			}
			return SourceResult{Value: "new-read", Version: 9}, nil
		},
	})
	first := make(chan any, 1)
	go func() {
		v, _ := c2.Get(ctx, "k")
		first <- v
	}()
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// The newer snapshot lands first via a second load after invalidation.
	c2.Invalidate("k")
	second := make(chan any, 1)
	go func() {
		v, _ := c2.Get(ctx, "k")
		second <- v
	}()
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(gate)
	if v := <-first; v != "old-read" {
		t.Fatalf("first waiter = %v", v)
	}
	if v := <-second; v != "new-read" {
		t.Fatalf("second waiter = %v", v)
	}
	if v, _ := c2.Get(ctx, "k"); v != "new-read" {
		t.Fatalf("stored = %v, want new-read", v)
	}
	if got := c2.SourceVersion("k"); got != 9 {
		t.Fatalf("source version = %d, want 9", got)
	}
	if s := c2.Stats(); s.StaleDrops != 1 {
		t.Fatalf("StaleDrops = %d, want 1", s.StaleDrops)
	}
}

// Invalidating a dependency must propagate to entries derived from it,
// and the next read must observe the changed source value.
func TestDependencyInvalidationPropagates(t *testing.T) {
	var profileVersion atomic.Int64
	profileVersion.Store(1)
	log := &eventLog{}
	c := New(Options{
		OnEvent: log.add,
		SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
			switch key {
			case "profile":
				v := profileVersion.Load()
				return SourceResult{Value: fmt.Sprintf("profile-%d", v), Version: uint64(v)}, nil
			case "greeting":
				v := profileVersion.Load()
				return SourceResult{
					Value:   fmt.Sprintf("hello-profile-%d", v),
					Version: uint64(v),
					Deps:    []string{"profile"},
				}, nil
			}
			return SourceResult{}, fmt.Errorf("unknown key %s", key)
		},
	})
	ctx := context.Background()
	if v, _ := c.Get(ctx, "profile"); v != "profile-1" {
		t.Fatalf("profile = %v", v)
	}
	if v, _ := c.Get(ctx, "greeting"); v != "hello-profile-1" {
		t.Fatalf("greeting = %v", v)
	}

	profileVersion.Store(2)
	c.Invalidate("profile")

	// The dependent entry was invalidated too.
	if c.Len() != 0 {
		t.Fatalf("len = %d, want 0 after propagated invalidation", c.Len())
	}
	if n := log.count(EventInvalidated, "invalidate"); n != 2 {
		t.Fatalf("invalidation events = %d, want 2", n)
	}
	if v, _ := c.Get(ctx, "greeting"); v != "hello-profile-2" {
		t.Fatalf("greeting after dep change = %v", v)
	}
}

// Dependency cycles must terminate instead of looping forever.
func TestDependencyCycleTerminates(t *testing.T) {
	c := New(Options{
		SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
			dep := "b"
			if key == "b" {
				dep = "a"
			}
			return SourceResult{Value: "v-" + key, Version: 1, Deps: []string{dep}}, nil
		},
	})
	ctx := context.Background()
	if _, err := c.Get(ctx, "a"); err != nil {
		t.Fatalf("Get a: %v", err)
	}
	if _, err := c.Get(ctx, "b"); err != nil {
		t.Fatalf("Get b: %v", err)
	}
	done := make(chan struct{})
	go func() {
		c.Invalidate("a") // a -> b -> a cycle
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Invalidate did not terminate on a dependency cycle")
	}
	if c.Len() != 0 {
		t.Fatalf("len = %d, want 0", c.Len())
	}
}
