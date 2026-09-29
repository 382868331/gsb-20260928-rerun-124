package cacheruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type controllableLoader struct {
	started atomic.Int64
	mu      sync.Mutex
	version uint64
	value   string
	gate    chan struct{}
}

func newControllableLoader(value string, version uint64) *controllableLoader {
	return &controllableLoader{value: value, version: version, gate: make(chan struct{})}
}

func (l *controllableLoader) load(ctx context.Context, key string) (Loaded[string], error) {
	l.started.Add(1)
	select {
	case <-l.gate:
	case <-ctx.Done():
		return Loaded[string]{}, ctx.Err()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return Loaded[string]{Value: l.value, Version: l.version}, nil
}

func (l *controllableLoader) set(value string, version uint64) {
	l.mu.Lock()
	l.value = value
	l.version = version
	l.mu.Unlock()
}

func (l *controllableLoader) release() {
	close(l.gate)
}

func waitForStats(t *testing.T, cache *Cache[string, string], match func(Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if match(cache.Stats()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for stats: %+v", cache.Stats())
}

func TestConcurrentGetCoalescesLoad(t *testing.T) {
	loader := newControllableLoader("first", 1)
	cache := New(Options[string, string]{Loader: loader.load})
	t.Cleanup(func() { _ = cache.Close() })

	results := make(chan result, 2)
	go func() { v, version, err := cache.Get(context.Background(), "k"); results <- result{v, version, err} }()
	waitForStats(t, cache, func(stats Stats) bool { return stats.StartedLoads == 1 })
	go func() { v, version, err := cache.Get(context.Background(), "k"); results <- result{v, version, err} }()
	waitForStats(t, cache, func(stats Stats) bool { return stats.CoalescedLoads == 1 })

	loader.release()
	for range 2 {
		got := <-results
		if got.value != "first" || got.version != 1 || got.err != nil {
			t.Fatalf("unexpected result: %+v", got)
		}
	}
	if got := loader.started.Load(); got != 1 {
		t.Fatalf("loader started %d times, want 1", got)
	}
	if v, version, err := cache.Get(context.Background(), "k"); err != nil || v != "first" || version != 1 {
		t.Fatalf("cached result = %q %d %v", v, version, err)
	}
}

type result struct {
	value   string
	version uint64
	err     error
}

func TestCanceledWaiterDoesNotCancelSharedLoad(t *testing.T) {
	loader := newControllableLoader("shared", 1)
	cache := New(Options[string, string]{Loader: loader.load})
	t.Cleanup(func() { _ = cache.Close() })

	waiterOne, cancelOne := context.WithCancel(context.Background())
	done := make(chan result, 1)
	go func() { v, version, err := cache.Get(waiterOne, "k"); done <- result{v, version, err} }()
	waitForStats(t, cache, func(stats Stats) bool { return stats.StartedLoads == 1 })
	go func() { v, version, err := cache.Get(context.Background(), "k"); done <- result{v, version, err} }()
	waitForStats(t, cache, func(stats Stats) bool { return stats.CoalescedLoads == 1 })

	cancelOne()
	got := <-done
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v", got.err)
	}
	loader.release()
	got = <-done
	if got.value != "shared" || got.version != 1 || got.err != nil {
		t.Fatalf("remaining waiter got %+v", got)
	}
	if got := loader.started.Load(); got != 1 {
		t.Fatalf("loader started %d times", got)
	}
}

func TestStaleReadAllowsBackgroundRefresh(t *testing.T) {
	loader := newControllableLoader("old", 1)
	cache := New(Options[string, string]{Loader: loader.load})
	t.Cleanup(func() { _ = cache.Close() })

	initialDone := make(chan result, 1)
	go func() {
		v, version, err := cache.Get(context.Background(), "k")
		initialDone <- result{v, version, err}
	}()
	waitForStats(t, cache, func(stats Stats) bool { return stats.StartedLoads == 1 })
	loader.release()
	if got := <-initialDone; got.err != nil || got.value != "old" || got.version != 1 {
		t.Fatalf("initial get = %+v", got)
	}
	loader.set("new", 2)
	loader.gate = make(chan struct{})
	cache.Invalidate("k")

	v, version, err := cache.GetWith(context.Background(), "k", GetOptions{AllowStaleOnRefresh: true})
	if err != nil || v != "old" || version != 1 {
		t.Fatalf("stale result = %q %d %v", v, version, err)
	}
	waitForStats(t, cache, func(stats Stats) bool { return stats.StartedLoads == 2 })
	loader.release()
	waitForStats(t, cache, func(stats Stats) bool { return stats.CompletedLoads == 2 })

	v, version, err = cache.Get(context.Background(), "k")
	if err != nil || v != "new" || version != 2 {
		t.Fatalf("refreshed result = %q %d %v", v, version, err)
	}
}

func TestInvalidatedLoadCannotOverwriteNewerValue(t *testing.T) {
	loader := newControllableLoader("old", 1)
	cache := New(Options[string, string]{Loader: loader.load})
	t.Cleanup(func() { _ = cache.Close() })

	oldDone := make(chan result, 1)
	go func() { v, version, err := cache.Get(context.Background(), "k"); oldDone <- result{v, version, err} }()
	waitForStats(t, cache, func(stats Stats) bool { return stats.StartedLoads == 1 })
	cache.Invalidate("k")
	if err := cache.Put("k", Loaded[string]{Value: "newer", Version: 10}); err != nil {
		t.Fatalf("put newer: %v", err)
	}
	loader.release()
	if got := <-oldDone; !errors.Is(got.err, ErrStaleVersion) {
		t.Fatalf("old load error = %v, want stale version", got.err)
	}
	v, version, err := cache.Get(context.Background(), "k")
	if err != nil || v != "newer" || version != 10 {
		t.Fatalf("value after stale load = %q %d %v", v, version, err)
	}
}

func TestOlderVersionIsRejected(t *testing.T) {
	loader := newControllableLoader("old", 1)
	cache := New(Options[string, string]{Loader: loader.load})
	t.Cleanup(func() { _ = cache.Close() })

	if err := cache.Put("k", Loaded[string]{Value: "current", Version: 5}); err != nil {
		t.Fatalf("put: %v", err)
	}
	loader.release()
	_, _, err := cache.Refresh(context.Background(), "k")
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("refresh error = %v, want stale version", err)
	}
	v, version, err := cache.Get(context.Background(), "k")
	if err != nil || v != "current" || version != 5 {
		t.Fatalf("current value changed: %q %d %v", v, version, err)
	}
}

func TestCapacityEvictsLeastRecentlyUsed(t *testing.T) {
	evicted := make(chan Eviction[string, string], 2)
	cache := New(Options[string, string]{
		Capacity: 2,
		OnEviction: func(event Eviction[string, string]) {
			evicted <- event
		},
	})
	t.Cleanup(func() { _ = cache.Close() })

	for _, key := range []string{"a", "b", "c"} {
		if err := cache.Put(key, Loaded[string]{Value: key, Version: 1}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	event := <-evicted
	if event.Key != "a" || event.Reason != EvictionCapacity {
		t.Fatalf("eviction = %+v", event)
	}
	if cache.Len() != 2 {
		t.Fatalf("len = %d", cache.Len())
	}
	if cache.Stats().Evictions != 1 {
		t.Fatalf("eviction stat = %d", cache.Stats().Evictions)
	}
}

type spyStorage struct {
	Storage[string, string]
	saves atomic.Int64
}

func (s *spyStorage) Save(key string, record Record[string]) {
	s.saves.Add(1)
	s.Storage.Save(key, record)
}

func TestStorageCanBeReplaced(t *testing.T) {
	storage := &spyStorage{Storage: NewMemoryStorage[string, string]()}
	cache := New(Options[string, string]{
		Storage: storage,
		Loader: func(ctx context.Context, key string) (Loaded[string], error) {
			return Loaded[string]{Value: "from-backend", Version: 7}, nil
		},
	})
	t.Cleanup(func() { _ = cache.Close() })

	v, version, err := cache.Get(context.Background(), "k")
	if err != nil || v != "from-backend" || version != 7 {
		t.Fatalf("get = %q %d %v", v, version, err)
	}
	if storage.saves.Load() == 0 {
		t.Fatal("replacement storage was not used")
	}
}
