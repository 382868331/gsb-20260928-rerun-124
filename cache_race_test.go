package cacheruntime

import (
	"context"
	"sync"
	"testing"
)

func TestConcurrentStaleReadsShareRefresh(t *testing.T) {
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
	<-initialDone

	loader.set("new", 2)
	loader.gate = make(chan struct{})
	cache.Invalidate("k")

	var wait sync.WaitGroup
	for range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			v, version, err := cache.GetWith(context.Background(), "k", GetOptions{AllowStaleOnRefresh: true})
			if err != nil || v != "old" || version != 1 {
				t.Errorf("stale read = %q %d %v", v, version, err)
			}
		}()
	}
	wait.Wait()

	if cache.Stats().StartedLoads != 2 {
		t.Fatalf("started loads = %d, want 2", cache.Stats().StartedLoads)
	}
	loader.release()
	waitForStats(t, cache, func(stats Stats) bool { return stats.CompletedLoads == 2 })
}
