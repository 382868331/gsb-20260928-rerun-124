package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func diamondResult(key string, version uint64) SourceResult {
	switch key {
	case "base":
		return SourceResult{Value: fmt.Sprintf("base-%d", version), Version: version}
	case "left":
		return SourceResult{
			Value:       fmt.Sprintf("left-%d", version),
			Version:     version,
			Deps:        []string{"base"},
			DepVersions: map[string]uint64{"base": version},
		}
	case "right":
		return SourceResult{
			Value:       fmt.Sprintf("right-%d", version),
			Version:     version,
			Deps:        []string{"base"},
			DepVersions: map[string]uint64{"base": version},
		}
	case "summary":
		return SourceResult{
			Value:       fmt.Sprintf("summary-%d", version),
			Version:     version,
			Deps:        []string{"left", "right"},
			DepVersions: map[string]uint64{"left": version, "right": version},
		}
	}
	return SourceResult{}
}

func TestCompositeViewDiamondRefresh(t *testing.T) {
	version := uint64(1)
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		return diamondResult(key, version), nil
	}})

	old, err := c.ReadView(context.Background(), "summary")
	if err != nil {
		t.Fatalf("initial ReadView: %v", err)
	}
	if !old.Complete || old.CommitID == 0 {
		t.Fatalf("initial view not committed: complete=%v commit=%d", old.Complete, old.CommitID)
	}
	for _, key := range []string{"summary", "left", "right", "base"} {
		if item := old.Items[key]; item.SourceVersion != 1 || item.Status != ViewCommitted {
			t.Fatalf("initial %s = %+v", key, item)
		}
	}

	version = 2
	newView, err := old.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got, _ := old.Get("summary"); got != "summary-1" {
		t.Fatalf("old view changed to %v", got)
	}
	if old.Read() != old {
		t.Fatal("repeated read did not retain the same view")
	}
	if got, _ := newView.Get("summary"); got != "summary-2" {
		t.Fatalf("new view summary = %v", got)
	}
	if newView.CommitID == old.CommitID || !newView.Complete {
		t.Fatalf("new diamond view was not committed: %+v", newView)
	}
	latest, err := c.ReadView(context.Background(), "summary")
	if err != nil {
		t.Fatalf("reopening ReadView: %v", err)
	}
	if got, _ := latest.Get("summary"); got != "summary-2" {
		t.Fatalf("reopened view = %v, want summary-2", got)
	}
	if got, _ := old.Get("summary"); got != "summary-1" {
		t.Fatalf("old view after reopen = %v, want summary-1", got)
	}
}

func TestCompositeRefreshKeepsOldViewAndExposesCandidates(t *testing.T) {
	var mu sync.Mutex
	version := uint64(1)
	failBase := false
	baseGate := make(chan struct{})
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		mu.Lock()
		v := version
		fail := failBase && key == "base"
		mu.Unlock()
		if key == "base" && v == 2 {
			<-baseGate
		}
		if fail {
			return SourceResult{}, fmt.Errorf("base unavailable")
		}
		return diamondResult(key, v), nil
	}})

	old, err := c.ReadView(context.Background(), "summary")
	if err != nil {
		t.Fatalf("initial ReadView: %v", err)
	}

	mu.Lock()
	version = 2
	failBase = true
	mu.Unlock()
	failedRefresh := make(chan struct{})
	go func() {
		_, _ = old.Refresh(context.Background())
		close(failedRefresh)
	}()
	select {
	case <-failedRefresh:
		t.Fatal("refresh returned before blocked base load")
	default:
	}
	close(baseGate)
	select {
	case <-failedRefresh:
	case <-time.After(2 * time.Second):
		t.Fatal("failed refresh did not return")
	}

	if got, _ := old.Get("base"); got != "base-1" {
		t.Fatalf("failed refresh changed retained view: %v", got)
	}
	if item := old.Items["left"]; item.SourceVersion != 1 || item.Status != ViewCommitted {
		t.Fatalf("old left changed: %+v", item)
	}
	if len(old.Candidates) != 0 {
		t.Fatalf("delivered view gained candidates later: %+v", old.Candidates)
	}

	// Repeat the failed refresh and capture its returned provisional view.
	baseGate = make(chan struct{})
	var provisional *View
	var refreshErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		provisional, refreshErr = old.Refresh(context.Background())
	}()
	time.Sleep(20 * time.Millisecond)
	close(baseGate)
	wg.Wait()
	if refreshErr == nil {
		t.Fatal("partial source failure unexpectedly committed")
	}
	if provisional.Complete {
		t.Fatal("provisional view marked complete")
	}
	if got, _ := provisional.Get("summary"); got != "summary-1" {
		t.Fatalf("provisional selected view mixed baseline: %v", got)
	}
	if provisional.Candidates["left"].SourceVersion != 2 || provisional.Candidates["right"].SourceVersion != 2 {
		t.Fatalf("candidate dependency changes unavailable: %+v", provisional.Candidates)
	}
	if item := provisional.Items["base"]; item.Status != ViewUnavailable {
		t.Fatalf("base status = %s, want unavailable", item.Status)
	}

	mu.Lock()
	failBase = false
	mu.Unlock()
	committed, err := provisional.Refresh(context.Background())
	if err != nil {
		t.Fatalf("retry refresh: %v", err)
	}
	if got, _ := committed.Get("summary"); got != "summary-2" {
		t.Fatalf("committed retry = %v", got)
	}
	if got, _ := old.Get("summary"); got != "summary-1" {
		t.Fatalf("commit mutated retained old view: %v", got)
	}
}

func TestCompositeWaiterExitDoesNotRetainLoad(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		if key == "summary" {
			calls.Add(1)
			<-release
		}
		return diamondResult(key, 1), nil
	}})

	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan struct{})
	go func() {
		_, _ = c.ReadView(ctx, "summary")
		close(exited)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-exited
	c.Invalidate("summary")
	close(release)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := len(c.viewLoads)
		c.mu.Unlock()
		if n == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.mu.Lock()
	n := len(c.viewLoads)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("view loads retained after waiter exit and source completion: %d", n)
	}
	if v, err := c.ReadView(context.Background(), "summary"); err != nil {
		t.Fatalf("new view after waiter exit: %v", err)
	} else if got, _ := v.Get("summary"); got != "summary-1" {
		t.Fatalf("new view = %v", got)
	}
}

func TestOverlappingCompositeRefreshesShareLoads(t *testing.T) {
	var calls atomic.Int64
	version := uint64(1)
	releaseSummary := make(chan struct{})
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		calls.Add(1)
		if key == "summary" && version == 2 {
			<-releaseSummary
		}
		return diamondResult(key, version), nil
	}})
	old, err := c.ReadView(context.Background(), "summary")
	if err != nil {
		t.Fatalf("initial ReadView: %v", err)
	}

	version = 2
	var wg sync.WaitGroup
	views := make([]*View, 2)
	errs := make([]error, 2)
	for i := range views {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			views[i], errs[i] = old.Refresh(context.Background())
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	close(releaseSummary)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("refresh %d: %v", i, err)
		}
		if got, _ := views[i].Get("summary"); got != "summary-2" {
			t.Fatalf("refresh %d summary = %v", i, got)
		}
	}
	if calls.Load() != 8 {
		t.Fatalf("loader calls = %d, want 8 (four keys across two versions)", calls.Load())
	}
}
