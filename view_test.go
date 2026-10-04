package cache

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// diamondLoader builds a source where b and c derive from a, and d
// derives from b and c (a diamond). Bumping aVersion changes every
// derived value.
func diamondLoader(aVersion *atomic.Int64, fail map[string]bool) SourceLoader {
	return func(ctx context.Context, key string) (SourceResult, error) {
		if fail[key] {
			return SourceResult{}, fmt.Errorf("source %s unavailable", key)
		}
		v := aVersion.Load()
		switch key {
		case "a":
			return SourceResult{Value: fmt.Sprintf("a-%d", v), Version: uint64(v)}, nil
		case "b":
			return SourceResult{Value: fmt.Sprintf("b-of-a-%d", v), Version: uint64(v), Deps: []string{"a"}}, nil
		case "c":
			return SourceResult{Value: fmt.Sprintf("c-of-a-%d", v), Version: uint64(v), Deps: []string{"a"}}, nil
		case "d":
			return SourceResult{Value: fmt.Sprintf("d-of-bc-%d", v), Version: uint64(v), Deps: []string{"b", "c"}}, nil
		}
		return SourceResult{}, fmt.Errorf("unknown key %s", key)
	}
}

func mustItem(t *testing.T, v *View, key string) ViewItem {
	t.Helper()
	it, ok := v.Get(key)
	if !ok {
		t.Fatalf("view has no key %s", key)
	}
	return it
}

// A retained view over a diamond dependency graph stays stable across
// repeated reads and across later refreshes, while a fresh view sees
// the latest values; the two views explain their difference through
// per-key source versions.
func TestViewDiamondStableAndExplainable(t *testing.T) {
	var aVersion atomic.Int64
	aVersion.Store(1)
	c := New(Options{SourceLoader: diamondLoader(&aVersion, nil)})
	ctx := context.Background()

	old := c.Snapshot(ctx, "d", "b", "c", "a")
	if !old.Complete() {
		t.Fatalf("initial view incomplete: %+v", old.Items())
	}
	if it := mustItem(t, old, "d"); it.Value != "d-of-bc-1" || it.SourceVersion != 1 {
		t.Fatalf("d = %+v", it)
	}

	// Refresh the world: a changes, invalidation propagates b/c/d.
	aVersion.Store(2)
	c.Invalidate("a")
	fresh := c.Snapshot(ctx, "d", "b", "c", "a")
	if it := mustItem(t, fresh, "d"); it.Value != "d-of-bc-2" {
		t.Fatalf("fresh d = %+v", it)
	}

	// The retained view never changed: repeated reads are identical.
	for i := 0; i < 3; i++ {
		for _, k := range []string{"a", "b", "c", "d"} {
			it := mustItem(t, old, k)
			if it.State != ItemReady || it.SourceVersion != 1 {
				t.Fatalf("retained view mutated at %s: %+v", k, it)
			}
		}
	}
	if it := mustItem(t, old, "d"); it.Value != "d-of-bc-1" {
		t.Fatalf("retained d = %+v, want d-of-bc-1", it)
	}

	// Reopening (a brand-new snapshot) still sees the latest, and the
	// difference between the two views is explainable per key.
	reopened := c.Snapshot(ctx, "a", "b", "c", "d")
	for _, k := range []string{"a", "b", "c", "d"} {
		oldIt := mustItem(t, old, k)
		newIt := mustItem(t, reopened, k)
		if newIt.SourceVersion != 2 || oldIt.SourceVersion != 1 {
			t.Fatalf("%s: versions old=%d new=%d, want 1 and 2",
				k, oldIt.SourceVersion, newIt.SourceVersion)
		}
		if oldIt.Value == newIt.Value {
			t.Fatalf("%s: expected differing values, both %v", k, oldIt.Value)
		}
	}
}

// When part of a refresh fails, the base view is retained, the
// candidate's partial changes stay inspectable, and adoption is refused
// until every dependency is satisfied.
func TestCandidatePartialFailureRetainsBase(t *testing.T) {
	var aVersion atomic.Int64
	aVersion.Store(1)
	fail := map[string]bool{}
	c := New(Options{SourceLoader: diamondLoader(&aVersion, fail)})
	ctx := context.Background()

	base := c.Snapshot(ctx, "a", "b", "c", "d")
	aVersion.Store(2)
	c.Invalidate("a")
	fail["c"] = true // c's source is down for this refresh round

	cand := base.Refresh(ctx)
	if cand.State() != CandidateFailed {
		t.Fatalf("candidate state = %v, want CandidateFailed (%s)", cand.State(), cand.Reason())
	}
	// Partial changes are inspectable: b and d carry the new values.
	if it, _ := cand.Get("b"); it.State != ItemReady || it.Value != "b-of-a-2" {
		t.Fatalf("candidate b = %+v", it)
	}
	if it, _ := cand.Get("c"); it.State != ItemFailed || it.Err == nil {
		t.Fatalf("candidate c = %+v, want failed", it)
	}
	// Adoption refused: dependencies of d are not all satisfied.
	if v, ok := cand.Adopt(); ok || v != base {
		t.Fatalf("Adopt = (%p, %v), want base view and false", v, ok)
	}
	// Base view untouched.
	if it := mustItem(t, base, "d"); it.Value != "d-of-bc-1" {
		t.Fatalf("base d = %+v after failed refresh", it)
	}

	// Source recovers: the next candidate is adoptable.
	delete(fail, "c")
	cand2 := base.Refresh(ctx)
	if cand2.State() != CandidateReady {
		t.Fatalf("candidate2 state = %v (%s)", cand2.State(), cand2.Reason())
	}
	next, ok := cand2.Adopt()
	if !ok {
		t.Fatal("Adopt of ready candidate refused")
	}
	if it := mustItem(t, next, "d"); it.Value != "d-of-bc-2" || it.SourceVersion != 2 {
		t.Fatalf("adopted d = %+v", it)
	}
	// The old view is still the old observation.
	if it := mustItem(t, base, "d"); it.Value != "d-of-bc-1" {
		t.Fatalf("base d mutated after adoption: %+v", it)
	}
}

// A source changing again while a candidate loads (crossing refresh)
// aborts the candidate instead of letting it linger or be adopted.
func TestCandidateSupersededByCrossRefresh(t *testing.T) {
	var aVersion atomic.Int64
	aVersion.Store(1)
	gate := make(chan struct{})
	var cLoads atomic.Int64
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		v := aVersion.Load()
		if key == "c" && cLoads.Add(1) == 2 {
			<-gate // hold the candidate's reload of c
		}
		switch key {
		case "a":
			return SourceResult{Value: fmt.Sprintf("a-%d", v), Version: uint64(v)}, nil
		case "c":
			return SourceResult{Value: fmt.Sprintf("c-of-a-%d", v), Version: uint64(v), Deps: []string{"a"}}, nil
		}
		return SourceResult{}, fmt.Errorf("unknown key %s", key)
	}})
	ctx := context.Background()

	base := c.Snapshot(ctx, "a", "c")
	aVersion.Store(2)
	c.Invalidate("a")

	type result struct{ cand *Candidate }
	done := make(chan result, 1)
	go func() { done <- result{base.Refresh(ctx)} }()
	deadline := time.Now().Add(2 * time.Second)
	for cLoads.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Cross refresh: the sources change again mid-candidate.
	aVersion.Store(3)
	c.Invalidate("a")
	c.Invalidate("c")
	close(gate)

	cand := (<-done).cand
	if cand.State() != CandidateAborted {
		t.Fatalf("candidate state = %v, want CandidateAborted", cand.State())
	}
	if !strings.Contains(cand.Reason(), "superseded") {
		t.Fatalf("reason = %q, want superseded", cand.Reason())
	}
	if _, ok := cand.Adopt(); ok {
		t.Fatal("superseded candidate must not be adoptable")
	}
	// A fresh view observes the newest source state.
	latest := c.Snapshot(ctx, "a", "c")
	if it := mustItem(t, latest, "c"); it.Value != "c-of-a-3" || it.SourceVersion != 3 {
		t.Fatalf("latest c = %+v, want c-of-a-3 v3", it)
	}
}

// A waiter leaving mid-refresh aborts the candidate with a recorded
// reason; the shared loads still complete for everyone else and nothing
// is pinned afterwards.
func TestCandidateWaiterLeft(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		if calls.Add(1) > 2 { // the refresh round blocks; the initial snapshot does not
			<-release
		}
		return SourceResult{Value: "v-" + key, Version: 1}, nil
	}})
	ctx := context.Background()
	base := c.Snapshot(ctx, "x", "y")
	c.Invalidate("x")
	c.Invalidate("y")

	rctx, cancel := context.WithCancel(ctx)
	done := make(chan *Candidate, 1)
	go func() { done <- base.Refresh(rctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel() // the waiter walks away
	cand := <-done
	if cand.State() != CandidateAborted || !strings.Contains(cand.Reason(), "waiter-left") {
		t.Fatalf("candidate = %v (%q), want aborted waiter-left", cand.State(), cand.Reason())
	}
	close(release)
	// Nothing lingers: a fresh snapshot completes normally.
	latest := c.Snapshot(ctx, "x", "y")
	if !latest.Complete() {
		t.Fatalf("snapshot after waiter left incomplete: %+v", latest.Items())
	}
}

// Snapshot reports pending and failed members explicitly instead of
// dropping them or blocking forever.
func TestSnapshotPendingAndFailedParts(t *testing.T) {
	block := make(chan struct{})
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		switch key {
		case "ok":
			return SourceResult{Value: "fine", Version: 1}, nil
		case "bad":
			return SourceResult{}, fmt.Errorf("boom")
		case "slow":
			<-block
			return SourceResult{Value: "late", Version: 1}, nil
		}
		return SourceResult{}, fmt.Errorf("unknown key %s", key)
	}})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	v := c.Snapshot(ctx, "ok", "bad", "slow")
	close(block)

	if v.Complete() {
		t.Fatal("view with failed/pending parts must not be complete")
	}
	if it := mustItem(t, v, "ok"); it.State != ItemReady || it.Value != "fine" {
		t.Fatalf("ok = %+v", it)
	}
	if it := mustItem(t, v, "bad"); it.State != ItemFailed || it.Err == nil {
		t.Fatalf("bad = %+v, want failed", it)
	}
	if it := mustItem(t, v, "slow"); it.State != ItemPending {
		t.Fatalf("slow = %+v, want pending", it)
	}
}

// Duplicate keys collapse into one member, and an abandoned candidate
// records where it went.
func TestViewDedupAndAbandon(t *testing.T) {
	var calls atomic.Int64
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		calls.Add(1)
		return SourceResult{Value: "v", Version: 1}, nil
	}})
	ctx := context.Background()
	v := c.Snapshot(ctx, "k", "k", "k")
	if got := len(v.Keys()); got != 1 {
		t.Fatalf("keys = %v, want 1 entry", v.Keys())
	}
	c.Invalidate("k")
	cand := v.Refresh(ctx)
	if cand.State() != CandidateReady {
		t.Fatalf("state = %v", cand.State())
	}
	cand.Abandon()
	if cand.State() != CandidateAborted || !strings.Contains(cand.Reason(), "abandoned") {
		t.Fatalf("after Abandon: %v (%q)", cand.State(), cand.Reason())
	}
	if _, ok := cand.Adopt(); ok {
		t.Fatal("abandoned candidate must not be adoptable")
	}
}
