package cache

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Regression: when a combined refresh partially fails, the values that
// DID load are only candidates. They must not enter the shared cache,
// and every ordinary combined read during the failure window must keep
// returning the previously confirmed, internally consistent view
// instead of stitching new roots/branches onto old leaves and claiming
// completeness.
func TestDiamondPartialFailureDoesNotPublishProvisional(t *testing.T) {
	var aVersion atomic.Int64
	aVersion.Store(1)
	fail := map[string]bool{}
	c := New(Options{SourceLoader: diamondLoader(&aVersion, fail)})
	ctx := context.Background()

	base := c.Snapshot(ctx, "a", "b", "c", "d")
	if !base.Complete() {
		t.Fatalf("initial view incomplete: %+v", base.Items())
	}

	aVersion.Store(2)
	c.Invalidate("a")
	fail["c"] = true // the c leaf source is down for this round

	cand := base.Refresh(ctx)
	if cand.State() != CandidateFailed {
		t.Fatalf("candidate = %v (%s), want CandidateFailed", cand.State(), cand.Reason())
	}
	if _, ok := cand.Adopt(); ok {
		t.Fatal("failed candidate must not be adoptable")
	}

	// Failed part and comparable new values are explained separately in
	// the candidate: c is the failed leaf; a/b carry ready v2 values; d
	// loaded a v2 value too, but it is explicitly excluded from adoption
	// because its c dependency is unavailable (value kept for
	// comparison, never claimed ready).
	if it, _ := cand.Get("c"); it.State != ItemFailed || it.Err == nil {
		t.Fatalf("candidate c = %+v, want failed with error", it)
	}
	for _, k := range []string{"a", "b"} {
		it, _ := cand.Get(k)
		if it.State != ItemReady || it.SourceVersion != 2 {
			t.Fatalf("candidate %s = %+v, want ready v2 for comparison", k, it)
		}
	}
	if it, _ := cand.Get("d"); it.State != ItemFailed ||
		it.Value != "d-of-bc-2" || it.Err == nil ||
		!strings.Contains(it.Err.Error(), "c") {
		t.Fatalf("candidate d = %+v, want excluded (dep c failed) but value retained", it)
	}

	// The provisional v2 values never reached the confirmed cache: d
	// stays at its confirmed v1, nothing was promoted to v2.
	if got := c.SourceVersion("d"); got != 1 {
		t.Fatalf("confirmed d source version = %d, want 1 (failed candidate leaked v2?)", got)
	}
	if got := c.SourceVersion("a"); got == 2 {
		t.Fatal("provisional a@2 leaked into the confirmed cache")
	}

	// Repeated ordinary combined reads during the failure window must
	// all observe one stable, complete OLD view. The failure blocks
	// publication of any v2 combination, so readers fall back to
	// re-reading the sources and receive a failed view, never a mixed
	// view claiming completeness. Here the source for c is down, so a
	// fresh Snapshot must be incomplete with c explicitly failed.
	for i := 0; i < 3; i++ {
		r := c.Snapshot(ctx, "a", "b", "c", "d")
		if r.Complete() {
			var vals []string
			for _, it := range r.Items() {
				vals = append(vals, fmt.Sprintf("%s=%v(s%d,%s)", it.Key, it.Value,
					it.SourceVersion, stateName(it.State)))
			}
			t.Fatalf("read %d during failure window claims complete: %v", i, vals)
		}
		if it, _ := r.Get("c"); it.State != ItemFailed {
			t.Fatalf("read %d: c = %+v, want failed", i, it)
		}
		if it, _ := r.Get("d"); it.State == ItemReady && it.SourceVersion == 2 {
			t.Fatalf("read %d: new d-of-bc-2 is published while c failed: %+v", i, it)
		}
	}

	// The retained base view stays stable and readable throughout.
	for i := 0; i < 3; i++ {
		for _, k := range []string{"a", "b", "c", "d"} {
			it := mustItem(t, base, k)
			if it.State != ItemReady || it.SourceVersion != 1 {
				t.Fatalf("base view mutated at %s: %+v", k, it)
			}
		}
	}

	// Source recovers: only a fully satisfied combination is published.
	delete(fail, "c")
	cand2 := base.Refresh(ctx)
	if cand2.State() != CandidateReady {
		t.Fatalf("recovered candidate = %v (%s)", cand2.State(), cand2.Reason())
	}
	next, ok := cand2.Adopt()
	if !ok {
		t.Fatal("ready, dependency-consistent candidate refused")
	}
	if !next.Complete() {
		t.Fatalf("adopted view incomplete: %+v", next.Items())
	}
	// Reopening explains exactly which source versions were adopted:
	// every member at version 2, derived along the diamond from a@2.
	reopened := c.Snapshot(ctx, "a", "b", "c", "d")
	for _, k := range []string{"a", "b", "c", "d"} {
		n := mustItem(t, next, k)
		r := mustItem(t, reopened, k)
		o := mustItem(t, base, k)
		if n.SourceVersion != 2 || r.SourceVersion != 2 {
			t.Fatalf("%s adopted/reopened at v%d/%d, want 2/2", k, n.SourceVersion, r.SourceVersion)
		}
		if o.SourceVersion != 1 || n.SourceVersion == o.SourceVersion {
			t.Fatalf("%s cannot explain adopted version against old view: old=%d new=%d",
				k, o.SourceVersion, n.SourceVersion)
		}
	}
	if it := mustItem(t, reopened, "d"); it.Value != "d-of-bc-2" {
		t.Fatalf("reopened d = %v, want d-of-bc-2", it.Value)
	}
}

// Ordinary Snapshot reads are themselves bounded by the dependency
// consistency check: a ready member whose in-view dependency is present
// at a different source version is excluded from the complete view
// instead of the read silently mixing the two generations.
func TestSnapshotBoundaryRejectsMixedVersions(t *testing.T) {
	// Store b derived from a@1, then confirm a newer a alone. In real
	// usage this split is exactly what a bypassing partial publish used
	// to create; the combined read must still refuse to call it complete.
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		switch key {
		case "a":
			return SourceResult{Value: "a-1", Version: 1}, nil
		case "b":
			return SourceResult{Value: "b-of-a-1", Version: 1, Deps: []string{"a"}}, nil
		}
		return SourceResult{}, fmt.Errorf("unknown %s", key)
	}})
	ctx := context.Background()
	old := c.Snapshot(ctx, "a", "b")
	if !old.Complete() {
		t.Fatalf("initial view: %+v", old.Items())
	}
	// Confirm a newer a alone (a key outside any combined publish).
	c.Invalidate("a")
	// Swap the loader so only a moves to v2; b cannot reload yet.
	c.loader = func(ctx context.Context, key string) (SourceResult, error) {
		switch key {
		case "a":
			return SourceResult{Value: "a-2", Version: 2}, nil
		case "b":
			return SourceResult{}, fmt.Errorf("b source unavailable")
		}
		return SourceResult{}, fmt.Errorf("unknown %s", key)
	}
	if v, err := c.Get(ctx, "a"); err != nil || v != "a-2" {
		t.Fatalf("Get a = %v, %v", v, err)
	}
	// A combined read with b still deriving from a@1 must not be
	// complete: b reads fail, and no new b is published.
	v := c.Snapshot(ctx, "a", "b")
	if v.Complete() {
		t.Fatalf("mixed a@2 + b@1 view reported complete: %+v", v.Items())
	}
	if it, _ := v.Get("a"); it.State != ItemReady || it.SourceVersion != 2 {
		t.Fatalf("a should still be inspectable at v2: %+v", it)
	}
}

// Cross refresh and waiter-exit during recovery still behave as the
// documented candidate destinations, and nothing provisional leaks to
// ordinary reads in either case.
func TestDiamondCrossRefreshAndWaiterExitStayBounded(t *testing.T) {
	var aVersion atomic.Int64
	aVersion.Store(1)
	gate := make(chan struct{})
	var cLoads atomic.Int64
	c := New(Options{SourceLoader: func(ctx context.Context, key string) (SourceResult, error) {
		v := aVersion.Load() // capture the source generation at load start
		if key == "c" && cLoads.Add(1) >= 2 {
			<-gate
		}
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
		return SourceResult{}, fmt.Errorf("unknown %s", key)
	}})
	ctx := context.Background()
	base := c.Snapshot(ctx, "a", "b", "c", "d")

	aVersion.Store(2)
	c.Invalidate("a")
	done := make(chan *Candidate, 1)
	go func() { done <- base.Refresh(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for cLoads.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	aVersion.Store(3)
	c.Invalidate("a")
	close(gate)
	cand := <-done
	if cand.State() != CandidateAborted || !strings.Contains(cand.Reason(), "superseded") {
		t.Fatalf("candidate = %v (%q), want aborted superseded", cand.State(), cand.Reason())
	}
	if _, ok := cand.Adopt(); ok {
		t.Fatal("aborted candidate adopted")
	}
	// Nothing provisional leaked; reopening publishes only the full v3
	// diamond combination.
	latest := c.Snapshot(ctx, "a", "b", "c", "d")
	if !latest.Complete() {
		t.Fatalf("reopened view incomplete: %+v", latest.Items())
	}
	for _, k := range []string{"a", "b", "c", "d"} {
		if it := mustItem(t, latest, k); it.SourceVersion != 3 {
			t.Fatalf("%s = %+v, want source version 3", k, it)
		}
	}
}

func stateName(s ItemState) string {
	switch s {
	case ItemReady:
		return "ready"
	case ItemPending:
		return "pending"
	default:
		return "failed"
	}
}
