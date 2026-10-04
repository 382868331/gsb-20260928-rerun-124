package cache

import (
	"context"
	"sync/atomic"
	"testing"
)

// Diamond dependency (a -> b,c -> d) with the leaf c failing: the
// successfully loaded new root and branches must stay inside the
// unadopted candidate and never enter the public cache. Repeated
// ordinary combined reads in the failure window must observe the same
// dependency-consistency boundary as adoption: they never claim
// completeness and never stitch a new root onto old leaves.
func TestDiamondLeafFailureKeepsOneBoundary(t *testing.T) {
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
	c.Invalidate("a") // propagates to b, c and d
	fail["c"] = true

	cand := base.Refresh(ctx)
	if cand.State() != CandidateFailed {
		t.Fatalf("candidate = %v, want CandidateFailed (%s)",
			cand.State(), cand.Reason())
	}
	if v, ok := cand.Adopt(); ok || v != base {
		t.Fatal("failed candidate must not be adopted")
	}

	// The new values are inspectable for comparison; the failure is
	// explained per member.
	if it, _ := cand.Get("a"); it.State != ItemReady || it.SourceVersion != 2 || it.Value != "a-2" {
		t.Fatalf("candidate a = %+v, want comparable new value a-2/v2", it)
	}
	if it, _ := cand.Get("c"); it.State != ItemFailed || it.Err == nil {
		t.Fatalf("candidate c = %+v, want explicit failure", it)
	}

	// Nothing unadopted reached the public cache.
	if n := c.Len(); n != 0 {
		t.Fatalf("cache len = %d, want 0; unadopted values must not publish", n)
	}

	// Repeated ordinary reads in the failure window all obey the same
	// boundary: incomplete, and ready members are never mixed with old
	// leaves (no v1 root/branch next to v2 values).
	for i := 0; i < 3; i++ {
		v := c.Snapshot(ctx, "a", "b", "c", "d")
		if v.Complete() {
			t.Fatalf("read %d falsely claims completeness: %+v", i, v.Items())
		}
		if it := mustItem(t, v, "c"); it.State != ItemFailed {
			t.Fatalf("read %d: c = %+v, want failed", i, it)
		}
		for _, k := range []string{"a", "b", "d"} {
			it := mustItem(t, v, k)
			if it.State == ItemReady {
				if it.SourceVersion != 2 {
					t.Fatalf("read %d: %s is old %+v beside newer members; boundary broken", i, k, it)
				}
			}
		}
	}
	if n := c.Len(); n != 0 {
		t.Fatalf("cache len after failed reads = %d, want 0", n)
	}

	// The retained old view stays stable and readable.
	for _, k := range []string{"a", "b", "c", "d"} {
		if it := mustItem(t, base, k); it.State != ItemReady || it.SourceVersion != 1 {
			t.Fatalf("base view changed at %s: %+v", k, it)
		}
	}

	// Source recovers: the next combined read publishes the full new
	// combination atomically, and reopening a view explains exactly which
	// source versions were taken.
	delete(fail, "c")
	recovered := c.Snapshot(ctx, "a", "b", "c", "d")
	if !recovered.Complete() {
		t.Fatalf("recovered view incomplete: %+v", recovered.Items())
	}
	reopened := c.Snapshot(ctx, "a", "b", "c", "d")
	for _, k := range []string{"a", "b", "c", "d"} {
		got := mustItem(t, reopened, k)
		old := mustItem(t, base, k)
		if got.SourceVersion != 2 || old.SourceVersion != 1 {
			t.Fatalf("%s versions: recovered=%d base=%d, want 2 and 1",
				k, got.SourceVersion, old.SourceVersion)
		}
	}
	if it := mustItem(t, reopened, "d"); it.Value != "d-of-bc-2" {
		t.Fatalf("reopened d = %+v, want d-of-bc-2", it)
	}

	// Adopting the recovered candidate publishes the same versions.
	next, ok := base.Refresh(ctx).Adopt()
	if !ok {
		t.Fatal("recovered candidate adoption refused")
	}
	if it := mustItem(t, next, "d"); it.SourceVersion != 2 || it.Value != "d-of-bc-2" {
		t.Fatalf("adopted d = %+v", it)
	}
}
