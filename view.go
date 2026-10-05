package cache

import (
	"context"
	"fmt"
	"time"
)

// ItemState describes whether one member of a View could be observed.
type ItemState int

const (
	// ItemReady: the value was captured in the view.
	ItemReady ItemState = iota
	// ItemPending: the load had not finished when the caller stopped
	// waiting (e.g. the caller's context ended).
	ItemPending
	// ItemFailed: the load finished with an error, or the entry was
	// unavailable at capture time.
	ItemFailed
)

// ViewItem is one key's slice of a View: the observed value, the source
// business version it was taken from, and why it is missing when the
// state is not ItemReady.
type ViewItem struct {
	Key   string
	State ItemState
	Value any
	// SourceVersion is the source business version of the captured
	// value, so two views can explain how they differ per key.
	SourceVersion uint64
	Err           error
}

// View is a retained, immutable combined read over several dependent
// keys. All values were captured under a single cache lock, so the view
// is one consistent observation: it never mixes values from before and
// after a refresh, and later refreshes never mutate an already
// delivered view.
type View struct {
	c     *Cache
	order []string
	items map[string]ViewItem
	// deps records the dependency edges each ready item was captured
	// with, so refreshes can re-check the same boundary.
	deps map[string][]string
}

// Snapshot loads the given keys (sharing in-flight loads with other
// callers) and returns a retained combined view of them. Keys whose
// loads fail or are still pending when ctx ends appear in the view with
// their state and error, instead of being silently dropped or mixed
// with values from a different observation.
func (c *Cache) Snapshot(ctx context.Context, keys ...string) *View {
	order := dedupKeys(keys)
	calls := make(map[string]*loadCall)
	stored := make(map[string]bool)
	now := time.Now()
	c.mu.Lock()
	for _, k := range order {
		if e, ok := c.store.Get(k); ok && !e.expired(now) {
			stored[k] = true // will be read from the confirmed store later
			continue
		}
		// Unconfirmed loads run as probes: their results must not enter
		// the public store unless the whole combined set passes the same
		// adoption check a candidate goes through.
		calls[k] = c.startProbeLocked(ctx, k)
	}
	c.mu.Unlock()

	left := waitOnCalls(ctx, calls)

	items := make(map[string]ViewItem, len(order))
	deps := make(map[string][]string, len(order))
	pending := make(map[string]pendingResult)
	gens := make(map[string]uint64, len(order))
	superseded := false
	c.mu.Lock()
	for _, k := range order {
		switch {
		case stored[k]:
			// Re-read the confirmed value at the capture instant.
			if e, ok := c.store.Get(k); ok {
				items[k] = ViewItem{Key: k, State: ItemReady, Value: e.Value,
					SourceVersion: e.SourceVersion}
				deps[k] = e.Deps
				gens[k] = e.Version
			} else {
				items[k] = ViewItem{Key: k, State: ItemFailed,
					Err: fmt.Errorf("entry unavailable at capture time")}
			}
		default:
			call := calls[k]
			switch {
			case left[k]:
				items[k] = ViewItem{Key: k, State: ItemPending, Err: ctx.Err()}
			case call.err != nil:
				items[k] = ViewItem{Key: k, State: ItemFailed, Err: call.err}
			default:
				if c.generations[k] != call.gen {
					// Invalidated again while loading: never assemble it
					// into a view as if it were current.
					superseded = true
					items[k] = ViewItem{Key: k, State: ItemFailed,
						Err: fmt.Errorf("entry invalidated while loading")}
					continue
				}
				res := call.result
				if c.sourceVersions[k] == res.Version {
					// An identical-version result was confirmed meanwhile;
					// read the confirmed entry instead of publishing twice.
					if e, ok := c.store.Get(k); ok {
						items[k] = ViewItem{Key: k, State: ItemReady, Value: e.Value,
							SourceVersion: e.SourceVersion}
						deps[k] = e.Deps
						gens[k] = e.Version
						continue
					}
				}
				if res.Version < c.sourceVersions[k] {
					superseded = true
					items[k] = ViewItem{Key: k, State: ItemFailed,
						Err: fmt.Errorf("older source version %d while %d known",
							res.Version, c.sourceVersions[k])}
					continue
				}
				items[k] = ViewItem{Key: k, State: ItemReady,
					Value: res.Value, SourceVersion: res.Version}
				deps[k] = res.Deps
				gens[k] = call.gen
				pending[k] = pendingResult{res: res, gen: call.gen}
			}
		}
	}
	// Publish only when the entire set crosses the same boundary a
	// candidate adoption requires. A partial failure leaves every new
	// value in the view object only; the public store keeps the old view.
	setFailed := false
	for _, it := range items {
		if it.State == ItemFailed || it.State == ItemPending {
			setFailed = true
		}
	}
	var events []Event
	var evicted []struct {
		key   string
		entry Entry
	}
	if !superseded && !setFailed && boundarySatisfied(order, items, deps) && len(pending) > 0 {
		events, evicted = c.publishResultsLocked(pending)
	}
	c.mu.Unlock()
	c.emit(events)
	if len(events) > 0 {
		// The combined set crossed the boundary and was published:
		// record the confirmed view durably before anyone can restart
		// and wonder which combination was last public.
		c.appendRecord(RecordConfirmed, "snapshot", order, items, deps, gens)
	}
	for _, ev := range evicted {
		if c.onEvict != nil {
			c.onEvict(ev.key, ev.entry)
		}
	}
	return &View{c: c, order: append([]string(nil), order...), items: items, deps: deps}
}

// waitOnCalls waits on the shared probe loads and reports the keys whose
// waiter context ended before the result arrived. A result that is
// already closed always outranks a simultaneously expiring context.
func waitOnCalls(ctx context.Context, calls map[string]*loadCall) map[string]bool {
	left := make(map[string]bool)
	for k, call := range calls {
		select {
		case <-call.done:
			continue
		default:
		}
		select {
		case <-call.done:
		case <-ctx.Done():
			left[k] = true
		}
	}
	return left
}

// boundarySatisfied is the single dependency-consistency rule shared by
// every read entry point: every member must be ready, and every
// dependency edge that lands inside the set must point at a ready
// member. External dependencies are outside this combined observation.
func boundarySatisfied(order []string, items map[string]ViewItem, deps map[string][]string) bool {
	for _, k := range order {
		if items[k].State != ItemReady {
			return false
		}
	}
	for k, ds := range deps {
		if items[k].State != ItemReady {
			continue
		}
		for _, d := range ds {
			if di, ok := items[d]; ok && di.State != ItemReady {
				return false
			}
		}
	}
	return true
}

// Get returns one member of the view. Repeated reads of the same view
// always return the same item, no matter what the cache does later.
func (v *View) Get(key string) (ViewItem, bool) {
	it, ok := v.items[key]
	return it, ok
}

// Items returns all members of the view in the order the keys were
// requested.
func (v *View) Items() []ViewItem {
	out := make([]ViewItem, 0, len(v.order))
	for _, k := range v.order {
		out = append(out, v.items[k])
	}
	return out
}

// Keys returns the view's keys in request order.
func (v *View) Keys() []string { return append([]string(nil), v.order...) }

// Complete reports whether every member of the view is ready.
func (v *View) Complete() bool {
	for _, it := range v.items {
		if it.State != ItemReady {
			return false
		}
	}
	return true
}

// CandidateState describes where a proposed refresh of a View stands.
type CandidateState int

const (
	// CandidateReady: every member loaded and still matches the cache;
	// the candidate can be adopted.
	CandidateReady CandidateState = iota
	// CandidateFailed: some sources failed. The base view is retained;
	// the partial changes stay inspectable but cannot be adopted.
	CandidateFailed
	// CandidateAborted: the waiter left or a source changed again while
	// the candidate was loading, or the caller abandoned it. The
	// candidate is discarded and holds nothing open.
	CandidateAborted
)

// Candidate is a proposed replacement for a View. It is inspectable at
// any time, but only becomes the delivered view through Adopt, and only
// when every member loaded and all in-view dependencies are satisfied.
// A candidate is pure data: it never pins loads, locks or goroutines,
// so a discarded candidate leaves nothing behind.
type Candidate struct {
	base    *View
	view    *View
	deps    map[string][]string
	gens    map[string]uint64
	pending map[string]pendingResult
	state   CandidateState
	reason  string
}

// Refresh reloads every member of the view and returns the resulting
// candidate. The base view is never mutated. If any source fails, the
// candidate is CandidateFailed: the partial changes remain inspectable
// but Adopt keeps the base view. If the caller's context ends while
// loads are in flight, or any source changed again before the candidate
// was assembled, the candidate is CandidateAborted with the reason
// recorded; the underlying shared loads still finish for other waiters.
func (v *View) Refresh(ctx context.Context) *Candidate {
	c := v.c
	keys := v.order
	calls := make(map[string]*loadCall, len(keys))
	c.mu.Lock()
	for _, k := range keys {
		calls[k] = c.startProbeLocked(ctx, k)
	}
	c.mu.Unlock()

	left := waitOnCalls(ctx, calls)

	items := make(map[string]ViewItem, len(keys))
	deps := make(map[string][]string, len(keys))
	gens := make(map[string]uint64, len(keys))
	pending := make(map[string]pendingResult, len(keys))
	failed := false
	if len(left) > 0 {
		for _, k := range keys {
			if left[k] {
				items[k] = ViewItem{Key: k, State: ItemPending, Err: ctx.Err()}
				continue
			}
			// Earlier members may still have completed; represent any
			// uninspected member honestly as pending too, since the set
			// was never assembled.
			call := calls[k]
			select {
			case <-call.done:
				if call.err != nil {
					items[k] = ViewItem{Key: k, State: ItemFailed, Err: call.err}
				} else {
					items[k] = ViewItem{Key: k, State: ItemReady,
						Value: call.result.Value, SourceVersion: call.result.Version}
					deps[k] = call.result.Deps
					gens[k] = call.gen
					pending[k] = pendingResult{res: call.result, gen: call.gen}
				}
			default:
				items[k] = ViewItem{Key: k, State: ItemPending, Err: ctx.Err()}
			}
		}
		cand := &Candidate{
			base: v,
			view: &View{c: c, order: append([]string(nil), keys...), items: items},
			deps: deps, gens: gens, pending: pending,
			state:  CandidateAborted,
			reason: "waiter-left: " + ctx.Err().Error(),
		}
		c.appendRecord(RecordCandidate, cand.reason, keys, items, deps, gens)
		return cand
	}

	c.mu.Lock()
	superseded := false
	for _, k := range keys {
		call := calls[k]
		gens[k] = call.gen
		switch {
		case call.err != nil:
			items[k] = ViewItem{Key: k, State: ItemFailed, Err: call.err}
			failed = true
		default:
			res := call.result
			if c.generations[k] != call.gen || res.Version < c.sourceVersions[k] {
				// A source that changed again during the candidate's
				// loads makes the candidate stale before delivery.
				superseded = true
				items[k] = ViewItem{Key: k, State: ItemFailed, Err: fmt.Errorf(
					"source changed again while the candidate was loading")}
				continue
			}
			items[k] = ViewItem{Key: k, State: ItemReady,
				Value: res.Value, SourceVersion: res.Version}
			deps[k] = res.Deps
			pending[k] = pendingResult{res: res, gen: call.gen}
		}
	}
	c.mu.Unlock()

	cand := &Candidate{
		base: v,
		view: &View{c: c, order: append([]string(nil), keys...), items: items},
		deps: deps, gens: gens, pending: pending,
	}
	switch {
	case superseded:
		cand.state = CandidateAborted
		cand.reason = "superseded: a source changed again while the candidate was loading"
	case failed:
		cand.state = CandidateFailed
		cand.reason = "some sources failed; base view retained"
	case !boundarySatisfied(keys, items, deps):
		// A ready member whose in-view dependency is not ready can never
		// be a consistent combination: keep it inspectable, not adoptable.
		cand.state = CandidateFailed
		cand.reason = "in-view dependencies not all satisfied; base view retained"
	default:
		cand.state = CandidateReady
	}
	if cand.state != CandidateReady {
		// Failed and aborted candidates are persisted for audit: their
		// would-be values stay inspectable after a restart but are
		// never restored into the public store.
		c.appendRecord(RecordCandidate, cand.reason, keys, items, deps, gens)
	}
	return cand
}

// State reports where the candidate stands.
func (cand *Candidate) State() CandidateState { return cand.state }

// Reason explains why a failed or aborted candidate cannot be adopted.
func (cand *Candidate) Reason() string { return cand.reason }

// Base returns the view the candidate was refreshing from.
func (cand *Candidate) Base() *View { return cand.base }

// Items returns the candidate's proposed members, including failed or
// pending ones, so callers can inspect the would-be changes.
func (cand *Candidate) Items() []ViewItem { return cand.view.Items() }

// Get returns one proposed member of the candidate.
func (cand *Candidate) Get(key string) (ViewItem, bool) { return cand.view.Get(key) }

// Adopt formally takes the candidate as the new view. It succeeds only
// for a CandidateReady candidate whose members still match the cache
// (generations and source versions re-checked under the lock) and whose
// in-view dependencies are all satisfied. The members are then published
// to the public store as one atomic set; otherwise the base view is
// returned unchanged, ok is false, and no candidate value is committed.
func (cand *Candidate) Adopt() (view *View, ok bool) {
	if cand.state != CandidateReady {
		return cand.base, false
	}
	c := cand.base.c
	c.mu.Lock()
	for _, k := range cand.view.order {
		it := cand.view.items[k]
		if it.State != ItemReady {
			c.mu.Unlock()
			return cand.base, false
		}
		if c.generations[k] != cand.gens[k] {
			c.mu.Unlock()
			cand.state = CandidateAborted
			cand.reason = "superseded: a source changed again before adoption"
			c.appendRecord(RecordCandidate, cand.reason, cand.view.order,
				cand.view.items, cand.deps, cand.gens)
			return cand.base, false
		}
		if known := c.sourceVersions[k]; known > it.SourceVersion {
			c.mu.Unlock()
			cand.state = CandidateAborted
			cand.reason = "superseded: a newer source version was confirmed before adoption"
			c.appendRecord(RecordCandidate, cand.reason, cand.view.order,
				cand.view.items, cand.deps, cand.gens)
			return cand.base, false
		}
	}
	if !boundarySatisfied(cand.view.order, cand.view.items, cand.deps) {
		c.mu.Unlock()
		return cand.base, false
	}
	events, evicted := c.publishResultsLocked(cand.pending)
	cand.view.deps = cand.deps
	c.mu.Unlock()
	c.emit(events)
	c.appendRecord(RecordConfirmed, "adopt", cand.view.order, cand.view.items, cand.deps, cand.gens)
	for _, ev := range evicted {
		if c.onEvict != nil {
			c.onEvict(ev.key, ev.entry)
		}
	}
	return cand.view, true
}

// Abandon explicitly discards a candidate the caller decided not to
// adopt. Already-adopted or aborted candidates are unaffected.
func (cand *Candidate) Abandon() {
	if cand.state != CandidateAborted {
		cand.state = CandidateAborted
		cand.reason = "abandoned by caller"
		cand.base.c.appendRecord(RecordCandidate, cand.reason, cand.view.order,
			cand.view.items, cand.deps, cand.gens)
	}
}

func dedupKeys(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
}
