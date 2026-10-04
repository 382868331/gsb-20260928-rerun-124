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
}

// Snapshot loads the given keys (sharing in-flight loads with other
// callers) and returns a retained combined view of them. Keys whose
// loads fail or are still pending when ctx ends appear in the view with
// their state and error, instead of being silently dropped or mixed
// with values from a different observation.
func (c *Cache) Snapshot(ctx context.Context, keys ...string) *View {
	order := dedupKeys(keys)
	calls := make(map[string]*loadCall, len(order))
	now := time.Now()
	c.mu.Lock()
	for _, k := range order {
		if e, ok := c.store.Get(k); ok && !e.expired(now) {
			continue // will be read from the store in the final lock
		}
		calls[k] = c.startLoadLocked(ctx, k)
	}
	c.mu.Unlock()

	errs := make(map[string]error, len(calls))
	pending := make(map[string]bool, len(calls))
	for k, call := range calls {
		select {
		case <-call.done:
			// Already finished: a completed load always outranks a
			// simultaneously expiring caller context.
			if call.err != nil {
				errs[k] = call.err
			}
			continue
		default:
		}
		select {
		case <-call.done:
			if call.err != nil {
				errs[k] = call.err
			}
		case <-ctx.Done():
			pending[k] = true
			errs[k] = ctx.Err()
		}
	}

	// Capture every member under one lock: the view is a single
	// observation instant, never a mix of pre- and post-refresh values.
	items := make(map[string]ViewItem, len(order))
	c.mu.Lock()
	for _, k := range order {
		if err, ok := errs[k]; ok {
			state := ItemFailed
			if pending[k] {
				state = ItemPending
			}
			items[k] = ViewItem{Key: k, State: state, Err: err}
			continue
		}
		if e, ok := c.store.Get(k); ok {
			items[k] = ViewItem{Key: k, State: ItemReady, Value: e.Value,
				SourceVersion: e.SourceVersion}
		} else {
			items[k] = ViewItem{Key: k, State: ItemFailed,
				Err: fmt.Errorf("entry unavailable at capture time")}
		}
	}
	c.mu.Unlock()
	return &View{c: c, order: order, items: items}
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
	base   *View
	view   *View
	deps   map[string][]string
	state  CandidateState
	reason string
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
	gens := make(map[string]uint64, len(keys))
	calls := make(map[string]*loadCall, len(keys))
	c.mu.Lock()
	for _, k := range keys {
		gens[k] = c.generations[k]
		calls[k] = c.startLoadLocked(ctx, k)
	}
	c.mu.Unlock()

	items := make(map[string]ViewItem, len(keys))
	deps := make(map[string][]string, len(keys))
	left := false
	for _, k := range keys {
		call := calls[k]
		if !left {
			select {
			case <-call.done:
				goto collected
			default:
			}
			select {
			case <-call.done:
			case <-ctx.Done():
				left = true
			}
		collected:
		}
		switch {
		case left:
			items[k] = ViewItem{Key: k, State: ItemPending, Err: ctx.Err()}
		case call.err != nil:
			items[k] = ViewItem{Key: k, State: ItemFailed, Err: call.err}
		default:
			items[k] = ViewItem{Key: k, State: ItemReady,
				Value: call.result.Value, SourceVersion: call.result.Version}
			deps[k] = call.result.Deps
		}
	}

	cand := &Candidate{base: v, deps: deps,
		view: &View{c: c, order: append([]string(nil), keys...), items: items}}
	if left {
		cand.state = CandidateAborted
		cand.reason = "waiter-left: " + ctx.Err().Error()
		return cand
	}
	failed := false
	for _, k := range keys {
		if items[k].State != ItemReady {
			failed = true
		}
	}
	// A source that changed again during the candidate's loads (local
	// generation bumped, or a newer source version already known) makes
	// the candidate stale before it was ever delivered: abort it.
	c.mu.Lock()
	superseded := false
	for _, k := range keys {
		if items[k].State != ItemReady {
			continue
		}
		if c.generations[k] != gens[k] || c.sourceVersions[k] != items[k].SourceVersion {
			superseded = true
		}
	}
	c.mu.Unlock()
	switch {
	case superseded:
		cand.state = CandidateAborted
		cand.reason = "superseded: a source changed again while the candidate was loading"
	case failed:
		cand.state = CandidateFailed
		cand.reason = "some sources failed; base view retained"
	default:
		cand.state = CandidateReady
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

// depsSatisfied reports whether every member is ready and every
// dependency edge that lands inside the view points at a ready member.
func (cand *Candidate) depsSatisfied() bool {
	for _, k := range cand.view.order {
		if cand.view.items[k].State != ItemReady {
			return false
		}
		for _, d := range cand.deps[k] {
			if di, ok := cand.view.items[d]; ok && di.State != ItemReady {
				return false
			}
		}
	}
	return true
}

// Adopt formally takes the candidate as the new view. It succeeds only
// for a CandidateReady candidate whose in-view dependencies are all
// satisfied; otherwise the base view is returned unchanged and ok is
// false.
func (cand *Candidate) Adopt() (view *View, ok bool) {
	if cand.state != CandidateReady || !cand.depsSatisfied() {
		return cand.base, false
	}
	return cand.view, true
}

// Abandon explicitly discards a candidate the caller decided not to
// adopt. Already-adopted or aborted candidates are unaffected.
func (cand *Candidate) Abandon() {
	if cand.state != CandidateAborted {
		cand.state = CandidateAborted
		cand.reason = "abandoned by caller"
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
