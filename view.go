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
	deps  map[string][]string
}

// provisional holds one member's probe result while a combined read is
// being assembled. Provisional values are not in the store: they only
// become visible to other readers if the whole group is consistent and
// is committed together.
type provisional struct {
	res SourceResult
	err error
	gen uint64
	// pending is set when the caller stopped waiting before the probe
	// finished. The detached probe still completes later, but its
	// result can never join this read.
	pending bool
}

// markInconsistent propagates the dependency-consistency boundary over
// a set of provisional or stored items. It works on abstracted data so
// that every read entry point enforces the same rule.
//
// A member is excluded from a complete view when it is not ready, or
// when a dependency edge landing inside the set points at a member that
// is either excluded or observed at a different source version. The
// check is iterated to a fixed point so a failed leaf also excludes its
// parents (including through a diamond). Deps outside the set are
// external inputs and do not break the group's internal consistency.
func markInconsistent(order []string, depsOf func(string) []string,
	versionOf func(string) (uint64, bool), ready func(string) bool) map[string]string {
	reason := make(map[string]string)
	bad := func(k string) bool { _, no := reason[k]; return no }
	changed := true
	for changed {
		changed = false
		for _, k := range order {
			if bad(k) {
				continue
			}
			if !ready(k) {
				reason[k] = "member not ready"
				changed = true
				continue
			}
			ver, _ := versionOf(k)
			for _, d := range depsOf(k) {
				if _, inSet := versionOf(d); !inSet {
					continue // external dependency, not part of the set
				}
				if bad(d) {
					reason[k] = "dependency " + d + " is not consistently available"
					changed = true
					break
				}
				dver, _ := versionOf(d)
				if dver != ver {
					reason[k] = fmt.Sprintf("dependency %s at source version %d does not match %s at %d",
						d, dver, k, ver)
					changed = true
					break
				}
			}
		}
	}
	return reason
}

// Snapshot returns a retained combined view of the given keys. If the
// confirmed cache already holds one internally consistent observation
// of the whole set it is captured directly; otherwise every member is
// re-read through a probe. Keys whose loads fail or are still pending
// when ctx ends appear in the view with their state and error, instead
// of being silently dropped or mixed with values from a different
// observation. Probe values are provisional: the whole group is only
// published if every in-view dependency is ready at one matching source
// version, so a partially failed refresh can never leave new roots and
// branches next to old leaves in the shared cache.
func (c *Cache) Snapshot(ctx context.Context, keys ...string) *View {
	order := dedupKeys(keys)
	now := time.Now()
	c.mu.Lock()
	// If the confirmed store already holds one internally consistent
	// view of the whole set, capture it directly without touching the
	// sources: a retained view is the stable fallback during failure
	// windows.
	if items, deps, consistent := c.consistentStoreViewLocked(order, now); consistent {
		for _, k := range order {
			c.touchLocked(k)
		}
		c.mu.Unlock()
		return &View{c: c, order: order, items: items, deps: deps}
	}
	gens := make(map[string]uint64, len(order))
	calls := make(map[string]*loadCall, len(order))
	for _, k := range order {
		gens[k] = c.generations[k]
		calls[k] = c.startProbeLocked(ctx, k)
	}
	c.mu.Unlock()

	prov := make(map[string]*provisional, len(order))
	left := false
	for _, k := range order {
		call := calls[k]
		select {
		case <-call.done:
			// Already finished: a completed load always outranks a
			// simultaneously expiring caller context.
		default:
			if left {
				prov[k] = &provisional{gen: gens[k], pending: true, err: ctx.Err()}
				continue
			}
		}
		if left {
			select {
			case <-call.done:
			default:
				prov[k] = &provisional{gen: gens[k], pending: true, err: ctx.Err()}
				continue
			}
		}
		select {
		case <-call.done:
			prov[k] = &provisional{res: call.result, err: call.err, gen: gens[k]}
		case <-ctx.Done():
			left = true
			prov[k] = &provisional{gen: gens[k], pending: true, err: ctx.Err()}
		}
	}

	view := c.assembleProvisional(order, gens, prov, true)
	return view
}

// consistentStoreViewLocked assembles a view purely from confirmed
// store entries and applies the same dependency-consistency boundary as
// candidate adoption. It returns consistent=false if any member is
// missing, expired or connected to an in-view member at a mismatching
// source version.
func (c *Cache) consistentStoreViewLocked(order []string, now time.Time) (map[string]ViewItem,
	map[string][]string, bool) {
	entries := make(map[string]Entry, len(order))
	for _, k := range order {
		e, ok := c.store.Get(k)
		if !ok || e.expired(now) {
			return nil, nil, false
		}
		entries[k] = e
	}
	depsOf := func(k string) []string { return entries[k].Deps }
	versionOf := func(k string) (uint64, bool) {
		e, ok := entries[k]
		return e.SourceVersion, ok
	}
	reason := markInconsistent(order, depsOf, versionOf, func(string) bool { return true })
	if len(reason) != 0 {
		return nil, nil, false
	}
	items := make(map[string]ViewItem, len(order))
	deps := make(map[string][]string, len(order))
	for _, k := range order {
		e := entries[k]
		items[k] = ViewItem{Key: k, State: ItemReady, Value: e.Value,
			SourceVersion: e.SourceVersion}
		deps[k] = e.Deps
	}
	return items, deps, true
}

// assembleProvisional builds the view from probe results, enforcing the
// dependency boundary and publishing the whole group atomically only
// when it is complete and consistent. Failed or pending members, and
// members excluded by the boundary, stay inspectable in the view but
// are never written to the store; the previously confirmed view stays
// intact and can be captured by a later read.
func (c *Cache) assembleProvisional(order []string, gens map[string]uint64,
	prov map[string]*provisional, commit bool) *View {
	items := make(map[string]ViewItem, len(order))
	deps := make(map[string][]string, len(order))
	for _, k := range order {
		p := prov[k]
		switch {
		case p == nil || p.pending:
			err := ctxErrUnavailable
			if p != nil && p.err != nil {
				err = p.err
			}
			items[k] = ViewItem{Key: k, State: ItemPending, Err: err}
		case p.err != nil:
			items[k] = ViewItem{Key: k, State: ItemFailed, Err: p.err}
		default:
			items[k] = ViewItem{Key: k, State: ItemReady,
				Value: p.res.Value, SourceVersion: p.res.Version}
			deps[k] = p.res.Deps
		}
	}

	versionOf := func(k string) (uint64, bool) {
		it, ok := items[k]
		return it.SourceVersion, ok
	}
	excluded := markInconsistent(order, func(k string) []string { return deps[k] },
		versionOf, func(k string) bool { return items[k].State == ItemReady })
	complete := len(excluded) == 0
	for k, why := range excluded {
		it := items[k]
		if it.State == ItemReady {
			// The probe produced a value, but adopting it would mix
			// versions across a dependency edge; keep it visible for
			// comparison but do not let it claim completeness.
			it.State = ItemFailed
			it.Err = fmt.Errorf("inconsistent combined read: %s", why)
			items[k] = it
		}
	}

	var events []Event
	var evicted []evictedEntry
	if complete && commit {
		results := make(map[string]SourceResult, len(order))
		for _, k := range order {
			results[k] = prov[k].res
		}
		c.mu.Lock()
		ok, commitReason, ev, evi := c.commitGroupLocked(results, gens)
		c.mu.Unlock()
		if ok {
			events, evicted = ev, evi
		} else {
			// The sources moved while the probes ran: do not publish
			// anything, and mark every provisional member as failed so
			// the read cannot masquerade as a complete observation.
			for _, k := range order {
				it := items[k]
				if it.State == ItemReady {
					it.State = ItemFailed
					it.Err = fmt.Errorf("inconsistent combined read: %s", commitReason)
					items[k] = it
				}
			}
		}
	}
	c.emit(events)
	for _, ev := range evicted {
		if c.onEvict != nil {
			c.onEvict(ev.key, ev.entry)
		}
	}
	return &View{c: c, order: order, items: items, deps: deps}
}

var ctxErrUnavailable = fmt.Errorf("load had not finished when the caller stopped waiting")

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
	results map[string]SourceResult
	state   CandidateState
	reason  string
}

// Refresh reloads every member of the view and returns the resulting
// candidate. Every load is a probe: no loaded value is published while
// the candidate is merely inspected, so an unadopted candidate can
// never change what ordinary combined reads observe. The base view is
// never mutated. If a source fails or the in-view dependency boundary
// cannot be satisfied, the candidate is CandidateFailed: the partial
// changes stay inspectable but Adopt keeps the base view. If the
// caller's context ends while loads are in flight, the candidate is
// CandidateAborted with the reason recorded. A source changing again
// before adoption is detected by Adopt's locked re-verification and
// aborts the candidate then.
func (v *View) Refresh(ctx context.Context) *Candidate {
	c := v.c
	keys := v.order
	gens := make(map[string]uint64, len(keys))
	c.mu.Lock()
	calls := make(map[string]*loadCall, len(keys))
	for _, k := range keys {
		gens[k] = c.generations[k]
		calls[k] = c.startProbeLocked(ctx, k)
	}
	c.mu.Unlock()

	prov := make(map[string]*provisional, len(keys))
	left := false
	for _, k := range keys {
		call := calls[k]
		if !left {
			select {
			case <-call.done:
			default:
				select {
				case <-call.done:
				case <-ctx.Done():
					left = true
				}
			}
		}
		if left {
			select {
			case <-call.done:
				prov[k] = &provisional{res: call.result, err: call.err, gen: gens[k]}
			default:
				prov[k] = &provisional{gen: gens[k], pending: true, err: ctx.Err()}
			}
		} else {
			prov[k] = &provisional{res: call.result, err: call.err, gen: gens[k]}
		}
	}

	if left {
		return &Candidate{base: v,
			view: &View{c: c, order: append([]string(nil), keys...),
				items: provisionalItems(keys, prov)},
			gens:   gens,
			state:  CandidateAborted,
			reason: "waiter-left: " + ctx.Err().Error()}
	}

	// Assemble under the same dependency boundary every read enforces,
	// publishing nothing.
	view := c.assembleProvisional(keys, gens, prov, false)
	results := make(map[string]SourceResult, len(keys))
	for _, k := range keys {
		results[k] = prov[k].res
	}
	cand := &Candidate{base: v, view: view, deps: view.deps, gens: gens,
		results: results}
	sourceFailed := false
	for _, k := range keys {
		if prov[k].err != nil {
			sourceFailed = true
		}
	}
	if !cand.depsSatisfied() {
		cand.state = CandidateFailed
		if sourceFailed {
			cand.reason = "some sources failed; base view retained"
		} else {
			cand.reason = "in-view dependency boundary not satisfied; base view retained"
		}
		return cand
	}
	// The probes produced one consistent group, but a source may have
	// changed again while they were in flight: another invalidation, or
	// a newer source version already known through another read, makes
	// the candidate stale before it is even offered for adoption.
	c.mu.Lock()
	superseded := ""
	for _, k := range keys {
		if c.generations[k] != gens[k] {
			superseded = "superseded: key " + k +
				" was invalidated again while the candidate was loading"
			break
		}
		if view.items[k].SourceVersion < c.sourceVersions[k] {
			superseded = "superseded: a newer source version became known while the candidate was loading"
			break
		}
	}
	c.mu.Unlock()
	if superseded != "" {
		cand.state = CandidateAborted
		cand.reason = superseded
	} else {
		cand.state = CandidateReady
	}
	return cand
}

func provisionalItems(keys []string, prov map[string]*provisional) map[string]ViewItem {
	items := make(map[string]ViewItem, len(keys))
	for _, k := range keys {
		p := prov[k]
		switch {
		case p == nil || p.pending:
			err := ctxErrUnavailable
			if p != nil && p.err != nil {
				err = p.err
			}
			items[k] = ViewItem{Key: k, State: ItemPending, Err: err}
		case p.err != nil:
			items[k] = ViewItem{Key: k, State: ItemFailed, Err: p.err}
		default:
			items[k] = ViewItem{Key: k, State: ItemReady,
				Value: p.res.Value, SourceVersion: p.res.Version}
		}
	}
	return items
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

// depsSatisfied reports whether the assembled candidate is one complete,
// internally consistent combined read. The consistency marking done at
// assembly time already enforces ready members and matching source
// versions across every in-view dependency edge.
func (cand *Candidate) depsSatisfied() bool {
	return cand.view.Complete()
}

// Adopt formally takes the candidate as the new view. It succeeds only
// for a CandidateReady candidate, and re-verifies under the cache lock
// that no member was invalidated or overtaken by a newer known source
// version while the candidate was being inspected. Only then is the
// whole group published atomically; otherwise the candidate is aborted
// with the reason recorded and the base (confirmed) view is returned
// unchanged. Adoption is the single path through which provisional
// values become visible, so no read entry point can bypass the
// dependency-consistency boundary.
func (cand *Candidate) Adopt() (view *View, ok bool) {
	if cand.state != CandidateReady || !cand.depsSatisfied() {
		return cand.base, false
	}
	c := cand.base.c
	c.mu.Lock()
	committed, reason, events, evicted := c.commitGroupLocked(cand.results, cand.gens)
	c.mu.Unlock()
	if !committed {
		cand.state = CandidateAborted
		cand.reason = reason
		return cand.base, false
	}
	c.emit(events)
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
