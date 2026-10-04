package cache

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

// Loader fetches the value for a key from the authoritative source.
type Loader func(ctx context.Context, key string) (any, error)

// SourceResult is a loader result carrying the source's own business
// version and the other keys this value depends on.
type SourceResult struct {
	Value any
	// Version is the business version assigned by the source. It is
	// unrelated to the cache's local invalidation generation: a result
	// with a lower source Version than an already-known one is stale,
	// no matter when its load started or finished.
	Version uint64
	// Deps lists keys this value was derived from. Invalidating any of
	// them also invalidates this entry.
	Deps []string
	// DepVersions optionally records the exact source version of each
	// dependency the value was derived from. A composite view uses it to
	// reject a mixture of values taken from different source snapshots.
	DepVersions map[string]uint64
}

// SourceLoader fetches a value together with its source version and
// cross-key dependencies.
type SourceLoader func(ctx context.Context, key string) (SourceResult, error)

// Entry is a stored value together with its version and expiry.
// A zero ExpiresAt means the entry never expires.
type Entry struct {
	Value any
	// Version is the local invalidation generation at store time. It
	// only counts local Invalidate calls and must never be compared
	// against source versions.
	Version uint64
	// SourceVersion is the business version reported by the source.
	SourceVersion uint64
	Deps          []string
	DepVersions   map[string]uint64
	// CommitID identifies the atomic composite refresh that stored the
	// entry. Entries loaded individually have a zero CommitID.
	CommitID  uint64
	ExpiresAt time.Time
}

func (e Entry) expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt)
}

// Stats reports observable cache behaviour.
type Stats struct {
	Hits       uint64
	StaleHits  uint64
	Misses     uint64
	Loads      uint64 // loader invocations actually started
	Evictions  uint64
	StaleDrops uint64 // load results dropped because the key was invalidated
	// OldSourceDrops counts load results dropped because they carried an
	// older source version than a result the cache already knows.
	OldSourceDrops uint64
}

// EventOp identifies what the cache did with a key.
type EventOp int

const (
	// EventStored: a load result became the stored entry.
	EventStored EventOp = iota
	// EventDropped: a load result was discarded instead of stored.
	EventDropped
	// EventInvalidated: an entry was removed by invalidation (possibly
	// propagated through a dependency edge).
	EventInvalidated
)

// Event records why the cache stored, dropped or invalidated something.
type Event struct {
	Key    string
	Op     EventOp
	Reason string
	// SourceVersion is the version carried by the involved load result
	// (zero for invalidations).
	SourceVersion uint64
}

// ViewItemStatus describes where one part of a composite view came from.
type ViewItemStatus string

const (
	// ViewCommitted marks a value from the currently committed cache view.
	ViewCommitted ViewItemStatus = "committed"
	// ViewCandidate marks a value that is available in a refresh candidate
	// but has not been committed because the whole dependency closure is
	// not yet consistent.
	ViewCandidate ViewItemStatus = "candidate"
	// ViewPending marks a part whose source load is still running.
	ViewPending ViewItemStatus = "pending"
	// ViewUnavailable marks a part that could not be obtained.
	ViewUnavailable ViewItemStatus = "unavailable"
)

// ViewItem is one key's place in an immutable composite read.
type ViewItem struct {
	Key           string
	Value         any
	SourceVersion uint64
	Deps          []string
	DepVersions   map[string]uint64
	CommitID      uint64
	Status        ViewItemStatus
	Error         error
}

// View is an immutable, retainable composite read. Later refreshes create
// new View values; they never mutate a view already handed to a caller.
type View struct {
	cache    *Cache
	rootKeys []string

	ID       uint64
	CommitID uint64
	Complete bool
	// Items contains committed values, candidate values and failed/pending
	// placeholders. Its iteration order is not meaningful; use Order.
	Items map[string]ViewItem
	Order []string
	// Candidates is populated only for a refresh that could not commit. The
	// retained Items stay on the old baseline; Candidates lets callers see
	// the parts that did become available.
	Candidates map[string]ViewItem
	// CandidateChanges lists keys whose candidate version differs from the
	// committed baseline that the refresh started from.
	CandidateChanges []string
}

// Get returns the value selected by this immutable view.
func (v *View) Get(key string) (any, bool) {
	item, ok := v.Items[key]
	if !ok || item.Status == ViewPending || item.Status == ViewUnavailable {
		return nil, false
	}
	return item.Value, true
}

// SourceVersion reports the source version used by this view for key.
func (v *View) SourceVersion(key string) (uint64, bool) {
	item, ok := v.Items[key]
	return item.SourceVersion, ok
}

// Read returns the same retained view. It exists so callers can express a
// repeated read without accidentally starting a refresh.
func (v *View) Read() *View { return v }

// Refresh starts a new candidate refresh without changing v.
func (v *View) Refresh(ctx context.Context) (*View, error) {
	return v.cache.refreshView(ctx, v.rootKeys)
}

// Options configures a Cache.
type Options struct {
	Loader Loader
	// SourceLoader, when set, takes precedence over Loader and supplies
	// source business versions and dependency edges.
	SourceLoader SourceLoader
	Store        Store // nil uses NewMapStore()
	Capacity     int   // max entries; <=0 means unbounded
	TTL          time.Duration
	// AllowStale serves an expired entry while a refresh runs in the
	// background. Without it, expired entries block on a merged reload.
	AllowStale bool
	// OnEvict is called (without the cache lock held) for every entry
	// evicted because of capacity pressure.
	OnEvict func(key string, e Entry)
	// OnEvent is called (without the cache lock held) for every store,
	// drop and invalidation, with the reason for the decision.
	OnEvent func(Event)
}

// Cache organises keys, versions and load tasks on top of a Store.
type Cache struct {
	loader     SourceLoader
	store      Store
	capacity   int
	ttl        time.Duration
	allowStale bool
	onEvict    func(string, Entry)
	onEvent    func(Event)

	mu       sync.Mutex
	lru      *list.List // front = most recently used; elements are keys
	lruIndex map[string]*list.Element
	// generations counts local Invalidate calls per key. It is a local
	// invalidation counter only, never a source freshness marker.
	generations map[string]uint64
	// sourceVersions is the newest source business version known per
	// key, kept even across invalidations so a late, older source read
	// can never overwrite a newer known result.
	sourceVersions map[string]uint64
	// dependents maps a key to the set of keys whose entries depend on
	// it; invalidation propagates along these edges.
	dependents map[string]map[string]struct{}
	inflight   map[string]*loadCall
	viewLoads  map[string]*viewLoad
	nextViewID uint64
	nextCommit uint64
	stats      Stats
}

// New builds a Cache. The loader is required.
func New(opts Options) *Cache {
	st := opts.Store
	if st == nil {
		st = NewMapStore()
	}
	loader := opts.SourceLoader
	if loader == nil && opts.Loader != nil {
		l := opts.Loader
		loader = func(ctx context.Context, key string) (SourceResult, error) {
			v, err := l(ctx, key)
			return SourceResult{Value: v}, err
		}
	}
	return &Cache{
		loader:         loader,
		store:          st,
		capacity:       opts.Capacity,
		ttl:            opts.TTL,
		allowStale:     opts.AllowStale,
		onEvict:        opts.OnEvict,
		onEvent:        opts.OnEvent,
		lru:            list.New(),
		lruIndex:       make(map[string]*list.Element),
		generations:    make(map[string]uint64),
		sourceVersions: make(map[string]uint64),
		dependents:     make(map[string]map[string]struct{}),
		inflight:       make(map[string]*loadCall),
		viewLoads:      make(map[string]*viewLoad),
	}
}

// loadCall is a shared in-flight load. Waiters select on done, so one
// waiter cancelling never cancels the load for the others.
type loadCall struct {
	done   chan struct{}
	result SourceResult
	err    error
	gen    uint64
}

type viewLoad struct {
	key    string
	done   chan struct{}
	cancel context.CancelFunc
	closed bool
	result SourceResult
	err    error
	gen    uint64
	refs   int
}

// Get returns the value for key, loading it on a miss. Concurrent Gets
// for the same key share a single loader invocation.
func (c *Cache) Get(ctx context.Context, key string) (any, error) {
	now := time.Now()
	c.mu.Lock()
	if e, ok := c.store.Get(key); ok {
		if !e.expired(now) {
			c.touchLocked(key)
			c.stats.Hits++
			c.mu.Unlock()
			return e.Value, nil
		}
		if c.allowStale {
			c.startLoadLocked(ctx, key) // background refresh
			c.touchLocked(key)
			c.stats.StaleHits++
			c.mu.Unlock()
			return e.Value, nil
		}
	}
	c.stats.Misses++
	call := c.startLoadLocked(ctx, key)
	c.mu.Unlock()

	select {
	case <-call.done:
		return call.result.Value, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// startLoadLocked joins an in-flight load or starts a new one. A load
// started before the latest invalidation belongs to an older generation:
// new waiters must not join it, so a fresh load takes its place (the old
// one still finishes and is dropped on commit).
func (c *Cache) startLoadLocked(ctx context.Context, key string) *loadCall {
	gen := c.generations[key]
	if call, ok := c.inflight[key]; ok && call.gen == gen {
		return call
	}
	call := &loadCall{done: make(chan struct{}), gen: gen}
	c.inflight[key] = call
	c.stats.Loads++
	// Detach from the caller's cancellation: the load belongs to all
	// current and future waiters, not to whoever happened to start it.
	loadCtx := context.WithoutCancel(ctx)
	go c.runLoad(loadCtx, key, call)
	return call
}

func (c *Cache) runLoad(ctx context.Context, key string, call *loadCall) {
	res, err := c.loader(ctx, key)
	var events []Event
	var evicted []struct {
		key   string
		entry Entry
	}
	c.mu.Lock()
	if err == nil {
		switch {
		case c.generations[key] != call.gen:
			// Key was invalidated while loading: the stale result
			// must not overwrite the newer state.
			c.stats.StaleDrops++
			events = append(events, Event{Key: key, Op: EventDropped,
				Reason:        "invalidated-during-load",
				SourceVersion: res.Version})
		case res.Version < c.sourceVersions[key]:
			// A read of an older source snapshot finished late.
			// Local generations say nothing about source freshness,
			// so compare against the known source version instead.
			c.stats.OldSourceDrops++
			events = append(events, Event{Key: key, Op: EventDropped,
				Reason:        "older-source-version",
				SourceVersion: res.Version})
		default:
			e := Entry{
				Value:         res.Value,
				Version:       call.gen,
				SourceVersion: res.Version,
				Deps:          res.Deps,
				DepVersions:   cloneVersions(res.DepVersions),
			}
			if c.ttl > 0 {
				e.ExpiresAt = time.Now().Add(c.ttl)
			}
			c.setDepsLocked(key, res.Deps)
			c.store.Set(key, e)
			c.sourceVersions[key] = res.Version
			c.touchLocked(key)
			evicted = c.evictLocked()
			events = append(events, Event{Key: key, Op: EventStored,
				Reason:        "loaded",
				SourceVersion: res.Version})
		}
	}
	call.result, call.err = res, err
	if c.inflight[key] == call {
		delete(c.inflight, key)
	}
	close(call.done)
	c.mu.Unlock()
	c.emit(events)
	for _, ev := range evicted {
		if c.onEvict != nil {
			c.onEvict(ev.key, ev.entry)
		}
	}
}

// ReadView returns a retainable view of roots and their complete committed
// dependency closure. Missing or expired parts are loaded together; the
// returned view never changes because of a later refresh.
func (c *Cache) ReadView(ctx context.Context, roots ...string) (*View, error) {
	return c.refreshView(ctx, roots)
}

// GetView is an explicit alias for ReadView.
func (c *Cache) GetView(ctx context.Context, roots ...string) (*View, error) {
	return c.ReadView(ctx, roots...)
}

func (c *Cache) refreshView(ctx context.Context, requested []string) (*View, error) {
	roots := uniqueStrings(requested)
	group := &viewGroup{}
	c.mu.Lock()
	calls := c.startViewWaveLocked(ctx, roots, group)
	c.mu.Unlock()

	if waitErr := waitViewCalls(ctx, calls); waitErr != nil {
		view := c.buildPartialView(roots, group, waitErr)
		c.releaseViewGroup(group)
		return view, waitErr
	}

	for {
		c.mu.Lock()
		closure, errs, complete := c.groupClosureLocked(roots, group)
		if !complete {
			missing := c.nextMissingViewCallsLocked(ctx, closure, group)
			if len(missing) == 0 {
				waitErr := firstViewError(errs)
				view := c.buildViewLocked(roots, closure, group, false, waitErr)
				c.mu.Unlock()
				c.releaseViewGroup(group)
				return view, waitErr
			}
			calls = missing
			c.mu.Unlock()
			if waitErr := waitViewCalls(ctx, calls); waitErr != nil {
				view := c.buildPartialView(roots, group, waitErr)
				c.releaseViewGroup(group)
				return view, waitErr
			}
			continue
		}

		if !candidateConsistent(closure) || !c.candidateVersionsCurrentLocked(closure) {
			waitErr := fmt.Errorf("cache: composite candidate is not consistent")
			view := c.buildViewLocked(roots, closure, group, false, waitErr)
			c.mu.Unlock()
			c.releaseViewGroup(group)
			return view, waitErr
		}

		if !candidateChanged(closure, group.baseline) {
			for key, entry := range group.baseline {
				if cur, ok := closure[key]; ok && cur.entry.SourceVersion == entry.SourceVersion {
					closure[key] = viewCandidate{entry: entry}
				}
			}
			view := c.buildViewLocked(roots, closure, group, true, nil)
			c.mu.Unlock()
			c.releaseViewGroup(group)
			return view, nil
		}

		result := c.commitViewLocked(roots, closure, group)
		c.mu.Unlock()
		c.releaseViewGroup(group)
		c.emit(result.events)
		for _, ev := range result.evicted {
			if c.onEvict != nil {
				c.onEvict(ev.key, ev.entry)
			}
		}
		return result.view, nil
	}
}

type viewGroup struct {
	baseline map[string]Entry
	calls    map[string]*viewLoad
}

type viewCandidate struct {
	entry     Entry
	candidate bool
	pending   bool
	stale     bool
	err       error
}

type committedView struct {
	view    *View
	events  []Event
	evicted []struct {
		key   string
		entry Entry
	}
}

func (c *Cache) startViewWaveLocked(ctx context.Context, roots []string, group *viewGroup) []*viewLoad {
	now := time.Now()
	group.baseline = c.storedClosureLocked(roots)
	group.calls = make(map[string]*viewLoad)
	return c.startViewCallsLocked(ctx, roots, group, now, true)
}

func (c *Cache) startViewCallsLocked(ctx context.Context, keys []string, group *viewGroup, now time.Time, force bool) []*viewLoad {
	var calls []*viewLoad
	seen := make(map[string]struct{})
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if call := group.calls[key]; call != nil && call.gen == c.generations[key] {
			continue
		}
		if !force {
			if e, ok := c.store.Get(key); ok && !e.expired(now) {
				continue
			}
		}
		calls = append(calls, c.startViewLoadLocked(ctx, key, group))
	}
	return calls
}

func (c *Cache) startViewLoadLocked(ctx context.Context, key string, group *viewLoadRefs) *viewLoad {
	gen := c.generations[key]
	if call, ok := c.viewLoads[key]; ok && call.gen == gen {
		call.refs++
		if group != nil {
			if group.calls == nil {
				group.calls = make(map[string]*viewLoad)
			}
			group.calls[key] = call
		}
		return call
	}
	loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	call := &viewLoad{key: key, done: make(chan struct{}), cancel: cancel, gen: gen, refs: 1}
	c.viewLoads[key] = call
	if group != nil {
		if group.calls == nil {
			group.calls = make(map[string]*viewLoad)
		}
		group.calls[key] = call
	}
	c.stats.Loads++
	go c.runViewLoad(loadCtx, call)
	return call
}

type viewLoadRefs = viewGroup

func (c *Cache) runViewLoad(ctx context.Context, call *viewLoad) {
	res, err := c.loader(ctx, call.key)
	c.mu.Lock()
	call.result, call.err = res, err
	if c.viewLoads[call.key] == call && call.refs == 0 {
		delete(c.viewLoads, call.key)
	}
	if !call.closed {
		call.closed = true
		close(call.done)
	}
	c.mu.Unlock()
}

func waitViewCalls(ctx context.Context, calls []*viewLoad) error {
	for _, call := range calls {
		select {
		case <-call.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (c *Cache) releaseViewGroup(group *viewGroup) {
	c.mu.Lock()
	var cancel []context.CancelFunc
	for _, call := range group.calls {
		call.refs--
		if call.refs == 0 {
			if c.viewLoads[call.key] == call {
				delete(c.viewLoads, call.key)
			}
			cancel = append(cancel, call.cancel)
		}
	}
	c.mu.Unlock()
	for _, fn := range cancel {
		fn()
	}
}

func (c *Cache) storedClosureLocked(roots []string) map[string]Entry {
	out := make(map[string]Entry)
	var walk func(string)
	walk = func(key string) {
		if _, ok := out[key]; ok {
			return
		}
		e, ok := c.store.Get(key)
		if !ok {
			return
		}
		out[key] = e
		for _, dep := range e.Deps {
			walk(dep)
		}
	}
	for _, key := range roots {
		walk(key)
	}
	return out
}

func (c *Cache) groupClosureLocked(roots []string, group *viewGroup) (map[string]viewCandidate, map[string]error, bool) {
	out := make(map[string]viewCandidate)
	errs := make(map[string]error)
	var walk func(string)
	walk = func(key string) {
		if _, ok := out[key]; ok {
			return
		}
		now := time.Now()
		if call := group.calls[key]; call != nil {
			switch {
			case call.gen != c.generations[key]:
				out[key] = viewCandidate{pending: true}
			case call.err != nil:
				errs[key] = call.err
				if e, ok := c.store.Get(key); ok {
					out[key] = viewCandidate{entry: e, candidate: true, stale: e.expired(now), err: call.err}
				} else if e, ok := group.baseline[key]; ok {
					out[key] = viewCandidate{entry: e, candidate: true, stale: true, err: call.err}
				} else {
					out[key] = viewCandidate{candidate: true, err: call.err}
				}
			default:
				select {
				case <-call.done:
					out[key] = viewCandidate{entry: sourceEntry(call.result, call.gen, 0), candidate: true}
				default:
					out[key] = viewCandidate{pending: true}
				}
			}
		}
		if _, ok := out[key]; !ok {
			if e, ok := c.store.Get(key); ok {
				out[key] = viewCandidate{entry: e, stale: e.expired(now)}
			} else if e, ok := group.baseline[key]; ok {
				out[key] = viewCandidate{entry: e, stale: true}
			} else {
				out[key] = viewCandidate{pending: true}
			}
		}
		cur := out[key]
		deps := cur.entry.Deps
		if len(deps) == 0 {
			if base, ok := group.baseline[key]; ok {
				deps = base.Deps
			}
		}
		for _, dep := range deps {
			walk(dep)
		}
	}
	for _, root := range roots {
		walk(root)
	}
	complete := true
	for key, cur := range out {
		if cur.pending || cur.err != nil || cur.stale {
			complete = false
		}
		for dep, version := range cur.entry.DepVersions {
			depCur, ok := out[dep]
			if !ok || depCur.pending || depCur.err != nil || depCur.entry.SourceVersion != version {
				complete = false
				if key != dep && version != 0 {
					errs[key] = fmt.Errorf("cache: dependency %s is not at source version %d", dep, version)
				}
			}
		}
	}
	return out, errs, complete
}

func (c *Cache) nextMissingViewCallsLocked(ctx context.Context, closure map[string]viewCandidate, group *viewGroup) []*viewLoad {
	now := time.Now()
	keySet := make(map[string]struct{})
	for key, cur := range closure {
		needsLoad := cur.pending || cur.err != nil || cur.stale
		deps := cur.entry.Deps
		if len(deps) == 0 {
			if base, ok := group.baseline[key]; ok {
				deps = base.Deps
			}
		}
		if needsLoad {
			keySet[key] = struct{}{}
		}
		if !needsLoad {
			for dep, version := range cur.entry.DepVersions {
				depCur, ok := closure[dep]
				if !ok || depCur.pending || depCur.err != nil || depCur.entry.SourceVersion != version {
					needsLoad = true
				}
				_ = version
			}
		}
		if needsLoad {
			keySet[key] = struct{}{}
		}
		for _, dep := range deps {
			depCur, ok := closure[dep]
			wantVersion, hasVersion := cur.entry.DepVersions[dep]
			if !ok || depCur.pending || depCur.err != nil || depCur.stale || (hasVersion && depCur.entry.SourceVersion != wantVersion) {
				keySet[dep] = struct{}{}
			}
		}
	}
	var keys []string
	for key := range keySet {
		keys = append(keys, key)
	}
	return c.startViewCallsLocked(ctx, keys, group, now, true)
}

func candidateConsistent(closure map[string]viewCandidate) bool {
	for key, cur := range closure {
		if cur.pending || cur.err != nil || cur.stale {
			return false
		}
		seen := make(map[string]struct{})
		for dep, version := range cur.entry.DepVersions {
			seen[dep] = struct{}{}
			depCur, ok := closure[dep]
			if !ok || depCur.pending || depCur.err != nil || depCur.entry.SourceVersion != version {
				return false
			}
		}
		for _, dep := range cur.entry.Deps {
			if _, hasVersion := seen[dep]; hasVersion {
				continue
			}
			depCur, ok := closure[dep]
			if !ok || depCur.pending || depCur.err != nil || depCur.stale {
				return false
			}
		}
		_ = key
	}
	return true
}

func (c *Cache) candidateVersionsCurrentLocked(closure map[string]viewCandidate) bool {
	for key, cur := range closure {
		if cur.entry.SourceVersion < c.sourceVersions[key] {
			return false
		}
	}
	return true
}

func candidateChanged(closure map[string]viewCandidate, baseline map[string]Entry) bool {
	for key, cur := range closure {
		old, ok := baseline[key]
		if !ok || old.SourceVersion != cur.entry.SourceVersion || old.Value != cur.entry.Value || !sameDeps(old.Deps, cur.entry.Deps) {
			return true
		}
	}
	return false
}

func sourceEntry(res SourceResult, gen, commitID uint64) Entry {
	return Entry{
		Value:         res.Value,
		Version:       gen,
		SourceVersion: res.Version,
		Deps:          append([]string(nil), res.Deps...),
		DepVersions:   cloneVersions(res.DepVersions),
		CommitID:      commitID,
	}
}

func (c *Cache) commitViewLocked(roots []string, closure map[string]viewCandidate, group *viewGroup) committedView {
	c.nextCommit++
	commitID := c.nextCommit
	for key, cur := range closure {
		if old, ok := c.store.Get(key); ok {
			removeDepsLocked(c, key, old.Deps)
		}
		e := cur.entry
		e.Version = c.generations[key]
		e.CommitID = commitID
		if c.ttl > 0 {
			e.ExpiresAt = time.Now().Add(c.ttl)
		}
		closure[key] = viewCandidate{entry: e, candidate: true}
	}

	var events []Event
	for key, cur := range closure {
		e := cur.entry
		c.setDepsLocked(key, e.Deps)
		c.store.Set(key, e)
		c.sourceVersions[key] = e.SourceVersion
		c.touchLocked(key)
		events = append(events, Event{
			Key: key, Op: EventStored, Reason: "composite-loaded", SourceVersion: e.SourceVersion,
		})
	}
	evicted := c.evictLocked()
	view := c.buildViewLocked(roots, closure, group, true, nil)
	view.CommitID = commitID
	return committedView{view: view, events: events, evicted: evicted}
}

func removeDepsLocked(c *Cache, key string, deps []string) {
	for _, dep := range deps {
		if set, ok := c.dependents[dep]; ok {
			delete(set, key)
			if len(set) == 0 {
				delete(c.dependents, dep)
			}
		}
	}
}

func (c *Cache) buildViewLocked(roots []string, closure map[string]viewCandidate, group *viewGroup, committed bool, err error) *View {
	c.nextViewID++
	view := &View{
		cache:      c,
		rootKeys:   append([]string(nil), roots...),
		ID:         c.nextViewID,
		Items:      make(map[string]ViewItem),
		Candidates: make(map[string]ViewItem),
	}
	order := closureOrder(roots, closure)
	view.Order = order
	var commitID uint64
	for _, key := range order {
		cur := closure[key]
		if cur.err != nil {
			view.Items[key] = ViewItem{Key: key, Status: ViewUnavailable, Error: cur.err}
		} else if cur.pending || cur.stale || (!committed && cur.candidate) {
			if base, ok := group.baseline[key]; ok && !base.expired(time.Now()) {
				view.Items[key] = viewItemFromEntry(key, base, ViewCommitted)
				commitID = base.CommitID
			} else if e, ok := c.store.Get(key); ok && !e.expired(time.Now()) {
				view.Items[key] = viewItemFromEntry(key, e, ViewCommitted)
				commitID = e.CommitID
			} else if cur.pending {
				view.Items[key] = ViewItem{Key: key, Status: ViewPending}
			} else {
				itemErr := cur.err
				if itemErr == nil {
					itemErr = err
				}
				view.Items[key] = ViewItem{Key: key, Status: ViewUnavailable, Error: itemErr}
			}
		} else {
			item := viewItemFromEntry(key, cur.entry, ViewCommitted)
			view.Items[key] = item
			commitID = cur.entry.CommitID
		}

		if cur.candidate && cur.err == nil && !cur.pending {
			candidate := viewItemFromEntry(key, cur.entry, ViewCandidate)
			view.Candidates[key] = candidate
			base, hasBase := group.baseline[key]
			if !hasBase || base.SourceVersion != cur.entry.SourceVersion {
				view.CandidateChanges = append(view.CandidateChanges, key)
			}
		}
	}
	if committed {
		view.CommitID = commitID
		view.Complete = true
		view.Candidates = nil
		view.CandidateChanges = nil
	}
	return view
}

func (c *Cache) buildPartialView(roots []string, group *viewGroup, err error) *View {
	c.mu.Lock()
	defer c.mu.Unlock()
	closure, _, _ := c.groupClosureLocked(roots, group)
	return c.buildViewLocked(roots, closure, group, false, err)
}

func viewItemFromEntry(key string, e Entry, status ViewItemStatus) ViewItem {
	return ViewItem{
		Key:           key,
		Value:         e.Value,
		SourceVersion: e.SourceVersion,
		Deps:          append([]string(nil), e.Deps...),
		DepVersions:   cloneVersions(e.DepVersions),
		CommitID:      e.CommitID,
		Status:        status,
	}
}

func closureOrder(roots []string, closure map[string]viewCandidate) []string {
	var order []string
	seen := make(map[string]struct{})
	var walk func(string)
	walk = func(key string) {
		if _, ok := seen[key]; ok {
			return
		}
		cur, ok := closure[key]
		if !ok {
			return
		}
		seen[key] = struct{}{}
		order = append(order, key)
		for _, dep := range cur.entry.Deps {
			walk(dep)
		}
	}
	for _, root := range roots {
		walk(root)
	}
	return order
}

func cloneVersions(in map[string]uint64) map[string]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func sameDeps(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]struct{}, len(a))
	for _, key := range a {
		seen[key] = struct{}{}
	}
	for _, key := range b {
		if _, ok := seen[key]; !ok {
			return false
		}
	}
	return true
}

func uniqueStrings(in []string) []string {
	var out []string
	seen := make(map[string]struct{})
	for _, value := range in {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func firstViewError(errs map[string]error) error {
	for _, err := range errs {
		return err
	}
	return nil
}

// Invalidate marks key stale and bumps its local generation, so any load
// started before this call is discarded on completion instead of being
// stored. The invalidation propagates to every entry that depends on key
// (transitively); dependency cycles are broken by a visited set so a
// cyclic graph cannot cause a refresh loop.
func (c *Cache) Invalidate(key string) {
	var events []Event
	c.mu.Lock()
	c.invalidateLocked(key, make(map[string]struct{}), &events)
	c.mu.Unlock()
	c.emit(events)
}

func (c *Cache) invalidateLocked(key string, seen map[string]struct{}, events *[]Event) {
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	c.generations[key]++
	if call, ok := c.viewLoads[key]; ok {
		delete(c.viewLoads, key)
		call.closed = true
		close(call.done)
		call.cancel()
	}
	deps := c.dependents[key]
	c.removeLocked(key)
	*events = append(*events, Event{Key: key, Op: EventInvalidated, Reason: "invalidate"})
	for d := range deps {
		c.invalidateLocked(d, seen, events)
	}
}

// setDepsLocked replaces the dependency edges of key, updating the
// reverse (dependents) index.
func (c *Cache) setDepsLocked(key string, deps []string) {
	if old, ok := c.store.Get(key); ok {
		for _, d := range old.Deps {
			if set, ok := c.dependents[d]; ok {
				delete(set, key)
				if len(set) == 0 {
					delete(c.dependents, d)
				}
			}
		}
	}
	for _, d := range deps {
		if d == key {
			continue // a self-edge would invalidate itself forever
		}
		set := c.dependents[d]
		if set == nil {
			set = make(map[string]struct{})
			c.dependents[d] = set
		}
		set[key] = struct{}{}
	}
}

func (c *Cache) emit(events []Event) {
	if c.onEvent == nil {
		return
	}
	for _, ev := range events {
		c.onEvent(ev)
	}
}

// Refresh forces a background reload of key and reports whether a load
// is now in flight (false if one was already running).
func (c *Cache) Refresh(ctx context.Context, key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.inflight[key]; ok {
		return false
	}
	c.startLoadLocked(ctx, key)
	return true
}

// Stats returns a snapshot of the cache counters.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Len reports the number of stored entries.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.Len()
}

// Version exposes the current local invalidation generation of key.
func (c *Cache) Version(key string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generations[key]
}

// SourceVersion exposes the newest source business version known for key.
func (c *Cache) SourceVersion(key string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sourceVersions[key]
}

func (c *Cache) touchLocked(key string) {
	if el, ok := c.lruIndex[key]; ok {
		c.lru.MoveToFront(el)
	} else {
		c.lruIndex[key] = c.lru.PushFront(key)
	}
}

func (c *Cache) removeLocked(key string) {
	if e, ok := c.store.Delete(key); ok {
		for _, d := range e.Deps {
			if set, ok := c.dependents[d]; ok {
				delete(set, key)
				if len(set) == 0 {
					delete(c.dependents, d)
				}
			}
		}
	}
	if el, ok := c.lruIndex[key]; ok {
		c.lru.Remove(el)
		delete(c.lruIndex, key)
	}
}

// evictLocked drops least-recently-used entries until within capacity.
func (c *Cache) evictLocked() []struct {
	key   string
	entry Entry
} {
	var out []struct {
		key   string
		entry Entry
	}
	if c.capacity <= 0 {
		return out
	}
	for c.store.Len() > c.capacity {
		back := c.lru.Back()
		if back == nil {
			break
		}
		key := back.Value.(string)
		e, _ := c.store.Get(key)
		c.removeLocked(key)
		c.stats.Evictions++
		out = append(out, struct {
			key   string
			entry Entry
		}{key, e})
	}
	return out
}
