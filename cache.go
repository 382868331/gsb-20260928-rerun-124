package cache

import (
	"container/list"
	"context"
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
	ExpiresAt     time.Time
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
