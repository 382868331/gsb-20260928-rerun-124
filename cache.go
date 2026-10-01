package cache

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// Loader fetches the value for a key from the authoritative source.
// The returned Result carries the source's own business version and
// the other keys the value was computed from.
type Loader func(ctx context.Context, key string) (Result, error)

// Result is what a Loader returns.
type Result struct {
	Value   any
	Version uint64   // source business version; higher is newer
	Deps    []string // keys this result depends on
}

// Entry is a stored value together with its source version and expiry.
// A zero ExpiresAt means the entry never expires.
type Entry struct {
	Value     any
	Version   uint64 // source business version of Value
	ExpiresAt time.Time
}

func (e Entry) expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt)
}

// Stats reports observable cache behaviour.
type Stats struct {
	Hits         uint64
	StaleHits    uint64
	Misses       uint64
	Loads        uint64 // loader invocations actually started
	Evictions    uint64
	StaleDrops   uint64 // results dropped: key invalidated while loading
	VersionDrops uint64 // results dropped: newer source version already known
}

// EventKind classifies a recorded cache event.
type EventKind int

const (
	EventStored EventKind = iota
	EventDropped
	EventInvalidated
)

func (k EventKind) String() string {
	switch k {
	case EventStored:
		return "stored"
	case EventDropped:
		return "dropped"
	case EventInvalidated:
		return "invalidated"
	}
	return "unknown"
}

// Event records why a result was stored or dropped, or a key invalidated.
type Event struct {
	Key     string
	Kind    EventKind
	Reason  string
	Version uint64 // source version involved, when applicable
}

// Options configures a Cache.
type Options struct {
	Loader   Loader
	Store    Store // nil uses NewMapStore()
	Capacity int   // max entries; <=0 means unbounded
	TTL      time.Duration
	// AllowStale serves an expired entry while a refresh runs in the
	// background. Without it, expired entries block on a merged reload.
	AllowStale bool
	// OnEvict is called (without the cache lock held) for every entry
	// evicted because of capacity pressure.
	OnEvict func(key string, e Entry)
	// OnEvent, if set, is called (without the cache lock held) for every
	// stored, dropped, or invalidated event. Events are also kept in a
	// bounded in-memory log, see Events().
	OnEvent func(Event)
}

// Cache organises keys, versions and load tasks on top of a Store.
//
// Two kinds of versions are kept strictly apart:
//   - the local generation (versions map) counts Invalidate calls and
//     only orders local events;
//   - the source business version (srcVers map, Entry.Version) comes
//     from the loader and decides which value is actually newer.
type Cache struct {
	loader     Loader
	store      Store
	capacity   int
	ttl        time.Duration
	allowStale bool
	onEvict    func(string, Entry)
	onEvent    func(Event)

	mu       sync.Mutex
	lru      *list.List // front = most recently used; elements are keys
	lruIndex map[string]*list.Element
	versions map[string]uint64              // local invalidation generation per key
	srcVers  map[string]uint64              // highest source version known per key
	deps     map[string]map[string]struct{} // dependency -> dependent keys
	keyDeps  map[string]map[string]struct{} // key -> its dependencies
	inflight map[string]*loadCall
	stats    Stats
	events   []Event
}

const maxEventLog = 256

// New builds a Cache. The loader is required.
func New(opts Options) *Cache {
	st := opts.Store
	if st == nil {
		st = NewMapStore()
	}
	return &Cache{
		loader:     opts.Loader,
		store:      st,
		capacity:   opts.Capacity,
		ttl:        opts.TTL,
		allowStale: opts.AllowStale,
		onEvict:    opts.OnEvict,
		onEvent:    opts.OnEvent,
		lru:        list.New(),
		lruIndex:   make(map[string]*list.Element),
		versions:   make(map[string]uint64),
		srcVers:    make(map[string]uint64),
		deps:       make(map[string]map[string]struct{}),
		keyDeps:    make(map[string]map[string]struct{}),
		inflight:   make(map[string]*loadCall),
	}
}

// loadCall is a shared in-flight load. Waiters select on done, so one
// waiter cancelling never cancels the load for the others.
type loadCall struct {
	done    chan struct{}
	value   any
	err     error
	version uint64 // local generation the load started at
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
		// Serve the expired entry only while it still reflects the
		// newest source version we know about.
		if c.allowStale && e.Version >= c.srcVers[key] {
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
		return call.value, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// startLoadLocked joins an in-flight load or starts a new one. A load
// started before the latest invalidation is left running for its own
// waiters, but new waiters get a fresh load at the current generation.
func (c *Cache) startLoadLocked(ctx context.Context, key string) *loadCall {
	if call, ok := c.inflight[key]; ok && call.version == c.versions[key] {
		return call
	}
	call := &loadCall{done: make(chan struct{}), version: c.versions[key]}
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
	var evicted []struct {
		key   string
		entry Entry
	}
	var events []Event
	c.mu.Lock()
	if err == nil {
		switch {
		case c.versions[key] != call.version:
			// Key was invalidated while loading: the stale result
			// must not overwrite the newer state.
			c.stats.StaleDrops++
			events = append(events, Event{Key: key, Kind: EventDropped,
				Reason: "invalidated-during-load", Version: res.Version})
		case res.Version < c.srcVers[key]:
			// The source itself told us earlier about a newer
			// version; this late, older read must not replace it.
			c.stats.VersionDrops++
			events = append(events, Event{Key: key, Kind: EventDropped,
				Reason: "superseded-by-newer-source-version", Version: res.Version})
		default:
			e := Entry{Value: res.Value, Version: res.Version}
			if c.ttl > 0 {
				e.ExpiresAt = time.Now().Add(c.ttl)
			}
			c.store.Set(key, e)
			c.srcVers[key] = res.Version
			c.setDepsLocked(key, res.Deps)
			c.touchLocked(key)
			evicted = c.evictLocked()
			events = append(events, Event{Key: key, Kind: EventStored,
				Reason: "updated", Version: res.Version})
		}
	}
	call.value, call.err = res.Value, err
	if c.inflight[key] == call {
		delete(c.inflight, key)
	}
	close(call.done)
	c.recordEventsLocked(events)
	c.mu.Unlock()
	for _, ev := range evicted {
		if c.onEvict != nil {
			c.onEvict(ev.key, ev.entry)
		}
	}
}

// setDepsLocked replaces the dependency edges of key, keeping the
// reverse index (dependency -> dependents) in sync.
func (c *Cache) setDepsLocked(key string, deps []string) {
	for old := range c.keyDeps[key] {
		if set, ok := c.deps[old]; ok {
			delete(set, key)
			if len(set) == 0 {
				delete(c.deps, old)
			}
		}
	}
	next := make(map[string]struct{}, len(deps))
	for _, d := range deps {
		if d == key {
			continue // a key never depends on itself
		}
		next[d] = struct{}{}
		set, ok := c.deps[d]
		if !ok {
			set = make(map[string]struct{})
			c.deps[d] = set
		}
		set[key] = struct{}{}
	}
	c.keyDeps[key] = next
}

// Invalidate marks key stale, bumps its local generation, and propagates
// the invalidation to every key that (transitively) depends on it. Any
// load started before this call is discarded on completion instead of
// being stored. The visited set keeps dependency cycles from looping.
func (c *Cache) Invalidate(key string) {
	var events []Event
	c.mu.Lock()
	queue := []string{key}
	seen := map[string]struct{}{key: {}}
	for len(queue) > 0 {
		k := queue[0]
		queue = queue[1:]
		c.versions[k]++
		c.removeLocked(k)
		reason := "invalidate"
		if k != key {
			reason = "dependency-invalidate"
		}
		events = append(events, Event{Key: k, Kind: EventInvalidated, Reason: reason})
		for dep := range c.deps[k] {
			if _, ok := seen[dep]; !ok {
				seen[dep] = struct{}{}
				queue = append(queue, dep)
			}
		}
	}
	c.recordEventsLocked(events)
	c.mu.Unlock()
}

// Refresh forces a background reload of key and reports whether a load
// is now in flight (false if a current-generation one was already running).
func (c *Cache) Refresh(ctx context.Context, key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if call, ok := c.inflight[key]; ok && call.version == c.versions[key] {
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

// Version exposes the current local invalidation generation of key. It
// counts Invalidate calls only and says nothing about source versions.
func (c *Cache) Version(key string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.versions[key]
}

// SourceVersion exposes the highest source business version known for key.
func (c *Cache) SourceVersion(key string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.srcVers[key]
}

// Events returns a copy of the bounded event log, oldest first.
func (c *Cache) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Event, len(c.events))
	copy(out, c.events)
	return out
}

// recordEventsLocked appends to the bounded log and invokes the hook.
func (c *Cache) recordEventsLocked(events []Event) {
	for _, ev := range events {
		c.events = append(c.events, ev)
		if len(c.events) > maxEventLog {
			c.events = append(c.events[:0], c.events[1:]...)
		}
		if c.onEvent != nil {
			ev := ev
			c.mu.Unlock()
			c.onEvent(ev)
			c.mu.Lock()
		}
	}
}

func (c *Cache) touchLocked(key string) {
	if el, ok := c.lruIndex[key]; ok {
		c.lru.MoveToFront(el)
	} else {
		c.lruIndex[key] = c.lru.PushFront(key)
	}
}

func (c *Cache) removeLocked(key string) {
	c.store.Delete(key)
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
