package cache

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// Loader fetches the value for a key from the authoritative source.
type Loader func(ctx context.Context, key string) (any, error)

// Entry is a stored value together with its version and expiry.
// A zero ExpiresAt means the entry never expires.
type Entry struct {
	Value     any
	Version   uint64
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
}

// Cache organises keys, versions and load tasks on top of a Store.
type Cache struct {
	loader     Loader
	store      Store
	capacity   int
	ttl        time.Duration
	allowStale bool
	onEvict    func(string, Entry)

	mu       sync.Mutex
	lru      *list.List // front = most recently used; elements are keys
	lruIndex map[string]*list.Element
	versions map[string]uint64
	inflight map[string]*loadCall
	stats    Stats
}

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
		lru:        list.New(),
		lruIndex:   make(map[string]*list.Element),
		versions:   make(map[string]uint64),
		inflight:   make(map[string]*loadCall),
	}
}

// loadCall is a shared in-flight load. Waiters select on done, so one
// waiter cancelling never cancels the load for the others.
type loadCall struct {
	done    chan struct{}
	value   any
	err     error
	version uint64
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
		return call.value, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// startLoadLocked joins an in-flight load or starts a new one.
func (c *Cache) startLoadLocked(ctx context.Context, key string) *loadCall {
	if call, ok := c.inflight[key]; ok {
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
	v, err := c.loader(ctx, key)
	var evicted []struct {
		key   string
		entry Entry
	}
	c.mu.Lock()
	if err == nil {
		if c.versions[key] == call.version {
			e := Entry{Value: v, Version: call.version}
			if c.ttl > 0 {
				e.ExpiresAt = time.Now().Add(c.ttl)
			}
			c.store.Set(key, e)
			c.touchLocked(key)
			evicted = c.evictLocked()
		} else {
			// Key was invalidated while loading: the stale result
			// must not overwrite the newer state.
			c.stats.StaleDrops++
		}
	}
	call.value, call.err = v, err
	delete(c.inflight, key)
	close(call.done)
	c.mu.Unlock()
	for _, ev := range evicted {
		if c.onEvict != nil {
			c.onEvict(ev.key, ev.entry)
		}
	}
}

// Invalidate marks key stale and bumps its version, so any load started
// before this call is discarded on completion instead of being stored.
func (c *Cache) Invalidate(key string) {
	c.mu.Lock()
	c.versions[key]++
	c.removeLocked(key)
	c.mu.Unlock()
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

// Version exposes the current invalidation version of key.
func (c *Cache) Version(key string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.versions[key]
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
