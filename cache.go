package cacheruntime

import (
	"context"
	"sync"
	"time"
)

type Cache[K comparable, V any] struct {
	mu          sync.Mutex
	rootCtx     context.Context
	loader      Loader[K, V]
	storage     Storage[K, V]
	capacity    int
	onEviction  func(Eviction[K, V])
	generations map[K]uint64
	calls       map[K]*loadCall[K, V]
	stats       Stats
	closed      bool
	closeOnce   sync.Once
	closeCancel context.CancelFunc
}

func New[K comparable, V any](opts Options[K, V]) *Cache[K, V] {
	storage := opts.Storage
	if storage == nil {
		storage = NewMemoryStorage[K, V]()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Cache[K, V]{
		rootCtx:     ctx,
		loader:      opts.Loader,
		storage:     storage,
		capacity:    opts.Capacity,
		onEviction:  opts.OnEviction,
		generations: make(map[K]uint64),
		calls:       make(map[K]*loadCall[K, V]),
		closeCancel: cancel,
	}
}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, uint64, error) {
	return c.GetWith(ctx, key, GetOptions{})
}

func (c *Cache[K, V]) GetWith(ctx context.Context, key K, opts GetOptions) (V, uint64, error) {
	call, stale, err := c.beginGet(key, opts.AllowStaleOnRefresh)
	if err != nil {
		var zero V
		return zero, 0, err
	}
	if stale != nil {
		return stale.Value, stale.Version, nil
	}
	return c.await(ctx, call)
}

func (c *Cache[K, V]) Refresh(ctx context.Context, key K) (V, uint64, error) {
	call, err := c.startOrJoin(key)
	if err != nil {
		var zero V
		return zero, 0, err
	}
	return c.await(ctx, call)
}

func (c *Cache[K, V]) Put(key K, loaded Loaded[V]) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	generation := c.generations[key]
	evictions, err := c.commitLocked(key, Record[V]{
		Value:        loaded.Value,
		Version:      loaded.Version,
		Generation:   generation,
		Fresh:        true,
		LastAccessed: time.Now(),
	})
	c.mu.Unlock()
	c.emit(evictions)
	return err
}

func (c *Cache[K, V]) Invalidate(key K) {
	c.mu.Lock()
	c.generations[key]++
	if record, ok := c.storage.Load(key); ok {
		record.Fresh = false
		record.Generation = c.generations[key]
		c.storage.Save(key, record)
	}
	c.mu.Unlock()
}

func (c *Cache[K, V]) Delete(key K) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	record, ok := c.storage.Remove(key)
	c.generations[key]++
	c.mu.Unlock()
	if ok {
		c.emit([]Eviction[K, V]{{Key: key, Value: record.Value, Reason: EvictionDeleted}})
	}
	return nil
}

func (c *Cache[K, V]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.storage.Len()
}

func (c *Cache[K, V]) Close() error {
	c.closeOnce.Do(func() {
		c.closeCancel()
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
	})
	return nil
}

func (c *Cache[K, V]) beginGet(key K, allowStale bool) (*loadCall[K, V], *Record[V], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, ErrClosed
	}
	if record, ok := c.storage.Load(key); ok {
		now := time.Now()
		record.LastAccessed = now
		c.storage.Touch(key, now)
		if record.Fresh {
			c.stats.Hits++
			return nil, &record, nil
		}
		if allowStale {
			c.stats.StaleHits++
			stale := record
			if call := c.calls[key]; call == nil || call.generation != record.Generation {
				_, _, _ = c.startOrJoinLocked(key)
			}
			return nil, &stale, nil
		}
	}
	c.stats.Misses++
	call, _, err := c.startOrJoinLocked(key)
	return call, nil, err
}

func (c *Cache[K, V]) startOrJoin(key K) (*loadCall[K, V], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	call, _, err := c.startOrJoinLocked(key)
	return call, err
}

func (c *Cache[K, V]) startOrJoinLocked(key K) (*loadCall[K, V], bool, error) {
	generation := c.generations[key]
	if call := c.calls[key]; call != nil && call.generation == generation {
		c.stats.CoalescedLoads++
		return call, true, nil
	}
	if c.loader == nil {
		return nil, false, ErrNoLoader
	}
	call := &loadCall[K, V]{key: key, generation: generation, done: make(chan struct{})}
	c.calls[key] = call
	c.stats.StartedLoads++
	loader := c.loader
	go c.runLoad(call, loader)
	return call, true, nil
}

func (c *Cache[K, V]) runLoad(call *loadCall[K, V], loader Loader[K, V]) {
	ctx, cancel := context.WithCancel(c.rootCtx)
	defer cancel()
	loaded, err := loader(ctx, call.key)
	c.finishLoad(call, loaded, err)
}

func (c *Cache[K, V]) finishLoad(call *loadCall[K, V], loaded Loaded[V], loadErr error) {
	c.mu.Lock()
	var evictions []Eviction[K, V]
	if c.calls[call.key] == call {
		delete(c.calls, call.key)
	}
	if loadErr != nil {
		c.stats.FailedLoads++
	} else if c.closed || c.generations[call.key] != call.generation {
		c.stats.RejectedStaleLoads++
		loadErr = ErrStaleVersion
	} else {
		c.stats.CompletedLoads++
		evictions, loadErr = c.commitLocked(call.key, Record[V]{
			Value:        loaded.Value,
			Version:      loaded.Version,
			Generation:   call.generation,
			Fresh:        true,
			LastAccessed: time.Now(),
		})
	}
	call.loaded = loaded
	call.err = loadErr
	close(call.done)
	c.mu.Unlock()
	c.emit(evictions)
}

func (c *Cache[K, V]) commitLocked(key K, incoming Record[V]) ([]Eviction[K, V], error) {
	if current, ok := c.storage.Load(key); ok && current.Version > incoming.Version {
		c.stats.RejectedStaleLoads++
		return nil, ErrStaleVersion
	}
	c.storage.Save(key, incoming)
	return c.enforceCapacityLocked(), nil
}

func (c *Cache[K, V]) enforceCapacityLocked() []Eviction[K, V] {
	if c.capacity <= 0 {
		return nil
	}
	var evictions []Eviction[K, V]
	for c.storage.Len() > c.capacity {
		key, record, ok := c.storage.Oldest()
		if !ok {
			break
		}
		c.storage.Remove(key)
		c.stats.Evictions++
		evictions = append(evictions, Eviction[K, V]{Key: key, Value: record.Value, Reason: EvictionCapacity})
	}
	return evictions
}

func (c *Cache[K, V]) await(ctx context.Context, call *loadCall[K, V]) (V, uint64, error) {
	select {
	case <-call.done:
		if call.err != nil {
			var zero V
			return zero, 0, call.err
		}
		return call.loaded.Value, call.loaded.Version, nil
	case <-ctx.Done():
		c.mu.Lock()
		c.stats.CanceledWaiters++
		c.mu.Unlock()
		var zero V
		return zero, 0, ctx.Err()
	}
}

func (c *Cache[K, V]) emit(evictions []Eviction[K, V]) {
	if c.onEviction == nil {
		return
	}
	for _, eviction := range evictions {
		c.onEviction(eviction)
	}
}
