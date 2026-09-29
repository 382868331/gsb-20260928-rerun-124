package cacheruntime

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoLoader     = errors.New("cacheruntime: loader is not configured")
	ErrClosed       = errors.New("cacheruntime: cache is closed")
	ErrStaleVersion = errors.New("cacheruntime: loaded version is older than cached version")
)

type Loader[K comparable, V any] func(ctx context.Context, key K) (Loaded[V], error)

type Loaded[V any] struct {
	Value   V
	Version uint64
}

type Record[V any] struct {
	Value        V
	Version      uint64
	Generation   uint64
	Fresh        bool
	LastAccessed time.Time
}

type Storage[K comparable, V any] interface {
	Load(key K) (Record[V], bool)
	Save(key K, record Record[V])
	Remove(key K) (Record[V], bool)
	Touch(key K, now time.Time) bool
	Len() int
	Oldest() (K, Record[V], bool)
}

type EvictionReason string

const (
	EvictionCapacity EvictionReason = "capacity"
	EvictionDeleted  EvictionReason = "deleted"
)

type Eviction[K comparable, V any] struct {
	Key    K
	Value  V
	Reason EvictionReason
}

type Stats struct {
	Hits               uint64
	StaleHits          uint64
	Misses             uint64
	CoalescedLoads     uint64
	StartedLoads       uint64
	CompletedLoads     uint64
	FailedLoads        uint64
	CanceledWaiters    uint64
	RejectedStaleLoads uint64
	Evictions          uint64
}

type Options[K comparable, V any] struct {
	Capacity   int
	Loader     Loader[K, V]
	Storage    Storage[K, V]
	OnEviction func(Eviction[K, V])
}

type GetOptions struct {
	AllowStaleOnRefresh bool
}

type loadCall[K comparable, V any] struct {
	key        K
	generation uint64
	done       chan struct{}
	loaded     Loaded[V]
	err        error
}
