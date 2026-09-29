package cacheruntime

import (
	"container/list"
	"time"
)

type memoryItem[K comparable, V any] struct {
	key    K
	record Record[V]
}

type MemoryStorage[K comparable, V any] struct {
	items *list.List
	index map[K]*list.Element
}

func NewMemoryStorage[K comparable, V any]() *MemoryStorage[K, V] {
	return &MemoryStorage[K, V]{
		items: list.New(),
		index: make(map[K]*list.Element),
	}
}

func (s *MemoryStorage[K, V]) Load(key K) (Record[V], bool) {
	element, ok := s.index[key]
	if !ok {
		return Record[V]{}, false
	}
	return element.Value.(*memoryItem[K, V]).record, true
}

func (s *MemoryStorage[K, V]) Save(key K, record Record[V]) {
	if element, ok := s.index[key]; ok {
		item := element.Value.(*memoryItem[K, V])
		item.record = record
		s.items.MoveToFront(element)
		return
	}
	element := s.items.PushFront(&memoryItem[K, V]{key: key, record: record})
	s.index[key] = element
}

func (s *MemoryStorage[K, V]) Remove(key K) (Record[V], bool) {
	element, ok := s.index[key]
	if !ok {
		return Record[V]{}, false
	}
	s.items.Remove(element)
	delete(s.index, key)
	return element.Value.(*memoryItem[K, V]).record, true
}

func (s *MemoryStorage[K, V]) Touch(key K, now time.Time) bool {
	element, ok := s.index[key]
	if !ok {
		return false
	}
	item := element.Value.(*memoryItem[K, V])
	item.record.LastAccessed = now
	s.items.MoveToFront(element)
	return true
}

func (s *MemoryStorage[K, V]) Len() int {
	return s.items.Len()
}

func (s *MemoryStorage[K, V]) Oldest() (K, Record[V], bool) {
	element := s.items.Back()
	if element == nil {
		var zero K
		return zero, Record[V]{}, false
	}
	item := element.Value.(*memoryItem[K, V])
	return item.key, item.record, true
}
