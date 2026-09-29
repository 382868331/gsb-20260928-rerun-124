package cache

// Store is the pluggable storage strategy. Replacing the
// implementation does not change results observed by Cache callers.
type Store interface {
	Get(key string) (Entry, bool)
	Set(key string, e Entry)
	Delete(key string) (Entry, bool)
	Len() int
}

type mapStore struct{ items map[string]Entry }

// NewMapStore returns the default in-memory Store.
func NewMapStore() Store { return &mapStore{items: make(map[string]Entry)} }

func (s *mapStore) Get(key string) (Entry, bool) {
	e, ok := s.items[key]
	return e, ok
}

func (s *mapStore) Set(key string, e Entry) { s.items[key] = e }

func (s *mapStore) Delete(key string) (Entry, bool) {
	e, ok := s.items[key]
	delete(s.items, key)
	return e, ok
}

func (s *mapStore) Len() int { return len(s.items) }
