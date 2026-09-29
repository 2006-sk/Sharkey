// Package cache implements a thread-safe LRU cache with O(1) operations.
//
// Layout: a hash map from key to list element, plus a doubly linked list
// ordered by recency (front = most recently used, back = least recently
// used). A sentinel node makes the list circular so insert/unlink never need
// nil checks.
//
//	Get:    map lookup, then move element to front          O(1)
//	Put:    map lookup; update+move, or insert at front      O(1)
//	Evict:  unlink the element before the sentinel (back)    O(1)
package cache

import (
	"sync"
	"sync/atomic"
)

// DefaultCapacity is the default number of entries per node.
const DefaultCapacity = 10000

type entry struct {
	key        string
	value      []byte
	version    uint64
	prev, next *entry
}

// Cache is a fixed-capacity LRU cache. All methods are safe for concurrent
// use. A single mutex guards the map and list: even Get mutates the list
// (move-to-front), so a RWMutex would not allow concurrent readers.
type Cache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*entry
	root     entry // sentinel: root.next is MRU, root.prev is LRU

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
}

// New returns a cache holding at most capacity entries (capacity must be > 0).
func New(capacity int) *Cache {
	if capacity <= 0 {
		panic("cache: capacity must be positive")
	}
	c := &Cache{capacity: capacity, items: make(map[string]*entry, capacity)}
	c.root.next = &c.root
	c.root.prev = &c.root
	return c
}

func (c *Cache) unlink(e *entry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev, e.next = nil, nil
}

func (c *Cache) pushFront(e *entry) {
	e.prev = &c.root
	e.next = c.root.next
	c.root.next.prev = e
	c.root.next = e
}

func (c *Cache) moveToFront(e *entry) {
	if c.root.next == e {
		return
	}
	c.unlink(e)
	c.pushFront(e)
}

// Get returns the cached value and its version, marking it most recently
// used. The returned slice must not be modified by the caller.
func (c *Cache) Get(key string) ([]byte, uint64, bool) {
	c.mu.Lock()
	e, ok := c.items[key]
	if !ok {
		c.mu.Unlock()
		c.misses.Add(1)
		return nil, 0, false
	}
	c.moveToFront(e)
	v, ver := e.value, e.version
	c.mu.Unlock()
	c.hits.Add(1)
	return v, ver, true
}

// Put inserts or updates key, evicting the least recently used entry if the
// cache is full. The cache keeps a reference to value; callers must not
// mutate it afterwards.
func (c *Cache) Put(key string, value []byte, version uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		e.value, e.version = value, version
		c.moveToFront(e)
		return
	}
	if len(c.items) >= c.capacity {
		lru := c.root.prev
		c.unlink(lru)
		delete(c.items, lru.key)
		c.evictions.Add(1)
	}
	e := &entry{key: key, value: value, version: version}
	c.items[key] = e
	c.pushFront(e)
}

// Update replaces the value of key only if it is already cached, and reports
// whether it was. Writes use this instead of Put so that replicas receiving
// write traffic don't fill their caches with keys nobody reads from them.
func (c *Cache) Update(key string, value []byte, version uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return false
	}
	e.value, e.version = value, version
	c.moveToFront(e)
	return true
}

// Delete removes key, reporting whether it was present.
func (c *Cache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return false
	}
	c.unlink(e)
	delete(c.items, key)
	return true
}

// Len returns the number of cached entries.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Keys returns keys from most to least recently used (for tests/debugging).
func (c *Cache) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.items))
	for e := c.root.next; e != &c.root; e = e.next {
		out = append(out, e.key)
	}
	return out
}

// Stats is a snapshot of cache counters.
type Stats struct {
	Capacity  int     `json:"capacity"`
	Size      int     `json:"size"`
	Hits      int64   `json:"hits"`
	Misses    int64   `json:"misses"`
	Evictions int64   `json:"evictions"`
	HitRate   float64 `json:"hit_rate"`
}

// Stats returns a snapshot of the counters.
func (c *Cache) Stats() Stats {
	s := Stats{
		Capacity:  c.capacity,
		Size:      c.Len(),
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Evictions: c.evictions.Load(),
	}
	if total := s.Hits + s.Misses; total > 0 {
		s.HitRate = float64(s.Hits) / float64(total)
	}
	return s
}
