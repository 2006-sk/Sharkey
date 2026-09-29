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
//
// # What LRU means and why these two structures
//
// A cache has a fixed capacity; when it is full and a new key arrives, some
// entry must go. "Least recently used" evicts the entry that has gone
// longest without being read or written, betting that recently used keys
// are the ones most likely to be used again (temporal locality, which a
// skewed workload such as the README's Zipf benchmark has plenty of).
//
// To do that in O(1) we need two things at once:
//
//  1. Find an entry by key in O(1): a hash map.
//  2. Know the recency order, and change it in O(1) when an entry is used:
//     a doubly linked list. Moving a node to the front is a few pointer
//     writes, and the least recently used entry is always at the back.
//
// Neither alone is enough. A map has no order. A list alone needs an O(n)
// walk to find a key. A slice or heap ordered by timestamp would need
// O(n) shifting or O(log n) re-heaping on every Get. The map stores
// pointers to list nodes, so after the O(1) lookup we can splice that node
// in O(1). The list must be doubly linked because unlinking a node requires
// its predecessor, and a singly linked list would need a walk to find it.
//
// # The sentinel
//
// Instead of head and tail pointers that can be nil, the list has one dummy
// node, root, that is always present and links the list into a circle:
//
//	          +--------------------------------------------------+
//	          v                                                  |
//	      [ root ] <-> [ MRU: k3 ] <-> [ k1 ] <-> [ LRU: k7 ] <---+
//	          |                                        ^
//	          +------------ root.prev -----------------+
//
//	root.next = most recently used, root.prev = least recently used.
//	Empty list: root.next == root.prev == &root.
//
// Every real node therefore always has a non-nil prev and next, so insert
// and unlink are the same few pointer writes whether the list is empty,
// has one element, or the node is at either end. No special cases means no
// "forgot to update tail" bugs. (Go's container/list uses the same trick;
// this package writes it out by hand, typed for this use and without the
// interface{} boxing that container/list requires.)
//
// # Concurrency
//
// A single sync.Mutex guards the map and the list. See Cache for why an
// RWMutex would not help. The hit, miss and eviction counters are atomics so
// that reading statistics does not contend with the data path for them.
package cache

import (
	"sync"
	"sync/atomic"
)

// DefaultCapacity is the default number of entries per node. The README
// lists it as the node default (--cache-capacity 10,000; 0 disables the
// cache at the node level, since New itself rejects a zero capacity).
const DefaultCapacity = 10000

// entry is one cached key, and simultaneously a node of the recency list.
// Embedding the list links in the entry itself (an "intrusive" list) means
// one allocation per cached key instead of two (entry + separate list
// node), and the map can point straight at the node that must be spliced.
//
// Fields:
//
//   - key is kept in the node so that eviction, which starts from the list
//     (root.prev), can also delete the matching map entry.
//   - value is the cached bytes; it is shared with callers, not copied.
//   - version is the write's last-writer-wins version, returned with the
//     value so a cached read reports the same version as a storage read.
//   - prev and next link the node into the circular list. They are nil only
//     while the node is not in the list (after unlink).
type entry struct {
	key        string
	value      []byte
	version    uint64
	prev, next *entry
}

// Cache is a fixed-capacity LRU cache. All methods are safe for concurrent
// use. A single mutex guards the map and list: even Get mutates the list
// (move-to-front), so a RWMutex would not allow concurrent readers.
//
// More precisely: an RWMutex lets readers share the lock only if they do not
// write shared state. Get usually rewrites list pointers, so it would
// have to take the write lock anyway, and an RWMutex taken only in write
// mode is just a slower Mutex. The critical sections are a map lookup plus
// a few pointer writes (nanoseconds), so one lock per cache is cheap. If it
// ever became a bottleneck, the next step would be lock striping: several
// independent LRUs, each with its own lock, selected by hash(key) (the same
// idea storage uses with its 64 shards), at the cost of the LRU order being
// only per stripe rather than global.
//
// Fields:
//
//   - mu guards items and the list rooted at root (including every
//     entry's fields). capacity is immutable after New.
//   - capacity is the maximum number of entries.
//   - items maps key -> list node, the O(1) index into the list.
//   - root is the sentinel. It is a value, not a pointer, so it lives inside
//     the Cache allocation; the list links point at &c.root. A Cache must
//     therefore never be copied after New (the copy's links would still
//     point at the original's root), which is why New returns *Cache.
//   - hits, misses, evictions are atomic counters. They are updated outside
//     (or independently of) mu, and Stats reads them without taking mu.
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
//
// A zero or negative capacity is a programming error, so it panics instead
// of returning an error. With capacity 0, Put would try to evict from an
// empty list and root.prev would be the sentinel itself. Callers that want
// "no cache" (the README's --cache-capacity 0) simply do not create one.
func New(capacity int) *Cache {
	if capacity <= 0 {
		panic("cache: capacity must be positive")
	}
	// Pre-size the map to capacity so it never has to grow (rehash) while
	// the cache fills up; it will never hold more than capacity keys.
	c := &Cache{capacity: capacity, items: make(map[string]*entry, capacity)}
	// Empty circular list: the sentinel points to itself both ways. This is
	// the invariant that lets pushFront and unlink skip all nil checks.
	c.root.next = &c.root
	c.root.prev = &c.root
	return c
}

// unlink removes e from the list in O(1). Caller holds c.mu.
//
//	before:  [p] <-> [e] <-> [n]
//	after:   [p] <-------> [n]        e.prev = e.next = nil
//
// Thanks to the sentinel, p and n always exist (they may be root), so no
// nil checks are needed.
func (c *Cache) unlink(e *entry) {
	// The predecessor now skips forward over e...
	e.prev.next = e.next
	// ...and the successor skips back over e.
	e.next.prev = e.prev
	// Clear e's own links. Not needed for correctness of the list, but it
	// drops references to neighbours (helping the GC if e is discarded) and
	// makes a use-after-unlink bug fail fast with a nil dereference instead
	// of silently corrupting the list.
	e.prev, e.next = nil, nil
}

// pushFront inserts e right after the sentinel, making it the most recently
// used entry, in O(1). Caller holds c.mu; e must not currently be in the list.
//
//	before:  [root] <-> [old front] ...
//	after:   [root] <-> [e] <-> [old front] ...
//
// On an empty list, "old front" is root itself, and the same four writes
// produce root <-> e <-> root, making e both MRU and LRU.
func (c *Cache) pushFront(e *entry) {
	// Set e's own links first, while c.root.next still names the old front.
	e.prev = &c.root
	e.next = c.root.next
	// Then point the old front's back-link at e...
	c.root.next.prev = e
	// ...and finally make root's forward link point at e. The order matters:
	// overwriting c.root.next first would lose the old front.
	c.root.next = e
}

// moveToFront marks e as most recently used: unlink then push, O(1).
// Caller holds c.mu.
func (c *Cache) moveToFront(e *entry) {
	// Fast path for a repeatedly read hot key: it is already the MRU, so
	// skip the eight pointer writes of unlink+pushFront.
	if c.root.next == e {
		return
	}
	c.unlink(e)
	c.pushFront(e)
}

// Get returns the cached value and its version, marking it most recently
// used. The returned slice must not be modified by the caller.
//
// The slice is returned without copying (a copy per hit would cost an
// allocation). That is safe only because nobody mutates a cached slice in
// place: Put/Update replace e.value with a new slice rather than writing
// into the old one, so a reader holding the old slice still sees a
// consistent (if outdated) value.
func (c *Cache) Get(key string) ([]byte, uint64, bool) {
	// Not deferring the unlock: the lock is released by hand on both paths
	// so that the atomic counter updates happen outside the critical
	// section, keeping it as short as possible.
	c.mu.Lock()
	// O(1) average: hash map lookup.
	e, ok := c.items[key]
	if !ok {
		c.mu.Unlock()
		// Atomic increment; no lock needed. Concurrent misses from many
		// goroutines are counted correctly without contending on mu.
		c.misses.Add(1)
		return nil, 0, false
	}
	// A read counts as a "use": this is the write that makes Get need the
	// exclusive lock (see Cache).
	c.moveToFront(e)
	// Copy the fields out while still holding the lock. After Unlock another
	// goroutine may Put a new value into e, so reading e.value after the
	// unlock would be a data race.
	v, ver := e.value, e.version
	c.mu.Unlock()
	c.hits.Add(1)
	return v, ver, true
}

// Put inserts or updates key, evicting the least recently used entry if the
// cache is full. The cache keeps a reference to value; callers must not
// mutate it afterwards.
//
// All steps are O(1): one map lookup, at most one eviction (unlink + map
// delete), one allocation, one map insert, one pushFront.
func (c *Cache) Put(key string, value []byte, version uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Case 1: key already cached. Replace the value in place and refresh its
	// recency. The size does not change, so nothing is evicted.
	if e, ok := c.items[key]; ok {
		e.value, e.version = value, version
		c.moveToFront(e)
		return
	}
	// Case 2: new key and the cache is full. Evict first so the size never
	// exceeds capacity. The LRU entry is always root.prev (the back of the
	// list). Because capacity >= 1 and the cache is full, root.prev is a real
	// entry here, never the sentinel.
	if len(c.items) >= c.capacity {
		lru := c.root.prev
		c.unlink(lru)
		// The entry stores its own key precisely so that this map delete is
		// possible when eviction starts from the list side.
		delete(c.items, lru.key)
		c.evictions.Add(1)
	}
	// Insert the new entry as most recently used, in both structures. The
	// map and list must always contain exactly the same set of entries;
	// TestConcurrentAccess checks that invariant after a concurrent storm.
	e := &entry{key: key, value: value, version: version}
	c.items[key] = e
	c.pushFront(e)
}

// Update replaces the value of key only if it is already cached, and reports
// whether it was. Writes use this instead of Put so that replicas receiving
// write traffic don't fill their caches with keys nobody reads from them.
//
// Background (README, "Caching"): every write goes to all N=3 replicas, but
// reads normally go only to the primary. If writes populated the cache, the
// two non-primary replicas would fill theirs with keys they never serve,
// wasting memory and evicting entries that are useful. Update keeps an
// already cached copy fresh (so the primary never serves a stale value)
// without allocating new entries. Reads populate the cache on a miss via Put.
func (c *Cache) Update(key string, value []byte, version uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return false
	}
	e.value, e.version = value, version
	// A write is also a use; refresh recency.
	c.moveToFront(e)
	return true
}

// Delete removes key, reporting whether it was present. The node calls it to
// invalidate a key when a delete (tombstone) is applied, so a later Get
// misses and falls through to storage, which knows the key is gone.
func (c *Cache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return false
	}
	// Remove from both structures to keep them in sync. The entry becomes
	// unreachable and is reclaimed by the garbage collector.
	c.unlink(e)
	delete(c.items, key)
	return true
}

// Len returns the number of cached entries. It takes the lock because Go
// maps are not safe to read (even len) while another goroutine writes them.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Keys returns keys from most to least recently used (for tests/debugging).
// It walks the list from root.next until it arrives back at the sentinel,
// the standard way to traverse a circular list: there is no nil to stop at.
// O(n), so not for the hot path.
func (c *Cache) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.items))
	for e := c.root.next; e != &c.root; e = e.next {
		out = append(out, e.key)
	}
	return out
}

// Stats is a snapshot of cache counters. The JSON tags are the field names
// that appear in a node's /stats document (the coordinator sums them).
// HitRate is Hits / (Hits + Misses), or 0 before any lookup.
type Stats struct {
	Capacity  int     `json:"capacity"`
	Size      int     `json:"size"`
	Hits      int64   `json:"hits"`
	Misses    int64   `json:"misses"`
	Evictions int64   `json:"evictions"`
	HitRate   float64 `json:"hit_rate"`
}

// Stats returns a snapshot of the counters.
//
// The snapshot is not atomic as a whole: each counter is read separately, so
// under load, Hits might include a Get whose effect on Size was not yet
// visible, for example. For monitoring that is fine; the numbers are
// individually exact and at most a few operations apart.
func (c *Cache) Stats() Stats {
	// capacity never changes after New, so it is read without the lock.
	// Len takes the mutex, briefly, because it reads the map. The three
	// counters are atomic loads: no lock, never blocking the data path.
	s := Stats{
		Capacity:  c.capacity,
		Size:      c.Len(),
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Evictions: c.evictions.Load(),
	}
	// Guard the division: before any lookup, total is 0 and 0/0 would be NaN,
	// which encoding/json refuses to encode.
	if total := s.Hits + s.Misses; total > 0 {
		s.HitRate = float64(s.Hits) / float64(total)
	}
	return s
}
