// Tests for the LRU cache: map/list consistency, eviction order, counters,
// and safety under concurrent use.
package cache

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

// TestGetPutBasic proves the basic map contract: a miss on an empty cache, a
// hit returning exactly the value and version stored, and that Put on an
// existing key overwrites it in place (Len stays 1) instead of adding a
// second entry. The version must round-trip because a cached read has to
// report the same last-writer-wins version as a storage read.
func TestGetPutBasic(t *testing.T) {
	c := New(2)
	if _, _, ok := c.Get("a"); ok {
		t.Fatal("empty cache hit")
	}
	c.Put("a", []byte("1"), 1)
	v, ver, ok := c.Get("a")
	if !ok || string(v) != "1" || ver != 1 {
		t.Fatalf("got %q %d %v", v, ver, ok)
	}
	c.Put("a", []byte("2"), 2)
	if v, ver, _ := c.Get("a"); string(v) != "2" || ver != 2 {
		t.Fatalf("update not visible: %q %d", v, ver)
	}
	if c.Len() != 1 {
		t.Fatalf("len = %d", c.Len())
	}
}

// TestEvictionOrder proves the "LR" in LRU: the entry evicted is the one
// used least recently, not the one inserted first (that would be FIFO).
// Reading "a" saves it; "b" goes instead. It then shows a Put on an existing
// key also counts as a use. Keys() exposes the list order (MRU first), so
// the test checks the linked list itself, not just membership. Evictions and
// Size confirm the counters agree with what happened.
//
//	after a,b,c:        c b a        (front = MRU)
//	Get(a):             a c b
//	Put(d) evicts b:    d a c
//	Put(c) refreshes:   c d a
//	Put(e) evicts a:    e c d
func TestEvictionOrder(t *testing.T) {
	c := New(3)
	c.Put("a", nil, 0)
	c.Put("b", nil, 0)
	c.Put("c", nil, 0)
	// Touch "a" so "b" becomes least recently used.
	c.Get("a")
	c.Put("d", nil, 0) // evicts b
	if _, _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if want := []string{"d", "a", "c"}; !reflect.DeepEqual(c.Keys(), want) {
		t.Fatalf("order = %v, want %v", c.Keys(), want)
	}
	// Updating an existing key refreshes recency without evicting.
	c.Put("c", nil, 0)
	c.Put("e", nil, 0) // evicts a (LRU after c was refreshed)
	if want := []string{"e", "c", "d"}; !reflect.DeepEqual(c.Keys(), want) {
		t.Fatalf("order = %v, want %v", c.Keys(), want)
	}
	if s := c.Stats(); s.Evictions != 2 || s.Size != 3 {
		t.Fatalf("stats = %+v", s)
	}
}

// TestDeleteInvalidates proves Delete removes a key from both the map and
// the list: it reports presence correctly (true, then false), the key then
// misses, and the list is still well-formed afterwards. The second half
// fills the cache past capacity after a delete. If unlink had left a
// dangling pointer, eviction or the Keys() walk would go wrong.
func TestDeleteInvalidates(t *testing.T) {
	c := New(2)
	c.Put("a", []byte("1"), 1)
	if !c.Delete("a") {
		t.Fatal("delete returned false for present key")
	}
	if c.Delete("a") {
		t.Fatal("second delete returned true")
	}
	if _, _, ok := c.Get("a"); ok {
		t.Fatal("deleted key still cached")
	}
	// List integrity after delete: fill to capacity and evict.
	c.Put("x", nil, 0)
	c.Put("y", nil, 0)
	c.Put("z", nil, 0)
	if want := []string{"z", "y"}; !reflect.DeepEqual(c.Keys(), want) {
		t.Fatalf("order = %v, want %v", c.Keys(), want)
	}
}

// TestStatsHitRate proves the counters and the derived hit rate: 3 hits and
// 1 miss must give exactly 0.75. The README reports hit rate from these
// counters (e.g. Test B's 90.9%), so the arithmetic must be right.
func TestStatsHitRate(t *testing.T) {
	c := New(10)
	c.Put("a", nil, 0)
	c.Get("a")
	c.Get("a")
	c.Get("a")
	c.Get("missing")
	s := c.Stats()
	if s.Hits != 3 || s.Misses != 1 || s.HitRate != 0.75 {
		t.Fatalf("stats = %+v", s)
	}
}

// TestCapacityOne covers the smallest legal cache, where one entry is both
// MRU and LRU (root.next == root.prev). Inserting a second key must evict
// the first. This is the edge case where pointer bugs in pushFront/unlink
// around the sentinel would show up first.
func TestCapacityOne(t *testing.T) {
	c := New(1)
	c.Put("a", nil, 0)
	c.Put("b", nil, 0)
	if want := []string{"b"}; !reflect.DeepEqual(c.Keys(), want) {
		t.Fatalf("keys = %v", c.Keys())
	}
}

// TestConcurrentAccess runs 16 goroutines doing a mix of Put, Get and Delete
// on 300 overlapping keys against a 100-entry cache, forcing constant
// eviction. Under `go test -race` it detects any access outside the mutex.
// Afterwards it checks two structural invariants: size never exceeds
// capacity, and the list holds exactly as many nodes as the map has keys.
// If a concurrent interleaving ever corrupted the pairing of map and list,
// these counts would disagree.
func TestConcurrentAccess(t *testing.T) {
	c := New(100)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := fmt.Sprintf("k%d", (i*7+g)%300)
				switch i % 3 {
				case 0:
					c.Put(k, []byte(k), uint64(i))
				case 1:
					c.Get(k)
				case 2:
					c.Delete(k)
				}
			}
		}(g)
	}
	wg.Wait()
	if n := c.Len(); n > 100 {
		t.Fatalf("len %d exceeds capacity", n)
	}
	if n := len(c.Keys()); n != c.Len() {
		t.Fatalf("list length %d != map length %d", n, c.Len())
	}
}

// BenchmarkGetHit measures the cost of a cache hit on a full cache: lock,
// map lookup, move-to-front (a no-op after the first iteration, since "k42"
// stays the MRU), unlock, atomic increment. It is the per-request price the
// README weighs against a storage lookup when explaining why the cache
// barely changes end-to-end latency.
func BenchmarkGetHit(b *testing.B) {
	c := New(DefaultCapacity)
	for i := 0; i < DefaultCapacity; i++ {
		c.Put(fmt.Sprintf("k%d", i), nil, 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("k42")
	}
}
