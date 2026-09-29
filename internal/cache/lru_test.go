package cache

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

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

func TestCapacityOne(t *testing.T) {
	c := New(1)
	c.Put("a", nil, 0)
	c.Put("b", nil, 0)
	if want := []string{"b"}; !reflect.DeepEqual(c.Keys(), want) {
		t.Fatalf("keys = %v", c.Keys())
	}
}

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
