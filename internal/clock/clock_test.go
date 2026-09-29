package clock

import (
	"sync"
	"testing"
)

func TestMonotonicUnderConcurrency(t *testing.T) {
	c := New()
	const goroutines, per = 8, 5000
	results := make([][]uint64, goroutines)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				results[g] = append(results[g], c.Next())
			}
		}(g)
	}
	wg.Wait()
	seen := make(map[uint64]bool, goroutines*per)
	for _, r := range results {
		for i, v := range r {
			if seen[v] {
				t.Fatalf("duplicate version %d", v)
			}
			seen[v] = true
			if i > 0 && v <= r[i-1] {
				t.Fatalf("non-increasing versions %d then %d", r[i-1], v)
			}
		}
	}
}

func TestStalledClock(t *testing.T) {
	c := &Clock{now: func() uint64 { return 100 }}
	if a, b := c.Next(), c.Next(); a != 100 || b != 101 {
		t.Fatalf("got %d %d", a, b)
	}
	c.Observe(1000)
	if v := c.Next(); v != 1001 {
		t.Fatalf("after observe got %d", v)
	}
}
