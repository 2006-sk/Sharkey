// Tests for the version clock: uniqueness under concurrency, and behaviour
// when the wall clock stalls or a larger version is observed.
package clock

import (
	"sync"
	"testing"
)

// TestMonotonicUnderConcurrency proves the two guarantees of Next when 8
// goroutines call it 5000 times each on one Clock: no version is ever handed
// out twice (across all goroutines), and each goroutine sees strictly
// increasing versions. Uniqueness is what the CAS loop exists for; a racy
// load-then-store would produce duplicates here, and in the real system a
// duplicate version would let one of two writes silently lose under
// last-writer-wins. Each goroutine appends only to its own results slot,
// so the test itself needs no lock.
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

// TestStalledClock injects a wall clock frozen at 100 to prove the logical
// fallback: the first Next returns the wall time (100), the second cannot
// reuse it and returns 101. Then Observe(1000), as if a write stamped by a
// clock far ahead arrived, must pull the clock forward so the next version
// is 1001, sorting after the observed write. Building Clock by hand (not
// via New) is possible because the test lives in the same package and can
// set the unexported now field.
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
