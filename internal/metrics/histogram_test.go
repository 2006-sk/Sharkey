package metrics

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestBucketBoundsMonotonic(t *testing.T) {
	prev := int64(-1)
	for us := int64(0); us < 1<<20; us += 7 {
		b := bucketOf(us)
		if up := bucketUpper(b); up < us {
			t.Fatalf("value %d in bucket %d with upper %d", us, b, up)
		}
		if int64(b) < prev {
			t.Fatalf("bucket index decreased at %d", us)
		}
		prev = int64(b)
	}
}

func TestPercentilesWithinError(t *testing.T) {
	var h Histogram
	// 1..10000 µs uniformly: p50≈5000, p95≈9500, p99≈9900.
	for i := 1; i <= 10000; i++ {
		h.Record(time.Duration(i) * time.Microsecond)
	}
	s := h.Snapshot()
	check := func(name string, got, want int64) {
		if rel := math.Abs(float64(got-want)) / float64(want); rel > 0.04 {
			t.Errorf("%s = %d, want ~%d (err %.1f%%)", name, got, want, rel*100)
		}
	}
	check("p50", s.P50US, 5000)
	check("p95", s.P95US, 9500)
	check("p99", s.P99US, 9900)
	if s.MaxUS != 10000 || s.Count != 10000 {
		t.Fatalf("summary %+v", s)
	}
}

func TestConcurrentRecord(t *testing.T) {
	var h Histogram
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				h.Record(time.Duration(i) * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	if s := h.Snapshot(); s.Count != 8000 || s.MaxUS != 999 {
		t.Fatalf("summary %+v", s)
	}
}
