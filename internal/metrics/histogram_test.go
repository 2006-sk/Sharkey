// Tests for the log-linear latency histogram: bucket arithmetic, percentile
// accuracy and lock-free concurrent recording.
package metrics

import (
	"math"
	"sync"
	"testing"
	"time"
)

// TestBucketBoundsMonotonic checks the bucket arithmetic over 0 .. 2^20 µs
// (about one second, stepping by 7 so both even and odd values and many
// bucket interiors are hit). Two properties: every value is <= the upper
// bound of the bucket it lands in (so reported percentiles never
// under-state a sample), and bucket indices never decrease as values grow
// (so walking buckets in index order is walking values in sorted order,
// which the cumulative percentile search in Snapshot depends on).
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

// TestPercentilesWithinError feeds a known distribution, 1..10000 µs once
// each, whose true nearest-rank percentiles are exactly 5000, 9500 and 9900,
// and checks each reported percentile is within 4% of the truth: the ~3%
// (1/32) bucket-width bound plus a little slack. Count and max are tracked
// exactly, not bucketed, so they must match exactly.
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

// TestConcurrentRecord records 8 x 1000 samples from 8 goroutines at once.
// With atomics and no lock, no increment may be lost: Count must be exactly
// 8000, and the CAS loop for the maximum must settle on 999 even when
// goroutines race to raise it. Under `go test -race` it also proves Record
// has no unsynchronised access.
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
