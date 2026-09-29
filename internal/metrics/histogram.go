// Package metrics provides a lock-free latency histogram and the small HTTP
// admin server exposing /health and /stats.
//
// The histogram answers "what are p50/p95/p99 latency?" for a server that
// runs indefinitely, in fixed memory, with a recording cost of a few atomic
// instructions and no lock. The admin server (http.go) exposes those
// numbers, a health check, and Go's pprof profiler over HTTP.
package metrics

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// Histogram records durations in log-linear buckets: each power-of-two range
// of microseconds is split into subBuckets linear buckets, so the relative
// error of any reported percentile is at most 1/subBuckets (~3%). Recording
// is a couple of atomic adds, cheap enough for every request on the hot path.
//
// The benchmark tool does NOT use this: it records every latency sample and
// computes exact percentiles. The histogram is for long-running servers,
// where keeping every sample would grow without bound.
// (Precisely: the benchmark's headline percentiles are exact. Its per-interval
// timeline, in internal/bench, does record into one Histogram per interval
// to get a cheap p99 and max for each point. The coordinator keeps one per
// operation type and reports their Summaries under "latency" in /stats.)
//
// # Why log-linear buckets
//
// Exact percentiles need every sample (memory grows forever). A histogram
// keeps only a count per value range. The question is how to choose ranges:
//
//   - Linear buckets (0-1ms, 1-2ms, ...) waste resolution: 1 ms is a huge
//     error at 50 µs and a negligible one at 5 s, and covering a wide range
//     needs a huge number of buckets.
//   - Pure log buckets (powers of two: 32-63, 64-127, ...) cover a vast range
//     in few buckets but are too coarse: the answer "between 4 and 8 ms" is
//     up to 2x off.
//   - Log-linear takes the best of both: a power-of-two range per group
//     (log), cut into 32 equal slices (linear). Every bucket's width is at
//     most 1/32 of its lower bound, so the relative error is at most ~3%
//     at any scale, microseconds or hours. (HdrHistogram uses the same
//     idea.)
//
// Layout with subBuckets = 32 (values in µs):
//
//	index        values covered          bucket width
//	0..31        0..31 (one each)        1   exact
//	32..63       32..63                  1   exact    [2^5, 2^6)
//	64..95       64..127                 2            [2^6, 2^7)
//	96..127      128..255                4            [2^7, 2^8)
//	...
//	(g+1)*32..   [2^(g+5), 2^(g+6))       2^g          group g
//	...
//	1280..1311   [2^44, 2^45)            2^39         last group (g = 39)
//
// So a value like 5000 µs (group 7, width 128) lands in the bucket covering
// 4992..5119, and would be reported as 5119: 2.4% high.
//
// # Concurrency
//
// Every field is an atomic, so Record never takes a lock and many request
// goroutines can record at once. Fields: counts is one counter per bucket;
// total is the number of samples; sumUS is their sum (for the mean); maxUS is
// the largest sample seen (exact, not bucketed). The zero value is an empty,
// ready-to-use histogram (the tests declare `var h Histogram`). It must not
// be copied after use (it contains atomics).
type Histogram struct {
	counts [numBuckets]atomic.Int64
	total  atomic.Int64
	sumUS  atomic.Int64
	maxUS  atomic.Int64
}

// Histogram geometry. subBits = 5 gives 32 linear sub-buckets per power of
// two, hence the ~3% (1/32) error bound. numBuckets = 41 * 32 = 1312
// counters, about 10 KiB of int64s per histogram.
//
// A precise note on range: indices 0..31 hold exact values and each of the
// remaining 40 groups of 32 covers one power of two, starting at [2^5, 2^6).
// The last group is therefore [2^44, 2^45), so values up to 2^45 - 1 µs
// (about a year) get their own bucket; the "~2^40 µs" in the comment below
// is a conservative lower bound. Anything larger is clamped into the last
// bucket by bucketOf, and Snapshot caps reported percentiles at the exact
// maximum anyway.
const (
	subBits    = 5
	subBuckets = 1 << subBits // 32 linear buckets per power of two
	maxExp     = 40           // covers up to ~2^40 µs (~12 days)
	numBuckets = (maxExp + 1) * subBuckets
)

// bucketOf maps a value in microseconds to its bucket index.
//
// The trick: take the position of the leading 1 bit (which power of two the
// value is in) and the next 5 bits after it (which of the 32 slices of that
// power of two). Worked example, us = 5000:
//
//	5000 = 0b1_00111_0001000          (13 bits long)
//	exp   = 12                        (leading 1 is bit 12: 4096 <= 5000 < 8192)
//	shift = 12 - 5 = 7                (drop the 7 lowest bits)
//	5000 >> 7 = 39 = 0b1_00111        (leading 1 + next 5 bits, always 32..63)
//	sub   = 39 - 32 = 7               (slice 7 of this power of two)
//	idx   = (12 - 5 + 1)*32 + 7 = 263
//
// Everything is shifts and a bits.Len64 (a single CPU instruction on most
// platforms), so there is no floating-point log and no loop: O(1).
func bucketOf(us int64) int {
	// Below 32 there are not yet 5 bits after the leading 1, so these values
	// get one bucket each; the index is the value itself.
	if us < subBuckets {
		return int(us) // exact for tiny values
	}
	// bits.Len64 is the number of bits needed to write us, so Len64 - 1 is
	// the index of the leading 1 bit, i.e. floor(log2(us)). us >= 32 here, so
	// exp >= 5.
	exp := bits.Len64(uint64(us)) - 1 // us in [2^exp, 2^(exp+1))
	// How many low bits to discard so that exactly the leading 1 and the
	// 5 bits after it remain. This is also log2 of the bucket width.
	shift := exp - subBits
	// us >> shift lies in [32, 64): the leading 1 contributes 32, and the
	// remaining 5 bits select the linear slice. Subtract 32 to get 0..31.
	sub := int(us>>uint(shift)) - subBuckets // top subBits bits below the leading 1
	// Group g = exp - 5 occupies indices (g+1)*32 .. (g+1)*32+31, placed right
	// after the 32 exact buckets (the +1 skips over them).
	idx := (exp-subBits+1)*subBuckets + sub
	// Clamp absurdly large values into the last bucket rather than index out
	// of range (which would panic inside Record on the request path).
	if idx >= numBuckets {
		return numBuckets - 1
	}
	return idx
}

// bucketUpper returns the largest value (µs) that maps to bucket idx.
//
// It is the inverse of bucketOf. For idx in group g (g = exp - 5) with slice
// sub, the bucket covers [(32+sub) << g, (32+sub+1) << g), so its largest
// member is ((32+sub+1) << g) - 1. Continuing the example: idx 263 gives
// g = 263/32 - 1 = 7, sub = 263 % 32 = 7, upper = (40 << 7) - 1 = 5119.
//
// Reporting the upper bound (not the midpoint) means a percentile is never
// under-reported: the true value is at most the reported one, and at most
// ~3% below it. For latency SLOs erring high is the safe direction.
func bucketUpper(idx int) int64 {
	// Exact buckets: the bucket holds only the value idx.
	if idx < subBuckets {
		return int64(idx)
	}
	// Integer division recovers the group, and the remainder the slice.
	group := idx/subBuckets - 1 // exp - subBits
	sub := idx % subBuckets
	// The bucket width is 2^group, so shifting by group scales back up.
	shift := uint(group)
	return (int64(subBuckets+sub+1) << shift) - 1
}

// Record adds one observation.
//
// Four atomic updates (bucket count, total, sum, and max via a CAS loop), no
// lock, no allocation: safe to call on every request from any goroutine.
func (h *Histogram) Record(d time.Duration) {
	// Microseconds truncates (e.g. 1.9 µs -> 1), so resolution below 1 µs is
	// dropped. That is fine for network request latencies.
	us := d.Microseconds()
	// A negative duration (should a caller ever pass one, e.g. by subtracting
	// wall-clock times) would give a negative bucket index and panic. Treat
	// it as zero instead.
	if us < 0 {
		us = 0
	}
	h.counts[bucketOf(us)].Add(1)
	h.total.Add(1)
	h.sumUS.Add(us)
	// Atomic "max": there is no atomic max instruction, so use a CAS loop.
	// Load the current max; if our value is not larger we are done. Otherwise
	// try to install it; the CAS fails only if another goroutine changed max
	// in between, in which case reload and compare again. Usually this loop
	// exits on the first comparison, because new maxima are rare.
	for {
		m := h.maxUS.Load()
		if us <= m || h.maxUS.CompareAndSwap(m, us) {
			break
		}
	}
}

// Summary is a percentile snapshot in microseconds. The JSON tags are the
// field names used when a Summary is encoded, e.g. in the coordinator's
// /stats "latency" section.
type Summary struct {
	Count  int64   `json:"count"`
	MeanUS float64 `json:"mean_us"`
	P50US  int64   `json:"p50_us"`
	P95US  int64   `json:"p95_us"`
	P99US  int64   `json:"p99_us"`
	MaxUS  int64   `json:"max_us"`
}

// Snapshot computes percentiles. Concurrent Records may be partially
// included; that is acceptable for monitoring.
//
// "Partially" concretely: Record's four updates are separate atomics, so a
// Snapshot taken mid-Record may see the bucket count but not the sum, or the
// max but not the count. Making them consistent would need a lock on the
// hot path, which is exactly what this design avoids.
//
// Cost: O(numBuckets) to copy the counts plus O(numBuckets) per percentile,
// a few thousand simple operations. Cheap for a /stats request.
func (h *Histogram) Snapshot() Summary {
	// Copy all counters first, into a local array (on the stack), and compute
	// every percentile from this copy. Otherwise concurrent Records could make
	// the cumulative counts disagree with total mid-computation, and the
	// percentile search could fail to reach its rank.
	var counts [numBuckets]int64
	var total int64
	for i := range counts {
		counts[i] = h.counts[i].Load()
		total += counts[i]
	}
	s := Summary{Count: total, MaxUS: h.maxUS.Load()}
	// No samples: all percentiles stay 0 (and we avoid dividing by zero).
	if total == 0 {
		return s
	}
	// Mean from the running sum. It uses the live h.total rather than the
	// copied total, so under concurrent Records it is approximate too.
	s.MeanUS = float64(h.sumUS.Load()) / float64(h.total.Load())
	// pct returns the p-quantile using the nearest-rank definition: the
	// smallest value v such that at least ceil(p * total) samples are <= v.
	// Example: total = 10000, p = 0.99 -> rank 9900; walk the buckets
	// accumulating counts until 9900 samples have been seen, and report that
	// bucket's upper bound.
	pct := func(p float64) int64 {
		rank := int64(math.Ceil(p * float64(total)))
		var seen int64
		for i, c := range counts {
			seen += c
			if seen >= rank {
				// The bucket's upper bound can exceed any real sample (e.g.
				// all samples are 5000 but the bucket reaches 5119). The
				// exact max is a tighter truthful bound, so report the
				// smaller of the two. (A p100 would thus be exact.)
				return min(bucketUpper(i), s.MaxUS)
			}
		}
		// Unreachable with a consistent copy (rank <= total), but return a
		// sane value rather than 0 if it ever happens.
		return s.MaxUS
	}
	s.P50US, s.P95US, s.P99US = pct(0.50), pct(0.95), pct(0.99)
	return s
}
