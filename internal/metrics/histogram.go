// Package metrics provides a lock-free latency histogram and the small HTTP
// admin server exposing /health and /stats.
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
type Histogram struct {
	counts [numBuckets]atomic.Int64
	total  atomic.Int64
	sumUS  atomic.Int64
	maxUS  atomic.Int64
}

const (
	subBits    = 5
	subBuckets = 1 << subBits // 32 linear buckets per power of two
	maxExp     = 40           // covers up to ~2^40 µs (~12 days)
	numBuckets = (maxExp + 1) * subBuckets
)

// bucketOf maps a value in microseconds to its bucket index.
func bucketOf(us int64) int {
	if us < subBuckets {
		return int(us) // exact for tiny values
	}
	exp := bits.Len64(uint64(us)) - 1 // us in [2^exp, 2^(exp+1))
	shift := exp - subBits
	sub := int(us>>uint(shift)) - subBuckets // top subBits bits below the leading 1
	idx := (exp-subBits+1)*subBuckets + sub
	if idx >= numBuckets {
		return numBuckets - 1
	}
	return idx
}

// bucketUpper returns the largest value (µs) that maps to bucket idx.
func bucketUpper(idx int) int64 {
	if idx < subBuckets {
		return int64(idx)
	}
	group := idx/subBuckets - 1 // exp - subBits
	sub := idx % subBuckets
	shift := uint(group)
	return (int64(subBuckets+sub+1) << shift) - 1
}

// Record adds one observation.
func (h *Histogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	h.counts[bucketOf(us)].Add(1)
	h.total.Add(1)
	h.sumUS.Add(us)
	for {
		m := h.maxUS.Load()
		if us <= m || h.maxUS.CompareAndSwap(m, us) {
			break
		}
	}
}

// Summary is a percentile snapshot in microseconds.
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
func (h *Histogram) Snapshot() Summary {
	var counts [numBuckets]int64
	var total int64
	for i := range counts {
		counts[i] = h.counts[i].Load()
		total += counts[i]
	}
	s := Summary{Count: total, MaxUS: h.maxUS.Load()}
	if total == 0 {
		return s
	}
	s.MeanUS = float64(h.sumUS.Load()) / float64(h.total.Load())
	pct := func(p float64) int64 {
		rank := int64(math.Ceil(p * float64(total)))
		var seen int64
		for i, c := range counts {
			seen += c
			if seen >= rank {
				return min(bucketUpper(i), s.MaxUS)
			}
		}
		return s.MaxUS
	}
	s.P50US, s.P95US, s.P99US = pct(0.50), pct(0.95), pct(0.99)
	return s
}
