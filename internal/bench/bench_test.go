package bench

// bench_test.go tests the load generator's building blocks, so that the
// benchmark numbers in the README can be trusted: a benchmark tool with a
// bug in its key distribution or percentile maths produces confident,
// wrong numbers, which is worse than no numbers.
//
// The tests are in package bench (not bench_test) so they can call
// unexported functions like summarize.

import (
	"context"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"distkv/internal/testcluster"
)

// TestZipfIsSkewed proves that the Zipf generator really produces a
// heavy-headed distribution and stays in range.
//
// Why it matters: the cache test (Test B) is only meaningful if "zipf" keys
// are genuinely skewed. A subtle bug in the Gray et al. formula (e.g. a wrong
// η) could silently produce an almost-uniform distribution, making the cache
// look useless. The test checks four properties:
//  1. every draw is a valid index in [0, 100000);
//  2. the top 100 ranks (0.1% of keys) get at least 30% of 500,000 draws,
//     whereas uniform keys would give them ~0.1%;
//  3. frequency decreases with rank (rank 0 > rank 10 > rank 1000);
//  4. invalid θ (≥ 1) is rejected instead of producing garbage.
//
// A fixed PCG seed makes the draws, and therefore the test, deterministic.
func TestZipfIsSkewed(t *testing.T) {
	z, err := NewZipf(100000, 0.99)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(1, 2))
	counts := make([]int, 100000)
	const draws = 500000
	for i := 0; i < draws; i++ {
		k := z.Next(r)
		if k < 0 || k >= 100000 {
			t.Fatalf("out of range: %d", k)
		}
		counts[k]++
	}
	top100 := 0
	for i := 0; i < 100; i++ {
		top100 += counts[i]
	}
	frac := float64(top100) / draws
	t.Logf("top 100 of 100k keys receive %.1f%% of requests", frac*100)
	// Uniform would give 0.1%; theta=0.99 concentrates far more.
	if frac < 0.3 {
		t.Fatalf("zipf not skewed enough: %.3f", frac)
	}
	if counts[0] <= counts[10] || counts[10] <= counts[1000] {
		t.Fatal("rank frequencies should decrease")
	}
	if _, err := NewZipf(10, 1.5); err == nil {
		t.Fatal("theta >= 1 should be rejected")
	}
}

// TestSummarizePercentiles proves that summarize computes exact nearest-rank
// percentiles and does not depend on input order.
//
// With samples 1..1000 µs the nearest-rank answers are known exactly:
// p50 = 500, p95 = 950, p99 = 990, max = 1000. This also pins down the
// floating-point "ceil" subtlety in summarize: 1000×0.95 must select rank
// 950, not 951. Swapping the first and last sample makes the input unsorted,
// so the test fails if summarize forgot to sort. Finally, an empty input must
// yield an empty summary rather than panic with an index out of range.
func TestSummarizePercentiles(t *testing.T) {
	var s []time.Duration
	for i := 1; i <= 1000; i++ {
		s = append(s, time.Duration(i)*time.Microsecond)
	}
	// Shuffle-independent.
	s[0], s[999] = s[999], s[0]
	l := summarize(s)
	if l.P50US != 500 || l.P95US != 950 || l.P99US != 990 || l.MaxUS != 1000 || l.Count != 1000 {
		t.Fatalf("summary %+v", l)
	}
	if e := summarize(nil); e.Count != 0 {
		t.Fatal("empty summary")
	}
}

// TestRebalance proves the three headline claims of the redistribution
// experiment on 20,000 keys:
//   - when growing 4 → 5 and shrinking 5 → 4, keys only move to added nodes or
//     from removed nodes (never between two surviving nodes);
//   - growing 4 → 5 moves under 30% of keys with consistent hashing (ideal
//     20%) but over 70% with modulo sharding (expected ~80%);
//   - 128 vnodes give a smaller load deviation than 1 vnode.
//
// The thresholds are loose on purpose: they catch a broken ring or a broken
// comparison without being flaky about exact hash-dependent percentages.
func TestRebalance(t *testing.T) {
	r := Rebalance(20000, 128, [][2]int{{4, 5}, {5, 4}}, []int{1, 128}, 4)
	add, remove := r.Transitions[0], r.Transitions[1]
	if !add.OnlyAffectedMoved || !remove.OnlyAffectedMoved {
		t.Fatal("keys moved between unaffected nodes")
	}
	if add.ConsistentMovedPct > 30 || add.ModuloMovedPct < 70 {
		t.Fatalf("add: %+v", add)
	}
	if r.Balance[1].StdDevPct >= r.Balance[0].StdDevPct {
		t.Fatalf("vnodes did not improve balance: %+v", r.Balance)
	}
}

// TestRunAgainstCluster runs a short benchmark end to end.
//
// It starts a real 3-node cluster inside the test process (testcluster talks
// real TCP over loopback), then runs Run with preload, Zipf keys, a timeline
// and a probe. It proves the plumbing, not the performance:
//   - exactly Requests (2000) operations run, split into GETs and PUTs, with
//     no errors: the shared request budget in runPhase is exact;
//   - no GET returns NotFound: the preload really wrote every key;
//   - the cache delta is available and shows hits: stats snapshots and
//     subtraction work, and Zipf keys produce cache hits;
//   - percentiles are positive and ordered (p99 >= p50);
//   - cluster size, timeline and probe samples are all populated;
//   - Print does not panic on a real result.
func TestRunAgainstCluster(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, CacheCapacity: 100})
	cfg := Config{
		Label: "test", Address: tc.Addr, Clients: 4, Requests: 2000, ReadRatio: 0.8,
		Keyspace: 500, ValueSize: 32, Distribution: "zipf", ZipfTheta: 0.99, Preload: true,
		TimelineInterval: 50 * time.Millisecond, ProbeKey: "probe", ProbeInterval: 5 * time.Millisecond,
	}
	res, err := Run(context.Background(), cfg, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if res.TotalOps != 2000 || res.Errors != 0 {
		t.Fatalf("ops=%d errors=%d %v", res.TotalOps, res.Errors, res.ErrorTypes)
	}
	if res.Get.Count+res.Put.Count != 2000 || res.Get.NotFound != 0 {
		t.Fatalf("get=%+v put=%+v", res.Get, res.Put)
	}
	if !res.Cache.Available || res.Cache.Hits == 0 {
		t.Fatalf("cache %+v", res.Cache)
	}
	if res.Latency.P50US <= 0 || res.Latency.P99US < res.Latency.P50US {
		t.Fatalf("latency %+v", res.Latency)
	}
	if res.ClusterNodes != 3 || len(res.Timeline) == 0 || len(res.Probe) == 0 {
		t.Fatalf("nodes=%d timeline=%d probe=%d", res.ClusterNodes, len(res.Timeline), len(res.Probe))
	}
	res.Print(io.Discard)
}
