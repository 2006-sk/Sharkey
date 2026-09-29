package bench

import (
	"context"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"distkv/internal/testcluster"
)

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
