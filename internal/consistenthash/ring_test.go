package consistenthash

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func nodeNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("node-%d:71%02d", i+1, i+1)
	}
	return out
}

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("key-%d", i)
	}
	return out
}

func TestEmptyRing(t *testing.T) {
	r := New(16)
	if _, ok := r.Owner("k"); ok {
		t.Fatal("empty ring should have no owner")
	}
	if got := r.Replicas("k", 3); got != nil {
		t.Fatalf("empty ring replicas = %v", got)
	}
}

func TestSingleNodeOwnsEverything(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add("a")
	for _, k := range keys(1000) {
		if o, _ := r.Owner(k); o != "a" {
			t.Fatalf("owner(%s) = %s", k, o)
		}
	}
}

func TestDeterministicRouting(t *testing.T) {
	r1, r2 := New(64), New(64)
	names := nodeNames(4)
	r1.Add(names...)
	// Insertion order must not matter.
	for i := len(names) - 1; i >= 0; i-- {
		r2.Add(names[i])
	}
	for _, k := range keys(5000) {
		a, _ := r1.Owner(k)
		b, _ := r2.Owner(k)
		if a != b {
			t.Fatalf("owner(%s) differs: %s vs %s", k, a, b)
		}
	}
}

func TestVirtualNodesBalanceLoad(t *testing.T) {
	// With 128 vnodes, every node's share of 100k keys should be close to 1/N.
	// With 1 vnode the imbalance is much worse; we assert vnodes help.
	ks := keys(100000)
	maxDeviation := func(vnodes, n int) float64 {
		r := New(vnodes)
		r.Add(nodeNames(n)...)
		counts := map[string]int{}
		for _, k := range ks {
			o, _ := r.Owner(k)
			counts[o]++
		}
		ideal := float64(len(ks)) / float64(n)
		worst := 0.0
		for _, name := range nodeNames(n) {
			worst = math.Max(worst, math.Abs(float64(counts[name])-ideal)/ideal)
		}
		return worst
	}
	for _, n := range []int{2, 4, 8} {
		with := maxDeviation(DefaultVirtualNodes, n)
		without := maxDeviation(1, n)
		t.Logf("n=%d max deviation from ideal: vnodes=128 %.1f%%, vnodes=1 %.1f%%", n, with*100, without*100)
		if with > 0.20 {
			t.Errorf("n=%d: 128 vnodes deviation %.1f%% exceeds 20%%", n, with*100)
		}
		if with >= without {
			t.Errorf("n=%d: vnodes did not improve balance (%.3f vs %.3f)", n, with, without)
		}
	}
}

// TestAddNodeMovesOnlyAFraction is the core consistent-hashing property: going
// from N to N+1 nodes moves about 1/(N+1) of keys, and every moved key moves
// to the new node. Modulo sharding, by contrast, moves about N/(N+1).
func TestAddNodeMovesOnlyAFraction(t *testing.T) {
	ks := keys(100000)
	for _, n := range []int{1, 2, 4, 8} {
		names := nodeNames(n + 1)
		r := New(DefaultVirtualNodes)
		r.Add(names[:n]...)
		before := make([]string, len(ks))
		for i, k := range ks {
			before[i], _ = r.Owner(k)
		}
		r.Add(names[n])

		moved, movedMod := 0, 0
		for i, k := range ks {
			after, _ := r.Owner(k)
			if after != before[i] {
				moved++
				if after != names[n] {
					t.Fatalf("key %s moved %s -> %s, but only the new node should gain keys", k, before[i], after)
				}
			}
			h := Hash(k)
			if h%uint64(n) != h%uint64(n+1) {
				movedMod++
			}
		}
		frac := float64(moved) / float64(len(ks))
		fracMod := float64(movedMod) / float64(len(ks))
		ideal := 1 / float64(n+1)
		t.Logf("%d -> %d nodes: consistent hashing moved %.2f%% (ideal %.2f%%), modulo moved %.2f%%",
			n, n+1, frac*100, ideal*100, fracMod*100)
		if math.Abs(frac-ideal) > 0.08 {
			t.Errorf("%d->%d: moved %.3f, want about %.3f", n, n+1, frac, ideal)
		}
		if n >= 2 && fracMod < 1.5*frac {
			t.Errorf("%d->%d: expected modulo (%.3f) to move far more keys than the ring (%.3f)", n, n+1, fracMod, frac)
		}
	}
}

func TestRemoveNodeMovesOnlyItsKeys(t *testing.T) {
	ks := keys(50000)
	names := nodeNames(5)
	r := New(DefaultVirtualNodes)
	r.Add(names...)
	before := make([]string, len(ks))
	for i, k := range ks {
		before[i], _ = r.Owner(k)
	}
	victim := names[2]
	r.Remove(victim)
	for i, k := range ks {
		after, _ := r.Owner(k)
		if before[i] != victim && after != before[i] {
			t.Fatalf("key %s owned by surviving node %s moved to %s", k, before[i], after)
		}
		if after == victim {
			t.Fatalf("key %s still routed to removed node", k)
		}
	}
	if r.Len() != 4 {
		t.Fatalf("len = %d", r.Len())
	}
}

func TestReplicasDistinctAndOrdered(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add(nodeNames(4)...)
	for _, k := range keys(10000) {
		reps := r.Replicas(k, 3)
		if len(reps) != 3 {
			t.Fatalf("replicas(%s) = %v", k, reps)
		}
		seen := map[string]bool{}
		for _, n := range reps {
			if seen[n] {
				t.Fatalf("duplicate physical node in replicas(%s) = %v", k, reps)
			}
			seen[n] = true
		}
		if o, _ := r.Owner(k); reps[0] != o {
			t.Fatalf("first replica %s != owner %s", reps[0], o)
		}
	}
}

func TestReplicasCappedByNodeCount(t *testing.T) {
	// Two physical nodes with 128 vnodes each and RF=3: virtual nodes must not
	// be counted as extra replicas, so effective RF is 2.
	r := New(DefaultVirtualNodes)
	r.Add(nodeNames(2)...)
	for _, k := range keys(1000) {
		if reps := r.Replicas(k, 3); len(reps) != 2 || reps[0] == reps[1] {
			t.Fatalf("replicas(%s) = %v, want 2 distinct nodes", k, reps)
		}
	}
	r1 := New(DefaultVirtualNodes)
	r1.Add("solo")
	if reps := r1.Replicas("k", 3); len(reps) != 1 {
		t.Fatalf("single node replicas = %v", reps)
	}
}

func TestReplicaSetStableUnderUnrelatedChange(t *testing.T) {
	// Removing a node only changes replica sets that contained it.
	r := New(DefaultVirtualNodes)
	names := nodeNames(6)
	r.Add(names...)
	ks := keys(5000)
	before := make([][]string, len(ks))
	for i, k := range ks {
		before[i] = r.Replicas(k, 3)
	}
	r.Remove(names[0])
	for i, k := range ks {
		if contains(before[i], names[0]) {
			continue
		}
		after := r.Replicas(k, 3)
		if fmt.Sprint(after) != fmt.Sprint(before[i]) {
			t.Fatalf("replicas(%s) changed %v -> %v though removed node wasn't in it", k, before[i], after)
		}
	}
}

func TestSharesSumToOne(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add(nodeNames(4)...)
	sum := 0.0
	for _, s := range r.Shares() {
		sum += s
	}
	if math.Abs(sum-1) > 1e-9 {
		t.Fatalf("shares sum to %f", sum)
	}
}

func TestConcurrentLookupsAndMembershipChanges(t *testing.T) {
	r := New(32)
	r.Add(nodeNames(4)...)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("k%d", j)
				r.Owner(k)
				r.Replicas(k, 3)
			}
		}()
	}
	for i := 0; i < 50; i++ {
		r.Add("extra")
		r.Remove("extra")
	}
	close(stop)
	wg.Wait()
}

func BenchmarkReplicas(b *testing.B) {
	r := New(DefaultVirtualNodes)
	r.Add(nodeNames(8)...)
	ks := keys(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Replicas(ks[i%len(ks)], 3)
	}
}
