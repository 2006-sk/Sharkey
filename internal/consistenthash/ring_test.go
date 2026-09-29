// Tests for the consistent-hash ring. Each test pins down one property the
// rest of the system relies on: routing is deterministic, load is balanced,
// membership changes move only the keys they must, replica sets are made of
// distinct physical nodes, and the ring is safe under concurrent use.
package consistenthash

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

// nodeNames returns n realistic node names ("node-1:7101", ...), shaped like
// the host:port addresses the coordinator puts on the ring. Using similar,
// short names on purpose exercises the case FNV-1a alone handles badly (see
// Hash), so a weak finaliser would show up as poor balance in these tests.
func nodeNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("node-%d:71%02d", i+1, i+1)
	}
	return out
}

// keys returns n distinct, deterministic keys ("key-0", "key-1", ...), so
// every run of the tests sees exactly the same key population.
func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("key-%d", i)
	}
	return out
}

// TestEmptyRing proves the edge case of a ring with no members: Owner must
// report "no owner" and Replicas must return nil, instead of panicking on an
// index into an empty slice. A coordinator that has not yet registered any
// node must fail requests cleanly, not crash.
func TestEmptyRing(t *testing.T) {
	r := New(16)
	if _, ok := r.Owner("k"); ok {
		t.Fatal("empty ring should have no owner")
	}
	if got := r.Replicas("k", 3); got != nil {
		t.Fatalf("empty ring replicas = %v", got)
	}
}

// TestSingleNodeOwnsEverything proves that with one member every key maps to
// it, including keys that hash above its largest point. Those keys only
// route correctly thanks to the wrap-around in search, so this is also a
// test of the wrap.
func TestSingleNodeOwnsEverything(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add("a")
	for _, k := range keys(1000) {
		if o, _ := r.Owner(k); o != "a" {
			t.Fatalf("owner(%s) = %s", k, o)
		}
	}
}

// TestDeterministicRouting proves that two rings with the same members agree
// on every key's owner, whatever order the members were added in. This is
// what lets independent processes (or a restarted coordinator) route
// consistently with no shared state: placement is a pure function of the
// membership. It relies on Hash being deterministic and on the sort's
// tie-break by node name.
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

// TestVirtualNodesBalanceLoad proves why virtual nodes exist. For 2, 4 and 8
// nodes it routes 100k keys and measures the worst node's relative
// deviation from the fair share len(keys)/N. With 128 vnodes that deviation
// must stay under 20% and must be smaller than with a single point per node.
// (The README's measured numbers: 1.04x the ideal with 128 vnodes versus
// 1.44x with 1 vnode, at 4 nodes.) Balance matters because the busiest node
// sets the cluster's capacity: it saturates first.
func TestVirtualNodesBalanceLoad(t *testing.T) {
	// With 128 vnodes, every node's share of 100k keys should be close to 1/N.
	// With 1 vnode the imbalance is much worse; we assert vnodes help.
	ks := keys(100000)
	// maxDeviation builds a ring, counts keys per owner, and returns the
	// largest |count - ideal| / ideal over all nodes (0 = perfect balance).
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
		// Iterate over the expected names (not the counts map) so that a
		// node that received zero keys still counts, as a 100% deviation.
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
//
// Two claims are checked. (1) Minimal movement: the moved fraction is within
// 8 percentage points of 1/(N+1). (2) Moves only go to the new node: no key
// is shuffled between two old nodes, which would be pure wasted network
// transfer. For contrast it also counts how many keys hash%N would move, and
// asserts (for N >= 2) that modulo moves at least 1.5x as many. For N = 1,
// h%1 is always 0 and h%2 differs half the time, so both schemes move ~50%
// and the comparison is skipped.
func TestAddNodeMovesOnlyAFraction(t *testing.T) {
	ks := keys(100000)
	for _, n := range []int{1, 2, 4, 8} {
		names := nodeNames(n + 1)
		r := New(DefaultVirtualNodes)
		r.Add(names[:n]...)
		// Record every key's owner with N nodes...
		before := make([]string, len(ks))
		for i, k := range ks {
			before[i], _ = r.Owner(k)
		}
		// ...then add node N+1 and compare.
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
			// The same key under modulo sharding: does its shard change when
			// the divisor goes from N to N+1?
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

// TestRemoveNodeMovesOnlyItsKeys is the mirror image of the add test. When a
// node leaves, only the keys it owned get a new owner (each of its arcs is
// absorbed by the next point clockwise); keys on surviving nodes must not
// move at all, and nothing may still route to the removed node. In the real
// system this bounds the work after a failure or decommission to the
// departed node's share of the data.
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

// TestReplicasDistinctAndOrdered proves the two contracts of Replicas that
// replication depends on: the n nodes returned are distinct physical nodes
// (vnodes of an already chosen node are skipped, so three copies really are
// on three machines), and the first one is the key's Owner (the coordinator
// reads from the primary, so the preference list must start there).
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

// TestReplicasCappedByNodeCount proves that asking for more replicas than
// there are physical nodes returns every node once, never a padded list
// with repeats. The README states this rule: a 2-node cluster has an
// effective RF of 2. Returning duplicates would let one machine's two acks
// satisfy a write quorum, silently breaking the durability W promises.
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
	// The degenerate single-node cluster: RF collapses to 1.
	r1 := New(DefaultVirtualNodes)
	r1.Add("solo")
	if reps := r1.Replicas("k", 3); len(reps) != 1 {
		t.Fatalf("single node replicas = %v", reps)
	}
}

// TestReplicaSetStableUnderUnrelatedChange extends the "minimal movement"
// property from owners to whole replica sets: removing a node must leave
// every replica list that did not include it exactly unchanged (same nodes,
// same order). Otherwise an unrelated membership change would force
// resynchronisation of data that never needed to move.
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
		// Sets that contained the removed node legitimately change.
		if contains(before[i], names[0]) {
			continue
		}
		after := r.Replicas(k, 3)
		// fmt.Sprint gives a cheap order-sensitive comparison of two slices.
		if fmt.Sprint(after) != fmt.Sprint(before[i]) {
			t.Fatalf("replicas(%s) changed %v -> %v though removed node wasn't in it", k, before[i], after)
		}
	}
}

// TestSharesSumToOne checks the arc arithmetic in Shares: the arcs of all
// points must tile the whole ring exactly once, so the fractions sum to 1
// (up to floating-point rounding). A bug in the wrap-around for the first
// point, whose arc crosses 2^64 -> 0, would lose or double-count that arc
// and break the sum.
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

// TestConcurrentLookupsAndMembershipChanges hammers the ring with 8 reader
// goroutines doing Owner/Replicas while the main goroutine repeatedly adds
// and removes a node. It asserts nothing directly; its value comes from
// running under `go test -race`, where any unsynchronised access to points
// or nodes (for example a lookup reading the slice while Remove compacts it)
// is reported as a data race. It also shows lookups never panic mid-change.
func TestConcurrentLookupsAndMembershipChanges(t *testing.T) {
	r := New(32)
	r.Add(nodeNames(4)...)
	var wg sync.WaitGroup
	// Closing stop broadcasts "finish" to all readers at once.
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; ; j++ {
				// Non-blocking check for the stop signal, so the reader keeps
				// looping until the writer is done.
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

// BenchmarkReplicas measures the per-request routing cost on the hot path:
// hash the key, binary-search 8*128 = 1024 points, and walk clockwise until 3
// distinct nodes are found. Cycling through 1024 keys avoids measuring a
// single, perfectly cached lookup.
func BenchmarkReplicas(b *testing.B) {
	r := New(DefaultVirtualNodes)
	r.Add(nodeNames(8)...)
	ks := keys(1024)
	// Exclude the ring set-up above from the timing.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Replicas(ks[i%len(ks)], 3)
	}
}
