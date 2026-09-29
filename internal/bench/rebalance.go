package bench

// rebalance.go is the key-redistribution experiment (README Test E). It runs
// entirely in memory, with no cluster and no network: it builds hash rings,
// asks each one "who owns key k?" for 100,000 keys, changes the membership,
// asks again, and counts how many answers changed.
//
// # The question it answers
//
// When a node joins or leaves, every key whose owner changes must be copied
// across the network. The fewer keys move, the cheaper and faster scaling is.
//
//   - Modulo sharding, owner = hash(key) % N: changing N from 4 to 5 changes
//     the remainder for most keys. A key keeps its owner only if
//     h % 4 == h % 5, which holds for about 1 in 5 keys, so ~80% move
//     (README: 80.02%). In general, going from N to N+1 nodes moves about
//     N/(N+1) of all keys: almost everything, and more as the cluster grows.
//   - Consistent hashing: nodes and keys are hashed onto the same circle, and
//     a key belongs to the first node point clockwise from it. A new node
//     only takes over the arcs just before its own points, so only ~1/(N+1)
//     of the keys move, and ALL of them move to the new node (README: 21.46%
//     for 4 → 5, theory 20%).
//
// # Virtual nodes and balance
//
// With a single point per node, the arcs between points have very uneven
// lengths, so some nodes own far more keys than others. Giving each node many
// points ("virtual nodes", vnodes) spreads each node over many small arcs,
// and the law of large numbers evens out the totals. The Balance part of the
// experiment measures that: README shows the most-loaded of 4 nodes at 1.438×
// its fair share with 1 vnode, but 1.041× with 128 vnodes.

import (
	"fmt"
	"math"

	"distkv/internal/consistenthash"
)

// Transition is one membership change measured by the rebalance experiment.
//
// From/To are node counts before and after. ConsistentMoved(Pct) is the
// number (percentage) of keys whose owner changed on the consistent-hash
// ring; ModuloMoved(Pct) the same under hash % N, computed from the very
// same key hashes so the comparison is apples to apples.
type Transition struct {
	From int `json:"from_nodes"`
	To   int `json:"to_nodes"`

	ConsistentMoved    int     `json:"consistent_moved"`
	ConsistentMovedPct float64 `json:"consistent_moved_pct"`
	ModuloMoved        int     `json:"modulo_moved"`
	ModuloMovedPct     float64 `json:"modulo_moved_pct"`
	// IdealPct is the minimum possible: |to-from| / max(from,to).
	IdealPct float64 `json:"ideal_pct"`
	// OnlyAffectedMoved is true if, when adding, keys only moved TO new
	// nodes, and when removing, only keys FROM removed nodes moved.
	OnlyAffectedMoved bool `json:"only_affected_nodes_changed"`
}

// Balance describes the key distribution for one vnode setting.
//
// For VirtualNodes points per node on a ring of Nodes nodes: the fewest and
// most keys any node owns, how far the most-loaded node is above its fair
// share (1.0 = perfect), and the standard deviation of per-node load as a
// percentage of the fair share (the coefficient of variation). The maximum
// matters most in practice: the busiest node is the one that saturates or
// runs out of memory first.
type Balance struct {
	VirtualNodes int     `json:"virtual_nodes"`
	Nodes        int     `json:"nodes"`
	MinKeys      int     `json:"min_keys"`
	MaxKeys      int     `json:"max_keys"`
	MaxOverIdeal float64 `json:"max_over_ideal"` // max load / (keys/nodes)
	StdDevPct    float64 `json:"stddev_pct"`     // stddev of per-node load as % of ideal
}

// RebalanceResult is the output of the key-redistribution experiment.
//
// It is written as JSON by `bin/benchmark rebalance` (scripts/
// bench_rebalance.sh) and turned into the Test E tables and the key
// redistribution graph by scripts/summarize.py.
type RebalanceResult struct {
	Label        string       `json:"label"`
	Keys         int          `json:"keys"`
	VirtualNodes int          `json:"virtual_nodes"`
	Transitions  []Transition `json:"transitions"`
	Balance      []Balance    `json:"balance"`
	Host         HostInfo     `json:"host"`
}

// nodeName returns the name of node i (0-based), e.g. "node-1:7101". The
// names only need to be distinct and stable: the ring hashes them to place
// the node's virtual points. The number is embedded so indexOf can recover i
// from an owner name when checking which node a key moved to or from.
func nodeName(i int) string { return fmt.Sprintf("node-%d:%d", i+1, 7101+i) }

// names returns the names of nodes 0..n-1. Because names(n) is always a
// prefix of names(n+1), growing from `from` to `to` nodes means adding
// names(to)[from:], and shrinking means removing names(from)[to:]: the
// surviving nodes keep their identities (and ring positions).
func names(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = nodeName(i)
	}
	return out
}

// Rebalance measures how many of `keys` keys change owner for each
// membership transition, under consistent hashing (vnodes per node) and
// under modulo sharding (hash(key) % n), and how evenly keys spread for
// several vnode counts.
//
// Parameters: keys = how many keys (KeyName(0..keys-1)); vnodes = points per
// node for the transition experiment; transitions = list of {from, to} node
// counts; balanceVNodes = vnode settings to compare for balance;
// balanceNodes = cluster size used for the balance comparison.
//
// The code uses the SAME consistenthash package as the coordinator, so the
// numbers describe the real routing, not a re-implementation of it.
func Rebalance(keys, vnodes int, transitions [][2]int, balanceVNodes []int, balanceNodes int) *RebalanceResult {
	// Build the key names once and hash each once. The hashes feed the modulo
	// comparison; the ring hashes keys internally with the same function.
	ks := make([]string, keys)
	hashes := make([]uint64, keys)
	for i := range ks {
		ks[i] = KeyName(i)
		hashes[i] = consistenthash.Hash(ks[i])
	}
	// Host metadata is recorded for consistency with the load-test results,
	// even though this experiment's outcome does not depend on the machine.
	res := &RebalanceResult{Label: "rebalance", Keys: keys, VirtualNodes: vnodes, Host: hostInfo()}

	// Part 1: key movement for each membership transition.
	for _, tr := range transitions {
		from, to := tr[0], tr[1]
		// A fresh ring per transition, so transitions are independent of each
		// other and of their order in the list.
		ring := consistenthash.New(vnodes)
		ring.Add(names(from)...)
		// Record every key's owner under the old membership.
		before := make([]string, keys)
		for i, k := range ks {
			before[i], _ = ring.Owner(k)
		}
		// Grow or shrink membership: nodes are added/removed at the end.
		if to > from {
			ring.Add(names(to)[from:]...)
		} else {
			for _, n := range names(from)[to:] {
				ring.Remove(n)
			}
		}
		// Start by assuming the invariant holds; any violation clears it.
		t := Transition{From: from, To: to, OnlyAffectedMoved: true}
		// Compare every key's new owner with its old one.
		for i, k := range ks {
			after, _ := ring.Owner(k)
			if after != before[i] {
				t.ConsistentMoved++
				// Adding: a moved key must now live on a new node.
				// Removing: a moved key must have lived on a removed node.
				// Nodes with index < from are the old ones (when adding), and
				// nodes with index < to are the survivors (when removing). A key
				// moving between two such nodes would be unnecessary traffic,
				// which consistent hashing promises never happens.
				if (to > from && indexOf(after) < from) || (to < from && indexOf(before[i]) < to) {
					t.OnlyAffectedMoved = false
				}
			}
			// Modulo sharding: owner index = hash % N. The key moves iff its
			// remainder differs between the two cluster sizes.
			if hashes[i]%uint64(from) != hashes[i]%uint64(to) {
				t.ModuloMoved++
			}
		}
		// Convert counts to percentages of all keys. IdealPct is the lower
		// bound: adding k nodes to n, the new nodes must end up owning their
		// fair share k/(n+k); removing k of n, the removed nodes' share k/n
		// must go somewhere. Both equal |to−from| / max(from, to).
		t.ConsistentMovedPct = 100 * float64(t.ConsistentMoved) / float64(keys)
		t.ModuloMovedPct = 100 * float64(t.ModuloMoved) / float64(keys)
		t.IdealPct = 100 * math.Abs(float64(to-from)) / float64(max(from, to))
		res.Transitions = append(res.Transitions, t)
	}

	// Part 2: load balance for each vnode count, on a fixed-size cluster.
	for _, v := range balanceVNodes {
		ring := consistenthash.New(v)
		ring.Add(names(balanceNodes)...)
		// Count how many keys each node owns.
		counts := map[string]int{}
		for _, k := range ks {
			o, _ := ring.Owner(k)
			counts[o]++
		}
		// The fair share: what each node would own with perfect balance.
		ideal := float64(keys) / float64(balanceNodes)
		// MinKeys starts at the largest possible value so min() can lower it.
		b := Balance{VirtualNodes: v, Nodes: balanceNodes, MinKeys: keys}
		// Iterate over ALL node names (not the map), so a node that owns zero
		// keys still counts as a minimum of 0 and contributes to the deviation.
		var sq float64
		for _, n := range names(balanceNodes) {
			c := counts[n]
			b.MinKeys = min(b.MinKeys, c)
			b.MaxKeys = max(b.MaxKeys, c)
			// Sum of squared deviations from the fair share.
			sq += (float64(c) - ideal) * (float64(c) - ideal)
		}
		// Population standard deviation sqrt(Σ(c−ideal)²/N), expressed as a
		// percentage of the ideal so different key counts are comparable.
		b.MaxOverIdeal = float64(b.MaxKeys) / ideal
		b.StdDevPct = 100 * math.Sqrt(sq/float64(balanceNodes)) / ideal
		res.Balance = append(res.Balance, b)
	}
	return res
}

// indexOf recovers the node index from a name produced by nodeName.
//
// Sscanf parses "node-%d:%d" back into its numbers; the error is ignored
// because every name here was produced by nodeName. The port is parsed only
// to consume the rest of the string.
func indexOf(name string) int {
	var i, port int
	fmt.Sscanf(name, "node-%d:%d", &i, &port)
	return i - 1
}
