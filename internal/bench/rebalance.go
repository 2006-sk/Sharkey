package bench

import (
	"fmt"
	"math"

	"distkv/internal/consistenthash"
)

// Transition is one membership change measured by the rebalance experiment.
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
type Balance struct {
	VirtualNodes int     `json:"virtual_nodes"`
	Nodes        int     `json:"nodes"`
	MinKeys      int     `json:"min_keys"`
	MaxKeys      int     `json:"max_keys"`
	MaxOverIdeal float64 `json:"max_over_ideal"` // max load / (keys/nodes)
	StdDevPct    float64 `json:"stddev_pct"`     // stddev of per-node load as % of ideal
}

// RebalanceResult is the output of the key-redistribution experiment.
type RebalanceResult struct {
	Label        string       `json:"label"`
	Keys         int          `json:"keys"`
	VirtualNodes int          `json:"virtual_nodes"`
	Transitions  []Transition `json:"transitions"`
	Balance      []Balance    `json:"balance"`
	Host         HostInfo     `json:"host"`
}

func nodeName(i int) string { return fmt.Sprintf("node-%d:%d", i+1, 7101+i) }

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
func Rebalance(keys, vnodes int, transitions [][2]int, balanceVNodes []int, balanceNodes int) *RebalanceResult {
	ks := make([]string, keys)
	hashes := make([]uint64, keys)
	for i := range ks {
		ks[i] = KeyName(i)
		hashes[i] = consistenthash.Hash(ks[i])
	}
	res := &RebalanceResult{Label: "rebalance", Keys: keys, VirtualNodes: vnodes, Host: hostInfo()}

	for _, tr := range transitions {
		from, to := tr[0], tr[1]
		ring := consistenthash.New(vnodes)
		ring.Add(names(from)...)
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
		t := Transition{From: from, To: to, OnlyAffectedMoved: true}
		for i, k := range ks {
			after, _ := ring.Owner(k)
			if after != before[i] {
				t.ConsistentMoved++
				// Adding: a moved key must now live on a new node.
				// Removing: a moved key must have lived on a removed node.
				if (to > from && indexOf(after) < from) || (to < from && indexOf(before[i]) < to) {
					t.OnlyAffectedMoved = false
				}
			}
			if hashes[i]%uint64(from) != hashes[i]%uint64(to) {
				t.ModuloMoved++
			}
		}
		t.ConsistentMovedPct = 100 * float64(t.ConsistentMoved) / float64(keys)
		t.ModuloMovedPct = 100 * float64(t.ModuloMoved) / float64(keys)
		t.IdealPct = 100 * math.Abs(float64(to-from)) / float64(max(from, to))
		res.Transitions = append(res.Transitions, t)
	}

	for _, v := range balanceVNodes {
		ring := consistenthash.New(v)
		ring.Add(names(balanceNodes)...)
		counts := map[string]int{}
		for _, k := range ks {
			o, _ := ring.Owner(k)
			counts[o]++
		}
		ideal := float64(keys) / float64(balanceNodes)
		b := Balance{VirtualNodes: v, Nodes: balanceNodes, MinKeys: keys}
		var sq float64
		for _, n := range names(balanceNodes) {
			c := counts[n]
			b.MinKeys = min(b.MinKeys, c)
			b.MaxKeys = max(b.MaxKeys, c)
			sq += (float64(c) - ideal) * (float64(c) - ideal)
		}
		b.MaxOverIdeal = float64(b.MaxKeys) / ideal
		b.StdDevPct = 100 * math.Sqrt(sq/float64(balanceNodes)) / ideal
		res.Balance = append(res.Balance, b)
	}
	return res
}

// indexOf recovers the node index from a name produced by nodeName.
func indexOf(name string) int {
	var i, port int
	fmt.Sscanf(name, "node-%d:%d", &i, &port)
	return i - 1
}
