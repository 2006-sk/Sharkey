// Package consistenthash implements a consistent-hashing ring with virtual
// nodes.
//
// Each physical node is placed on a 64-bit ring at V pseudo-random positions
// ("virtual nodes"), obtained by hashing "<node>#<i>" for i in [0, V). A key is
// owned by the first virtual node clockwise from hash(key). Because a node
// only owns the arcs immediately preceding its own points, adding or removing
// one node of N only moves roughly 1/N of the keys — unlike hash(key) % N,
// where almost every key changes owner when N changes.
//
// Virtual nodes smooth out the load: with one point per node the arc lengths
// (and so the key shares) vary wildly; with V points per node each node's
// share is the sum of V arcs and concentrates around 1/N.
package consistenthash

import (
	"sort"
	"strconv"
	"sync"
)

// DefaultVirtualNodes is the default number of ring points per physical node.
const DefaultVirtualNodes = 128

// Hash hashes s with 64-bit FNV-1a followed by a 64-bit avalanche finaliser.
//
// FNV-1a alone is stable and fast but disperses poorly for short, similar
// inputs such as "node-1#17" / "node-1#18", which would cluster virtual nodes
// on the ring. The finaliser (from MurmurHash3's fmix64) mixes every input bit
// into every output bit, which fixes that without changing stability.
func Hash(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

type point struct {
	hash uint64
	node string
}

// Ring is a consistent-hash ring. It is safe for concurrent use: lookups take
// a read lock, membership changes take a write lock.
type Ring struct {
	mu     sync.RWMutex
	vnodes int
	points []point // sorted by hash (ties broken by node name)
	nodes  map[string]struct{}
}

// New returns an empty ring placing vnodes points per physical node.
func New(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVirtualNodes
	}
	return &Ring{vnodes: vnodes, nodes: make(map[string]struct{})}
}

// VirtualNodes returns the number of points per physical node.
func (r *Ring) VirtualNodes() int { return r.vnodes }

// Add inserts physical nodes. Adding an existing node is a no-op.
func (r *Ring) Add(nodes ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range nodes {
		if _, ok := r.nodes[n]; ok {
			continue
		}
		r.nodes[n] = struct{}{}
		for i := 0; i < r.vnodes; i++ {
			r.points = append(r.points, point{hash: Hash(n + "#" + strconv.Itoa(i)), node: n})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		return r.points[i].node < r.points[j].node
	})
}

// Remove deletes a physical node and all of its virtual nodes.
func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; !ok {
		return
	}
	delete(r.nodes, node)
	kept := r.points[:0]
	for _, p := range r.points {
		if p.node != node {
			kept = append(kept, p)
		}
	}
	r.points = kept
}

// Nodes returns the physical nodes, sorted.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Len returns the number of physical nodes.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// search returns the index of the first point clockwise from h, wrapping
// around to 0 past the largest point. Caller holds the lock; points non-empty.
func (r *Ring) search(h uint64) int {
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0
	}
	return i
}

// Owner returns the physical node owning key (its primary).
func (r *Ring) Owner(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return "", false
	}
	return r.points[r.search(Hash(key))].node, true
}

// Replicas returns up to n distinct physical nodes responsible for key, in
// preference order: the owner first, then the next distinct physical nodes
// clockwise. Virtual nodes belonging to a physical node already chosen are
// skipped, so a node never counts as two replicas. If the ring has fewer than
// n physical nodes, all of them are returned.
func (r *Ring) Replicas(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 || n <= 0 {
		return nil
	}
	n = min(n, len(r.nodes))
	out := make([]string, 0, n)
	start := r.search(Hash(key))
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		node := r.points[(start+i)%len(r.points)].node
		if !contains(out, node) {
			out = append(out, node)
		}
	}
	return out
}

// contains is a linear scan: replica lists are tiny (RF is typically 3), so
// this beats allocating a set.
func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// Shares returns the fraction of the 2^64 hash space owned by each physical
// node — a direct measure of how evenly virtual nodes spread the load.
func (r *Ring) Shares() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]float64, len(r.nodes))
	if len(r.points) == 0 {
		return out
	}
	const space = float64(1<<63) * 2
	for i, p := range r.points {
		// Point i owns the arc (prev, p.hash]; uint64 subtraction wraps
		// correctly for the first point, whose arc crosses zero.
		prev := r.points[(i+len(r.points)-1)%len(r.points)].hash
		arc := p.hash - prev
		if len(r.points) == 1 {
			out[p.node] = 1
			continue
		}
		out[p.node] += float64(arc) / space
	}
	return out
}
