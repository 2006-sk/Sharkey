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
//
// # The problem this package solves
//
// A cluster of storage nodes must agree on which node holds which key,
// without a central lookup table. The naive answer is modulo sharding:
//
//	owner = hash(key) % N
//
// That is perfectly balanced, but it is catastrophic when N changes. A key
// keeps its owner only if hash%N == hash%(N+1), which is true for only about
// 1/(N+1) of keys, so about N/(N+1) of all keys change owner and must be
// copied across the network. The README measures this: going from 4 to 5
// nodes moves ~80% of keys under modulo, but ~21% on the ring (theory: 20%).
//
// # The ring
//
// Picture the whole uint64 range [0, 2^64) bent into a circle, so that the
// value after 2^64-1 is 0 again. Every node is hashed onto the circle at some
// points, and every key is hashed onto the same circle. A key belongs to the
// first node point found by walking clockwise (towards larger hashes,
// wrapping past the top back to 0):
//
//	                  0 / 2^64
//	                     |
//	        A#2 *  ------+------  * B#0
//	          /                     \
//	key k2 -> x                       x <- key k1  (walks clockwise to C#1)
//	         |                         |
//	    B#1 *                           * C#1
//	          \                       /
//	        C#0 *  ---------------  * A#0
//
// Each point owns the arc that ends at it: the keys between its
// counter-clockwise neighbour (exclusive) and itself (inclusive).
//
// When a new node D is added, its points land inside existing arcs and each
// one steals only the part of one arc that lies just before it. Every other
// key keeps its owner. When a node is removed, each of its arcs is absorbed
// by the next point clockwise. Either way only the keys of the node that
// joined or left ever move: that is the whole point of consistent hashing.
//
// # Why virtual nodes
//
// With one point per node, N random points cut the circle into N arcs whose
// lengths are very uneven (the README measured the busiest of 4 nodes at
// 1.44x its fair share). Giving each node V points (default 128) makes a
// node's share the sum of V independent arcs; by the law of large numbers
// that sum concentrates around 1/N (README: 1.04x the ideal with 128 vnodes).
// A second benefit: when a node leaves, its load is spread over many
// successors instead of being dumped on one unlucky neighbour.
//
// # Data structure
//
// The ring is stored as a slice of (hash, node) points sorted by hash. A
// lookup is a binary search for the first point with hash >= hash(key),
// wrapping to index 0 if the key hashes past the last point. With N nodes and
// V vnodes that is O(log(N*V)) comparisons, and no per-lookup allocation.
package consistenthash

import (
	"sort"
	"strconv"
	"sync"
)

// DefaultVirtualNodes is the default number of ring points per physical node.
//
// 128 is the value the README benchmarks (Test E compares 1/8/32/128/256):
// enough points that each node's share lands within a few percent of 1/N,
// while the ring stays small (e.g. 4 nodes -> 512 points, about 9 binary
// search steps). More points improve balance slowly (roughly with 1/sqrt(V))
// but cost memory and make Add/Remove re-sorting more expensive.
const DefaultVirtualNodes = 128

// Hash hashes s with 64-bit FNV-1a followed by a 64-bit avalanche finaliser.
//
// FNV-1a alone is stable and fast but disperses poorly for short, similar
// inputs such as "node-1#17" / "node-1#18", which would cluster virtual nodes
// on the ring. The finaliser (from MurmurHash3's fmix64) mixes every input bit
// into every output bit, which fixes that without changing stability.
//
// Requirements for a ring hash, and how this function meets them:
//
//   - Deterministic across processes and restarts. Every coordinator (and
//     every test) must place the same key on the same node. Go's built-in map
//     hash is randomly seeded per process, so it cannot be used here. FNV-1a
//     plus fmix64 is a pure function of the input bytes.
//   - Uniform. Points and keys must spread evenly over [0, 2^64), otherwise
//     some arcs are systematically longer and their nodes get more keys.
//   - Avalanche. Flipping any one input bit should flip each output bit with
//     probability ~1/2. Vnode names differ only in their last character or
//     two, so without good avalanche their hashes would sit close together
//     and form clumps on the ring instead of being scattered.
//   - Cheap. It runs on every request. It is not cryptographic, which is fine:
//     nobody is attacking the placement of keys.
//
// FNV-1a works byte by byte: XOR the byte into the state, then multiply by a
// large odd prime. The multiply only propagates bits upwards (a product's low
// bits depend only on the factors' low bits), so when the last byte changes,
// every bit below the lowest changed bit comes out identical, and nearby
// strings get hashes that agree in many low bits. fmix64 fixes
// that with alternating "xor with a right-shifted copy" (moves high bits
// down) and "multiply by an odd constant" (moves low bits up). After three
// rounds every output bit depends on every input bit.
func Hash(s string) uint64 {
	// The standard 64-bit FNV parameters. offset64 is the FNV offset basis
	// (the starting state, so the empty string does not hash to 0) and
	// prime64 is the FNV prime 2^40 + 2^8 + 0xb3, chosen for good dispersion.
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	// FNV-1a main loop. The "1a" variant XORs before multiplying (FNV-1
	// multiplies first), which lets the last byte take part in a multiply and
	// gives slightly better mixing. Indexing s[i] walks raw bytes, not runes,
	// which is exactly what we want: the hash is over the key's bytes.
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		// Multiplication is modulo 2^64: Go's unsigned overflow silently
		// wraps, and that wraparound is part of the hash's definition.
		h *= prime64
	}
	// MurmurHash3 fmix64 finaliser: xor-shift, multiply, xor-shift, multiply,
	// xor-shift. Shifting right by 33 folds the high half into the low half;
	// the odd multipliers are the constants published with MurmurHash3,
	// found by search to maximise avalanche. Every step is a bijection on
	// uint64 (xor-shift is invertible, and multiplying by an odd number is
	// invertible modulo 2^64), so fmix64 adds no collisions of its own: two
	// strings collide after it only if they already collided under FNV-1a.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// point is one virtual node: a position on the ring and the physical node it
// belongs to. The ring is a slice of these kept sorted by hash. Storing the
// node name next to the hash (rather than, say, an index into a node table)
// means a lookup returns the answer straight from the slice with no second
// indirection.
type point struct {
	// hash is the position on the ring, Hash("<node>#<i>").
	hash uint64
	// node is the physical node (its address, e.g. "host:port") that owns
	// this point.
	node string
}

// Ring is a consistent-hash ring. It is safe for concurrent use: lookups take
// a read lock, membership changes take a write lock.
//
// Why sync.RWMutex and not sync.Mutex: lookups (Owner, Replicas) happen on
// every client request and only read the slice, while membership changes
// (Add, Remove) are rare admin events. An RWMutex lets any number of lookups
// run in parallel and only makes them wait during the brief moment a node is
// added or removed. A plain Mutex would serialise every request in the
// cluster through this one lock. (Contrast the LRU cache, where even a read
// mutates the recency list, so an RWMutex would buy nothing there.)
//
// Fields:
//
//   - mu guards points and nodes. vnodes is set once in New and never
//     changes, so it may be read without the lock.
//   - vnodes is how many points each physical node gets (V).
//   - points holds every virtual node of every physical node, len = N*V.
//     Keeping it sorted is what makes lookup a binary search. The tie-break
//     by node name makes the order fully deterministic even in the (very
//     unlikely) case that two vnodes hash to the same value, so two rings
//     built from the same members always route identically, regardless of
//     the order in which the members were added.
//   - nodes is the set of physical nodes (a map to empty structs is Go's
//     idiomatic set; struct{} takes zero bytes). It makes "is this node
//     already on the ring?" O(1) and gives the physical node count, which
//     Replicas uses to cap the replication factor.
type Ring struct {
	mu     sync.RWMutex
	vnodes int
	points []point // sorted by hash (ties broken by node name)
	nodes  map[string]struct{}
}

// New returns an empty ring placing vnodes points per physical node.
// A non-positive vnodes falls back to DefaultVirtualNodes rather than
// producing a ring on which nodes have no points and nothing can be routed.
func New(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVirtualNodes
	}
	// points starts as a nil slice; append allocates on the first Add. The
	// nodes map must be made up front because writing to a nil map panics.
	return &Ring{vnodes: vnodes, nodes: make(map[string]struct{})}
}

// VirtualNodes returns the number of points per physical node. No lock is
// needed: vnodes is immutable after New.
func (r *Ring) VirtualNodes() int { return r.vnodes }

// Add inserts physical nodes. Adding an existing node is a no-op.
//
// Cost: O(V) hashes per new node, then a full sort of all N*V points,
// O(P log P) with P = N*V. That is fine because membership changes are rare
// (cluster start-up, an operator adding a node) and P is small (hundreds to
// a few thousand). A variadic signature lets start-up add every node with a
// single sort instead of one sort per node.
func (r *Ring) Add(nodes ...string) {
	// Exclusive lock: we are mutating points, and a concurrent lookup must
	// never observe a half-appended or half-sorted slice (binary search on an
	// unsorted slice returns garbage).
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range nodes {
		// Idempotence: re-adding a member must not give it a second set of
		// points (it would double its share of the keys).
		if _, ok := r.nodes[n]; ok {
			continue
		}
		r.nodes[n] = struct{}{}
		// Place V points for this node at Hash("n#0"), Hash("n#1"), ...
		// The names are deterministic, so every process that adds the same
		// node computes exactly the same positions: no coordination needed.
		// They are appended unsorted; the single sort below orders them.
		for i := 0; i < r.vnodes; i++ {
			r.points = append(r.points, point{hash: Hash(n + "#" + strconv.Itoa(i)), node: n})
		}
	}
	// Restore the sorted invariant. Order by hash; on an exact hash tie, by
	// node name, so the result does not depend on insertion order (which
	// TestDeterministicRouting checks). Runs even if nothing was added,
	// which is harmless.
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		return r.points[i].node < r.points[j].node
	})
}

// Remove deletes a physical node and all of its virtual nodes.
//
// Removing points from a sorted slice keeps it sorted, so no re-sort is
// needed: one O(P) filtering pass suffices. The arcs the removed points
// owned are now owned by the next surviving point clockwise; no other key
// changes owner.
func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Removing an unknown node is a no-op (and skips the O(P) pass).
	if _, ok := r.nodes[node]; !ok {
		return
	}
	delete(r.nodes, node)
	// In-place filter idiom: kept shares r.points' backing array, starting
	// with length 0. Each surviving point is written at or before the index
	// it was read from, so the write never clobbers an element not yet read.
	// No allocation. This is safe only because we hold the write lock: no
	// reader can be looking at the array while it is being compacted.
	kept := r.points[:0]
	for _, p := range r.points {
		if p.node != node {
			kept = append(kept, p)
		}
	}
	// Shrink the visible length to the survivors. (The tail of the backing
	// array beyond len still holds stale copies until the next append
	// overwrites them; they are unreachable through the slice.)
	r.points = kept
}

// Nodes returns the physical nodes, sorted.
//
// It returns a fresh copy so callers can use it after the lock is released
// without racing with Add/Remove. Map iteration order in Go is randomised on
// purpose, so the copy is sorted to give callers a stable, reproducible order.
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

// Len returns the number of physical nodes (not the number of points).
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// search returns the index of the first point clockwise from h, wrapping
// around to 0 past the largest point. Caller holds the lock; points non-empty.
//
// Example with four points (hashes shown small for readability):
//
//	index:   0     1     2     3
//	hash:   10    40    70    90
//
//	h = 40 -> 1  (exact match: the point owns its own position)
//	h = 55 -> 2  (first point with hash >= 55)
//	h = 5  -> 0
//	h = 95 -> sort.Search returns 4 == len, wrap to 0
//
// The wrap is what turns a sorted line into a circle: a key hashed above the
// last point belongs to the lowest point, whose arc crosses 2^64 -> 0.
func (r *Ring) search(h uint64) int {
	// sort.Search is a generic binary search: it finds the smallest index i
	// for which the predicate is true, given that the predicate is false then
	// true along the slice (it is, since points are sorted by hash). It runs
	// in O(log P) and returns len(points) when no point satisfies it.
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	// Past the largest point: wrap around the ring to the smallest point.
	// Without this, keys hashing into the top arc would index out of range.
	if i == len(r.points) {
		i = 0
	}
	return i
}

// Owner returns the physical node owning key (its primary).
// ok is false only for an empty ring, where no node can own anything.
func (r *Ring) Owner(key string) (string, bool) {
	// Shared lock: any number of concurrent Owner/Replicas calls may run
	// together; only Add/Remove exclude them.
	r.mu.RLock()
	defer r.mu.RUnlock()
	// search requires a non-empty slice (on an empty one it would return 0,
	// and indexing points[0] would panic).
	if len(r.points) == 0 {
		return "", false
	}
	// Hash the key onto the ring, find the next point clockwise, and return
	// the physical node behind that virtual node.
	return r.points[r.search(Hash(key))].node, true
}

// Replicas returns up to n distinct physical nodes responsible for key, in
// preference order: the owner first, then the next distinct physical nodes
// clockwise. Virtual nodes belonging to a physical node already chosen are
// skipped, so a node never counts as two replicas. If the ring has fewer than
// n physical nodes, all of them are returned.
//
// Why skipping matters: with 128 vnodes per node, the next few points
// clockwise frequently belong to the same physical node. Taking "the next 3
// points" would often put two or even all three copies of a key on one
// machine, and losing that machine would lose the key despite "RF=3".
//
// Walk-through for n = 3, starting at the key's owner:
//
//	points:  ... [A#7] [A#90] [C#3] [A#12] [B#44] ...
//	                ^ start
//	         take A   skip A   take C   skip A   take B  -> [A, C, B], stop
//
// This preference list is also what gives failure handling its fallback
// order: if the owner is down, the coordinator tries the next entry.
func (r *Ring) Replicas(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Empty ring or a request for zero replicas: nothing to return.
	if len(r.points) == 0 || n <= 0 {
		return nil
	}
	// Cap n at the number of physical nodes: a 2-node cluster cannot hold 3
	// distinct copies (the effective RF becomes 2). Without the cap, the loop
	// below would walk the entire ring looking for a third node that does not
	// exist (still terminating, thanks to the i < len bound, but wastefully).
	n = min(n, len(r.nodes))
	// Pre-size the result to exactly n so append never reallocates.
	out := make([]string, 0, n)
	// The owner's point is where the walk begins; the owner is always first.
	start := r.search(Hash(key))
	// Walk clockwise one point at a time. Two exit conditions: we have n
	// distinct nodes (the normal case, usually after a handful of steps), or
	// we have visited every point once (a hard bound that guarantees
	// termination whatever the ring contents).
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		// (start+i) % len wraps the index past the end back to 0: the same
		// wrap-around as in search, applied to walking instead of lookup.
		node := r.points[(start+i)%len(r.points)].node
		// Skip vnodes of a physical node already chosen.
		if !contains(out, node) {
			out = append(out, node)
		}
	}
	return out
}

// contains is a linear scan: replica lists are tiny (RF is typically 3), so
// this beats allocating a set.
//
// For a slice of at most a few strings, a loop over contiguous memory is
// faster than hashing into a map, and it avoids a heap allocation on every
// request. Asymptotically O(n), but n is tiny.
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
//
// If keys hash uniformly, a node's expected fraction of keys equals the
// fraction of the ring its arcs cover, so this computes the load balance
// exactly from the geometry, without hashing any sample keys. The values sum
// to 1 (TestSharesSumToOne).
func (r *Ring) Shares() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]float64, len(r.nodes))
	if len(r.points) == 0 {
		return out
	}
	// The size of the ring, 2^64. That does not fit in a uint64 (whose max is
	// 2^64-1), so it is formed directly as a float64: 2^63 * 2. Arcs are
	// divided by it to get fractions of the whole ring.
	const space = float64(1<<63) * 2
	for i, p := range r.points {
		// Point i owns the arc (prev, p.hash]; uint64 subtraction wraps
		// correctly for the first point, whose arc crosses zero.
		//
		// (i + len - 1) % len is "the previous index, wrapping": for i = 0 it
		// gives len-1, the largest point. Adding len before subtracting keeps
		// the operand non-negative (in Go, -1 % len is -1, not len-1).
		prev := r.points[(i+len(r.points)-1)%len(r.points)].hash
		// Modular arithmetic does the wrap-around for us. Example with a
		// small 8-bit ring (values 0..255): first point at 10, last at 250.
		// The first point's arc runs 251..255 then 0..10, i.e. 16 values,
		// and 10 - 250 in uint8 is (10 - 250 + 256) = 16. The same identity
		// holds for uint64 modulo 2^64, so no special case is needed.
		arc := p.hash - prev
		// A single point is its own predecessor, so its arc computes as 0,
		// but it actually owns the whole ring. Special-case it.
		if len(r.points) == 1 {
			out[p.node] = 1
			continue
		}
		// Accumulate: a physical node's share is the sum of the arcs of all
		// its virtual nodes.
		out[p.node] += float64(arc) / space
	}
	return out
}
