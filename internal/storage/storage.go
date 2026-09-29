// Package storage holds the per-node key-value data.
//
// Engine is the seam for a future persistent engine (e.g. an LSM tree or a
// write-ahead log + snapshot). The only implementation today is Memory, a
// sharded in-memory map: it is NOT durable — a process restart loses all data.
//
// # Three ideas in this package
//
//  1. Last-writer-wins (LWW) versioning. Every write carries a 64-bit
//     version (issued by the coordinator's clock, see package clock). A
//     replica keeps whichever write for a key has the highest version and
//     ignores older ones. Because "keep the max" gives the same answer in
//     any order, replicas converge to the same state no matter how
//     replication messages, retries and resync copies are reordered or
//     duplicated. That makes applying a write idempotent (applying it twice
//     equals applying it once), which is what makes retries safe.
//
//  2. Tombstones. A delete is stored as a versioned "this key is deleted"
//     marker instead of removing the key. See Entry for why.
//
//  3. Sharded locking (lock striping). The data is split over 64
//     independent maps, each with its own lock, chosen by hashing the key.
//     See numShards.
package storage

import (
	"encoding/binary"
	"errors"
	"sort"
	"sync"
)

// Entry is a versioned value. Deletes are stored as tombstones so that a
// delete can be ordered against concurrent or replayed writes (a plain
// map delete would let an older, delayed PUT resurrect the key).
//
// The resurrection problem, step by step, if deletes really removed keys:
//
//	t1  PUT  k=v   version 5   reaches replicas A and B, but is delayed to C
//	t2  DEL  k     version 6   reaches A, B and C; C has no k, nothing to do
//	t3  the delayed PUT v5 finally reaches C. C has no record of k at all,
//	    so it cannot know v5 is stale, and stores it: k is back from the dead
//
// With a tombstone, C stores {Tombstone, version 6} at t2, and at t3 compares
// 5 < 6 and rejects the PUT. The delete is just another write in the
// version order. The cost: tombstones occupy memory forever, since this
// system has no garbage collection of old tombstones (real systems purge
// them after a grace period, once every replica is known to have seen them).
//
// Fields: Value is the data (nil for a tombstone); Version orders writes
// (larger wins); Tombstone marks a delete.
type Entry struct {
	Value     []byte
	Version   uint64
	Tombstone bool
}

// KV pairs a key with its entry, used by Scan. Entry is embedded, so a KV's
// Value, Version and Tombstone fields can be accessed directly (kv.Version).
type KV struct {
	Key string
	Entry
}

// Engine is a versioned key-value store with last-writer-wins semantics.
//
// The node talks to storage only through this interface, so a durable
// engine could replace Memory without touching the node or replication
// code. Note that there is no Delete method: a delete is Apply with a
// tombstone Entry, so it follows the same version rules as a PUT.
type Engine interface {
	// Get returns the entry for key, including tombstones.
	// Callers must check Tombstone: a found tombstone means "deleted", which
	// is different from "never seen" (ok == false), and both the version and
	// the deletion matter to replication and resync.
	Get(key string) (Entry, bool)
	// Apply stores e if its version is newer than the stored one. It returns
	// whether e was applied and the entry now stored.
	// Returning the current entry on rejection lets the caller report the
	// newer version it lost to (and avoids a second Get, which could race).
	Apply(key string, e Entry) (applied bool, current Entry)
	// Scan returns entries after cursor, up to roughly maxBytes of key+value
	// data (at least one entry if any remain). An empty next cursor means
	// the scan is complete. Scans are not a consistent snapshot: entries
	// written concurrently may or may not be included.
	// Resync uses Scan to stream a node's whole contents to a recovering
	// peer in bounded pages, rather than one giant response.
	Scan(cursor []byte, maxBytes int) (entries []KV, next []byte, err error)
	// Stats returns live-key and tombstone counts.
	Stats() Stats
}

// Stats counts stored entries: Keys is the number of live (non-deleted) keys
// and Tombstones the number of delete markers. Keys+Tombstones is the total
// number of map entries.
type Stats struct {
	Keys       int `json:"keys"`
	Tombstones int `json:"tombstones"`
}

// numShards spreads keys over independently locked maps so concurrent
// requests for different keys rarely contend on the same mutex.
//
// With a single map and a single lock, every request on a node would queue
// on one mutex, and adding CPU cores would not add throughput. With 64
// shards, two requests conflict only if their keys hash to the same shard
// (probability 1/64 for two random keys). This is "lock striping": the same
// trick as a concurrent hash map. The layout:
//
//	key --FNV-1a 32--> h --h % 64--> shard index
//
//	shards[0]   { RWMutex, map, live, tombstones }
//	shards[1]   { RWMutex, map, live, tombstones }
//	...
//	shards[63]  { RWMutex, map, live, tombstones }
//
// A power of two is used by convention; any count works because the index
// is computed with %, not with a bit mask.
const numShards = 64

// shard is one stripe: its own map and its own lock.
//
// It uses an RWMutex (unlike the cache's Mutex) because here reads really
// are read-only: Get only looks up the map. So concurrent Gets of keys in
// the same shard proceed in parallel, and only Apply needs exclusivity.
//
// live and tombstones are maintained incrementally by Apply so that Stats is
// O(numShards) rather than a walk over every key. They are guarded by mu
// like the map.
type shard struct {
	mu         sync.RWMutex
	m          map[string]Entry
	live       int
	tombstones int
}

// Memory is a sharded, in-memory Engine.
//
// The shards are an array, not a slice of pointers: all 64 shards are laid
// out in one allocation and a shard is reached by indexing, with no extra
// indirection. (Neighbouring shards' mutexes may share a CPU cache line,
// which can cause some false sharing under heavy load; this code does not
// pad them.) A Memory must not be copied, because it contains mutexes;
// use the *Memory returned by NewMemory.
type Memory struct {
	shards [numShards]shard
}

// NewMemory returns an empty in-memory engine. The shard structs are
// already zero-valued (unlocked mutexes, zero counters) inside the array,
// but each map must be made explicitly: writing to a nil map panics.
func NewMemory() *Memory {
	m := &Memory{}
	for i := range m.shards {
		m.shards[i].m = make(map[string]Entry)
	}
	return m
}

// shardIndex maps a key to its shard, deterministically.
//
// This is deliberately a different hash from the ring's (32-bit FNV-1a with
// no finaliser, versus 64-bit FNV-1a plus fmix64). Here the only goal is to
// spread keys across 64 local locks cheaply; placement across machines is
// not at stake, so the cheaper hash is enough.
func shardIndex(key string) int {
	// FNV-1a (32-bit): cheap and good enough for shard selection.
	// 2166136261 is the 32-bit FNV offset basis; 16777619 is the 32-bit
	// FNV prime (2^24 + 2^8 + 0x93). Multiplication wraps modulo 2^32.
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	// Reduce to [0, 64). Since 64 divides 2^32, this takes the low 6 bits of
	// h. Modulo sharding is fine *here*: numShards is a compile-time constant
	// that never changes, so the remapping problem that consistent hashing
	// solves (see package consistenthash) cannot arise.
	return int(h % numShards)
}

// Get implements Engine.
//
// Cost: one hash of the key, a shared (read) lock on one shard, one map
// lookup. Returning Entry by value copies the struct but not the value
// bytes: the slice header still points at the stored array. That is safe
// because stored values are never modified in place, only replaced.
func (m *Memory) Get(key string) (Entry, bool) {
	// Take a pointer: copying a shard would copy its mutex, which is a bug
	// (go vet's copylocks check flags it).
	s := &m.shards[shardIndex(key)]
	// Read lock: other Gets in this shard can run at the same time; an
	// Apply to this shard waits until we release.
	s.mu.RLock()
	e, ok := s.m[key]
	// Unlock explicitly rather than defer: a tiny critical section on a hot
	// path, with no early return that could skip the unlock.
	s.mu.RUnlock()
	return e, ok
}

// Apply implements Engine.
//
// This is the heart of last-writer-wins. The compare and the store happen
// under the same exclusive shard lock, so they are one atomic step. If the
// check and the write were separate locked steps, two concurrent writers
// could both pass the version check and the older one could land last.
func (m *Memory) Apply(key string, e Entry) (bool, Entry) {
	s := &m.shards[shardIndex(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[key]
	// Reject unless strictly newer. The ">=" is essential:
	//   - cur.Version > e.Version: a stale write (a delayed replica message,
	//     an old resync copy, or a late PUT that must not resurrect a
	//     tombstone). Ignoring it keeps the newer state.
	//   - cur.Version == e.Version: the same write replayed (e.g. a transport
	//     retry). Reporting it as not applied makes replays idempotent and
	//     keeps the live/tombstone counters from being adjusted twice.
	// Nothing is written, and cur is returned so the caller can see what won.
	if ok && cur.Version >= e.Version {
		return false, cur
	}
	// Normalise tombstones to carry no payload, so a delete never pins the
	// old value's bytes in memory, and every tombstone looks the same.
	if e.Tombstone {
		e.Value = nil
	}
	s.m[key] = e
	// Keep the per-shard counters in step with the transition just made.
	// The four state changes that alter a count:
	//
	//	absent  -> tombstone   tombstones+1  (delete of a never-seen key)
	//	absent  -> live        live+1        (first write)
	//	tomb    -> live        tomb-1 live+1 (key revived by a newer PUT)
	//	live    -> tombstone   live-1 tomb+1 (delete)
	//
	// live -> live and tomb -> tomb change nothing, so there is no default
	// case. Case order matters: the !ok cases come first, because when !ok,
	// cur is the zero Entry and its Tombstone field means nothing.
	switch {
	case !ok && e.Tombstone:
		s.tombstones++
	case !ok:
		s.live++
	case cur.Tombstone && !e.Tombstone:
		s.tombstones--
		s.live++
	case !cur.Tombstone && e.Tombstone:
		s.live--
		s.tombstones++
	}
	return true, e
}

// Stats implements Engine.
//
// It locks one shard at a time, never all 64 at once, so it never stalls
// the whole node. The price is that the totals are not a single consistent
// snapshot: a write can land in shard 5 after shard 5 was counted. That is
// acceptable for a monitoring number.
func (m *Memory) Stats() Stats {
	var st Stats
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.RLock()
		st.Keys += s.live
		st.Tombstones += s.tombstones
		s.mu.RUnlock()
	}
	return st
}

// errBadCursor is returned for a cursor that Scan could not have produced
// (too short, or naming a shard that does not exist).
var errBadCursor = errors.New("storage: invalid scan cursor")

// Scan implements Engine. The cursor encodes (shard index, last key
// returned); within a shard, keys are visited in sorted order so the cursor
// stays valid across concurrent inserts and deletes.
//
// # Cursor-based pagination
//
// Offset-based paging ("skip the first 500 entries") breaks when data
// changes between pages: an insert before the offset shifts everything and
// an entry is returned twice; a delete shifts the other way and an entry is
// skipped. A cursor instead names a position by content: "resume after key
// K in shard S". Keys inserted or deleted elsewhere do not move that
// position, so every key that exists for the whole scan is returned exactly
// once. Go map iteration order is random, which is why each shard's keys
// are sorted: "after K" needs a stable order to mean anything.
//
// Cursor wire format (opaque to callers, who just echo it back):
//
//	+----------------------------+------------------------------+
//	| shard index: 4 bytes, BE   | last key returned: rest      |
//	+----------------------------+------------------------------+
//
// The walk goes shard 0..63, and within each shard in ascending key order:
//
//	shard 0: a1 a7 b2 | shard 1: a3 c9 | shard 2: ...
//	                ^ page ends here -> cursor = (0, "b2")
//	next call: shard 0 keys > "b2" (none), then shard 1 from the start, ...
//
// Cost: each call collects and sorts the keys of every shard it touches,
// O(k log k) for a shard of k keys, even if it returns only a few of them.
// Re-sorting per page is simple and holds no lock between calls; the
// trade-off is extra CPU for small pages over large shards.
func (m *Memory) Scan(cursor []byte, maxBytes int) ([]KV, []byte, error) {
	// Defaults for a fresh scan: start at shard 0 with no lower bound.
	// ("" is below every non-empty key; the protocol rejects empty keys.)
	shardIdx, after := 0, ""
	if len(cursor) > 0 {
		// A valid cursor has at least the 4-byte shard index.
		if len(cursor) < 4 {
			return nil, nil, errBadCursor
		}
		// Decode: the first 4 bytes are the shard, the rest is the last key.
		shardIdx = int(binary.BigEndian.Uint32(cursor))
		after = string(cursor[4:])
		// A shard index past the end would make the loop return an empty,
		// "complete" scan; reporting an error surfaces the corrupt cursor.
		if shardIdx >= numShards {
			return nil, nil, errBadCursor
		}
	}
	var out []KV
	// size is the running byte estimate of this page. lastShard remembers
	// which shard the most recently returned entry came from.
	size, lastShard := 0, 0
	// Visit shards from the cursor's onwards. The post statement moves to
	// the next shard and clears the lower bound: "after" applies only to the
	// shard the cursor was in; every later shard starts from its first key.
	for ; shardIdx < numShards; shardIdx, after = shardIdx+1, "" {
		s := &m.shards[shardIdx]
		// Read lock for this shard only; writes to other shards proceed.
		s.mu.RLock()
		// Gather the keys strictly after the cursor position...
		keys := make([]string, 0, len(s.m))
		for k := range s.m {
			if k > after {
				keys = append(keys, k)
			}
		}
		// ...and put them in a deterministic order.
		sort.Strings(keys)
		for _, k := range keys {
			e := s.m[k]
			// Estimated wire size: key + value + a fixed 16-byte allowance
			// per entry for the non-key/value data (e.g. the 8-byte version).
			sz := len(k) + len(e.Value) + 16
			// Page full? Stop before this entry, unless the page is still
			// empty: the len(out) > 0 condition guarantees progress by
			// always returning at least one entry, even if that single
			// entry exceeds maxBytes. Otherwise a large value could stall
			// the scan forever.
			if len(out) > 0 && size+sz > maxBytes {
				// Release the lock before returning (not deferred, because
				// the lock is taken once per loop iteration).
				s.mu.RUnlock()
				// Resume after the last entry returned, which may live in an
				// earlier shard than the one that overflowed the page.
				//
				// (Using shardIdx here would be the bug the regression test
				// TestScanSingleEntryPages guards against: if the page filled
				// on the first key of a new shard, the cursor would name the
				// new shard with the previous shard's last key, and keys of
				// the new shard sorting at or below that key would be skipped.)
				next := binary.BigEndian.AppendUint32(nil, uint32(lastShard))
				return out, append(next, out[len(out)-1].Key...), nil
			}
			out = append(out, KV{Key: k, Entry: e})
			size += sz
			lastShard = shardIdx
		}
		s.mu.RUnlock()
	}
	// Every shard visited without filling a page: the scan is complete, as
	// signalled by a nil next cursor.
	return out, nil, nil
}
