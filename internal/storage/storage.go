// Package storage holds the per-node key-value data.
//
// Engine is the seam for a future persistent engine (e.g. an LSM tree or a
// write-ahead log + snapshot). The only implementation today is Memory, a
// sharded in-memory map: it is NOT durable — a process restart loses all data.
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
type Entry struct {
	Value     []byte
	Version   uint64
	Tombstone bool
}

// KV pairs a key with its entry, used by Scan.
type KV struct {
	Key string
	Entry
}

// Engine is a versioned key-value store with last-writer-wins semantics.
type Engine interface {
	// Get returns the entry for key, including tombstones.
	Get(key string) (Entry, bool)
	// Apply stores e if its version is newer than the stored one. It returns
	// whether e was applied and the entry now stored.
	Apply(key string, e Entry) (applied bool, current Entry)
	// Scan returns entries after cursor, up to roughly maxBytes of key+value
	// data (at least one entry if any remain). An empty next cursor means
	// the scan is complete. Scans are not a consistent snapshot: entries
	// written concurrently may or may not be included.
	Scan(cursor []byte, maxBytes int) (entries []KV, next []byte, err error)
	// Stats returns live-key and tombstone counts.
	Stats() Stats
}

// Stats counts stored entries.
type Stats struct {
	Keys       int `json:"keys"`
	Tombstones int `json:"tombstones"`
}

// numShards spreads keys over independently locked maps so concurrent
// requests for different keys rarely contend on the same mutex.
const numShards = 64

type shard struct {
	mu         sync.RWMutex
	m          map[string]Entry
	live       int
	tombstones int
}

// Memory is a sharded, in-memory Engine.
type Memory struct {
	shards [numShards]shard
}

// NewMemory returns an empty in-memory engine.
func NewMemory() *Memory {
	m := &Memory{}
	for i := range m.shards {
		m.shards[i].m = make(map[string]Entry)
	}
	return m
}

func shardIndex(key string) int {
	// FNV-1a (32-bit): cheap and good enough for shard selection.
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % numShards)
}

// Get implements Engine.
func (m *Memory) Get(key string) (Entry, bool) {
	s := &m.shards[shardIndex(key)]
	s.mu.RLock()
	e, ok := s.m[key]
	s.mu.RUnlock()
	return e, ok
}

// Apply implements Engine.
func (m *Memory) Apply(key string, e Entry) (bool, Entry) {
	s := &m.shards[shardIndex(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[key]
	if ok && cur.Version >= e.Version {
		return false, cur
	}
	if e.Tombstone {
		e.Value = nil
	}
	s.m[key] = e
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

var errBadCursor = errors.New("storage: invalid scan cursor")

// Scan implements Engine. The cursor encodes (shard index, last key
// returned); within a shard, keys are visited in sorted order so the cursor
// stays valid across concurrent inserts and deletes.
func (m *Memory) Scan(cursor []byte, maxBytes int) ([]KV, []byte, error) {
	shardIdx, after := 0, ""
	if len(cursor) > 0 {
		if len(cursor) < 4 {
			return nil, nil, errBadCursor
		}
		shardIdx = int(binary.BigEndian.Uint32(cursor))
		after = string(cursor[4:])
		if shardIdx >= numShards {
			return nil, nil, errBadCursor
		}
	}
	var out []KV
	size, lastShard := 0, 0
	for ; shardIdx < numShards; shardIdx, after = shardIdx+1, "" {
		s := &m.shards[shardIdx]
		s.mu.RLock()
		keys := make([]string, 0, len(s.m))
		for k := range s.m {
			if k > after {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			e := s.m[k]
			sz := len(k) + len(e.Value) + 16
			if len(out) > 0 && size+sz > maxBytes {
				s.mu.RUnlock()
				// Resume after the last entry returned, which may live in an
				// earlier shard than the one that overflowed the page.
				next := binary.BigEndian.AppendUint32(nil, uint32(lastShard))
				return out, append(next, out[len(out)-1].Key...), nil
			}
			out = append(out, KV{Key: k, Entry: e})
			size += sz
			lastShard = shardIdx
		}
		s.mu.RUnlock()
	}
	return out, nil, nil
}
