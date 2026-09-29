// Tests for the storage node. They call Handle directly (no network) except where the
// wire path itself is under test (TestScanOverTCP). Together they pin down the node's
// three correctness contracts: last-writer-wins by version, tombstones for deletes, and
// a cache that never disagrees with storage.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"distkv/internal/protocol"
	"distkv/internal/transport"
)

// newTestNode builds a node with the given cache capacity (0 disables the cache) and a
// logger that discards output, so tests stay quiet.
func newTestNode(cacheCap int) *Node {
	cfg := DefaultConfig()
	cfg.CacheCapacity = cacheCap
	cfg.Logger = log.New(io.Discard, "", 0)
	return New(cfg)
}

// do sends one request through Handle, exactly as the transport server would after
// decoding a frame. version 0 means "unversioned" (the node stamps its own version);
// flags are protocol.FlagReplica / protocol.FlagSync.
func do(n *Node, op protocol.Op, key, val string, version uint64, flags uint8) *protocol.Response {
	return n.Handle(context.Background(), &protocol.Request{Op: op, Key: key, Value: []byte(val), Version: version, Flags: flags})
}

// TestPutGetDelete proves the basic single-key lifecycle (missing -> put -> overwrite
// -> delete) behaves identically with the cache disabled (cache=0) and enabled
// (cache=100). Running both variants matters because the cache must be a pure
// optimisation: it may change latency but never an answer. It also checks that
// unversioned writes get a non-zero version and that a deleted key reads as NOT_FOUND
// *carrying the tombstone's version*, which the coordinator needs to order a delete
// against older values on other replicas.
func TestPutGetDelete(t *testing.T) {
	for _, capacity := range []int{0, 100} {
		t.Run(fmt.Sprintf("cache=%d", capacity), func(t *testing.T) {
			n := newTestNode(capacity)
			if r := do(n, protocol.OpGet, "k", "", 0, 0); r.Status != protocol.StatusNotFound {
				t.Fatalf("missing key: %v", r.Status)
			}
			if r := do(n, protocol.OpPut, "k", "v1", 0, 0); r.Status != protocol.StatusOK || r.Version == 0 {
				t.Fatalf("put: %+v", r)
			}
			if r := do(n, protocol.OpGet, "k", "", 0, 0); string(r.Value) != "v1" {
				t.Fatalf("get: %+v", r)
			}
			do(n, protocol.OpPut, "k", "v2", 0, 0)
			if r := do(n, protocol.OpGet, "k", "", 0, 0); string(r.Value) != "v2" {
				t.Fatalf("get after overwrite: %q", r.Value)
			}
			do(n, protocol.OpDelete, "k", "", 0, 0)
			r := do(n, protocol.OpGet, "k", "", 0, 0)
			if r.Status != protocol.StatusNotFound || r.Version == 0 {
				t.Fatalf("get after delete should be NOT_FOUND carrying tombstone version: %+v", r)
			}
		})
	}
}

// TestCacheReadThroughAndInvalidation checks each cache policy in turn:
//   - read-through: the first GET misses and populates, the second hits;
//   - update-in-place: a PUT to a cached key replaces the cached value and version;
//   - invalidation: a DELETE removes the cached entry, and the key then reads as gone;
//   - no write-allocate: a PUT to an uncached key does not create a cache entry.
//
// The last point matters because two of every three replicas receive writes but rarely
// serve reads; allocating on write would fill their caches with keys nobody reads.
func TestCacheReadThroughAndInvalidation(t *testing.T) {
	n := newTestNode(100)
	do(n, protocol.OpPut, "k", "v1", 10, 0)
	do(n, protocol.OpGet, "k", "", 0, 0) // miss -> populate
	do(n, protocol.OpGet, "k", "", 0, 0) // hit
	cs := n.Stats().Cache
	if cs.Hits != 1 || cs.Misses != 1 || cs.Size != 1 {
		t.Fatalf("cache stats %+v", cs)
	}
	// PUT updates the cached value in place.
	do(n, protocol.OpPut, "k", "v2", 11, 0)
	if r := do(n, protocol.OpGet, "k", "", 0, 0); string(r.Value) != "v2" || r.Version != 11 {
		t.Fatalf("stale cached value after put: %+v", r)
	}
	// DELETE invalidates.
	do(n, protocol.OpDelete, "k", "", 12, 0)
	if n.Stats().Cache.Size != 0 {
		t.Fatal("delete did not invalidate cache")
	}
	if r := do(n, protocol.OpGet, "k", "", 0, 0); r.Status != protocol.StatusNotFound {
		t.Fatalf("deleted key served: %+v", r)
	}
	// A PUT for an uncached key must not populate the cache.
	do(n, protocol.OpPut, "other", "x", 13, 0)
	if n.Stats().Cache.Size != 0 {
		t.Fatal("write allocated a cache entry")
	}
}

// TestStaleReplicatedWriteIgnored proves last-writer-wins: a replica write carrying an
// older version (10) arriving after a newer one (20) is acknowledged with OK but ignored,
// and the response reports the version the node actually holds (20). The key is cached
// first, so the test also shows a stale write can't overwrite the cache. This is what
// lets replicas converge regardless of message order, and why retries and resync
// copies are safe to replay. The stats check confirms both FlagReplica writes were
// counted and exactly one was ignored as stale.
func TestStaleReplicatedWriteIgnored(t *testing.T) {
	n := newTestNode(100)
	do(n, protocol.OpPut, "k", "new", 20, protocol.FlagReplica)
	do(n, protocol.OpGet, "k", "", 0, 0)
	r := do(n, protocol.OpPut, "k", "old", 10, protocol.FlagReplica)
	if r.Status != protocol.StatusOK || r.Version != 20 {
		t.Fatalf("stale write response %+v", r)
	}
	if r := do(n, protocol.OpGet, "k", "", 0, 0); string(r.Value) != "new" {
		t.Fatalf("stale write overwrote value: %q", r.Value)
	}
	st := n.Stats()
	if st.StaleWrites != 1 || st.ReplicationOps != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// TestCacheNeverStaleUnderConcurrency hammers a few keys with concurrent
// GET/PUT/DELETE and then checks the cache agrees with storage for each key.
//
// This is the regression test for the cache coherence race described on
// Node.keyLocks (a GET miss reads v1, a PUT writes v2, then the GET caches v1 forever).
// 16 goroutines x 3000 operations over only 8 keys maximise collisions on the same key.
// Versions come from a shared counter so they are globally unique and increasing, like
// the coordinator's clock. At the end, any cached entry must match storage exactly
// (same version and value, and never a tombstone). Remove the stripe lock from
// Node.get or Node.apply and this test can fail (most reliably under -race).
func TestCacheNeverStaleUnderConcurrency(t *testing.T) {
	n := newTestNode(1000)
	var wg sync.WaitGroup
	var mu sync.Mutex
	next := uint64(0)
	version := func() uint64 { mu.Lock(); defer mu.Unlock(); next++; return next }
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 3000; i++ {
				k := fmt.Sprintf("k%d", i%8)
				switch (i + g) % 4 {
				case 0:
					do(n, protocol.OpPut, k, fmt.Sprintf("v%d-%d", g, i), version(), 0)
				case 1:
					do(n, protocol.OpDelete, k, "", version(), 0)
				default:
					do(n, protocol.OpGet, k, "", 0, 0)
				}
			}
		}(g)
	}
	wg.Wait()
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%d", i)
		e, _ := n.store.Get(k)
		cv, cver, cached := n.cache.Get(k)
		if cached && (e.Tombstone || cver != e.Version || !bytes.Equal(cv, e.Value)) {
			t.Fatalf("%s: cache has (%q,v%d) but storage has %+v", k, cv, cver, e)
		}
	}
}

// TestConcurrentDistinctKeys checks that many goroutines working on disjoint keys see
// their own writes immediately (read-your-writes on a single node) and that the
// storage live-key counter stays exact under concurrency: each goroutine deletes half
// of its 500 keys, so 16*250 live keys must remain. A miscounted live/tombstone
// transition in a storage shard would show up here.
func TestConcurrentDistinctKeys(t *testing.T) {
	n := newTestNode(500)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("g%d-k%d", g, i)
				do(n, protocol.OpPut, k, k, 0, 0)
				if r := do(n, protocol.OpGet, k, "", 0, 0); string(r.Value) != k {
					t.Errorf("read own write %s: %q", k, r.Value)
					return
				}
				if i%2 == 0 {
					do(n, protocol.OpDelete, k, "", 0, 0)
				}
			}
		}(g)
	}
	wg.Wait()
	if keys := n.Stats().Storage.Keys; keys != 16*250 {
		t.Fatalf("stored keys = %d, want %d", keys, 16*250)
	}
}

// TestScanOverTCP exercises the resync read side end to end over a real TCP
// connection: it pages through 1000 keys using the cursor returned by each page and a
// deliberately tiny 1 KiB page size (so many pages are needed), and checks every entry
// is returned exactly once in total. This proves the cursor resumes correctly across
// pages and shards. It then PINGs the node and checks the health document carries the
// node's boot ID and live key count, which the coordinator's health checker relies on
// to detect restarts.
func TestScanOverTCP(t *testing.T) {
	n := newTestNode(0)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go n.Serve(ln)
	defer n.Shutdown(context.Background())

	for i := 0; i < 1000; i++ {
		do(n, protocol.OpPut, fmt.Sprintf("key-%d", i), "value", 0, 0)
	}
	pool := transport.NewPool(ln.Addr().String(), time.Second, 1, 0)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var cursor []byte
	total := 0
	small := []byte{0, 0, 4, 0} // 1 KiB pages
	for {
		r, err := pool.Do(ctx, &protocol.Request{Op: protocol.OpScan, Key: string(cursor), Value: small})
		if err != nil || r.Status != protocol.StatusOK {
			t.Fatalf("scan: %v %+v", err, r)
		}
		page, err := protocol.DecodeScanPage(r.Value)
		if err != nil {
			t.Fatal(err)
		}
		total += len(page.Entries)
		if len(page.NextCursor) == 0 {
			break
		}
		cursor = page.NextCursor
	}
	if total != 1000 {
		t.Fatalf("scanned %d entries", total)
	}

	r, err := pool.Do(ctx, &protocol.Request{Op: protocol.OpPing})
	if err != nil {
		t.Fatal(err)
	}
	var h Health
	if err := json.Unmarshal(r.Value, &h); err != nil || h.BootID != n.BootID() || h.Keys != 1000 {
		t.Fatalf("health %+v %v", h, err)
	}
}

// TestApplyBatch proves the resync write side: an APPLY_BATCH applies each entry with
// last-writer-wins, exactly like individual writes. It checks that a new key is
// created with the version from the batch, that a tombstone is applied (the node
// learns about a delete it missed), and that an entry older than what the node holds
// (version 50 vs 100) is ignored in both storage and cache. The last case is what makes
// resync safe to run concurrently with live writes. It also checks all three entries
// count as sync ops (one of them stale), and that malformed or version-less batches are
// rejected with BAD_REQUEST instead of being applied.
func TestApplyBatch(t *testing.T) {
	n := newTestNode(100)
	do(n, protocol.OpPut, "newer", "keep", 100, 0)
	do(n, protocol.OpGet, "newer", "", 0, 0) // cache it
	page := &protocol.ScanPage{Entries: []protocol.ScanEntry{
		{Key: "a", Value: []byte("1"), Version: 5},
		{Key: "gone", Version: 6, Tombstone: true},
		{Key: "newer", Value: []byte("stale"), Version: 50},
	}}
	r := n.Handle(context.Background(), &protocol.Request{Op: protocol.OpApplyBatch, Value: protocol.EncodeScanPage(page)})
	if r.Status != protocol.StatusOK {
		t.Fatalf("apply batch: %+v", r)
	}
	if r := do(n, protocol.OpGet, "a", "", 0, 0); string(r.Value) != "1" || r.Version != 5 {
		t.Fatalf("a: %+v", r)
	}
	if r := do(n, protocol.OpGet, "gone", "", 0, 0); r.Status != protocol.StatusNotFound || r.Version != 6 {
		t.Fatalf("gone: %+v", r)
	}
	if r := do(n, protocol.OpGet, "newer", "", 0, 0); string(r.Value) != "keep" {
		t.Fatalf("batch overwrote newer value (or stale cache): %q", r.Value)
	}
	if st := n.Stats(); st.SyncOps != 3 || st.StaleWrites != 1 {
		t.Fatalf("stats %+v", st)
	}
	// Malformed and version-less batches are rejected.
	bad := n.Handle(context.Background(), &protocol.Request{Op: protocol.OpApplyBatch, Value: []byte{1, 2}})
	if bad.Status != protocol.StatusBadRequest {
		t.Fatalf("malformed batch: %v", bad.Status)
	}
	noVer := protocol.EncodeScanPage(&protocol.ScanPage{Entries: []protocol.ScanEntry{{Key: "x", Value: []byte("v")}}})
	if r := n.Handle(context.Background(), &protocol.Request{Op: protocol.OpApplyBatch, Value: noVer}); r.Status != protocol.StatusBadRequest {
		t.Fatalf("unversioned batch: %v", r.Status)
	}
}
