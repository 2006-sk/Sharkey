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

func newTestNode(cacheCap int) *Node {
	cfg := DefaultConfig()
	cfg.CacheCapacity = cacheCap
	cfg.Logger = log.New(io.Discard, "", 0)
	return New(cfg)
}

func do(n *Node, op protocol.Op, key, val string, version uint64, flags uint8) *protocol.Response {
	return n.Handle(context.Background(), &protocol.Request{Op: op, Key: key, Value: []byte(val), Version: version, Flags: flags})
}

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
