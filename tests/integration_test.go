// Integration tests: a real coordinator and real storage nodes talking over
// loopback TCP, exercised through the public client.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"distkv/internal/client"
	"distkv/internal/cluster"
	"distkv/internal/coordinator"
	"distkv/internal/protocol"
	"distkv/internal/testcluster"
)

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

func mustPut(t *testing.T, c *client.Client, k, v string) {
	t.Helper()
	if err := c.Put(ctx(t), k, []byte(v)); err != nil {
		t.Fatalf("put %s: %v", k, err)
	}
}

func mustGet(t *testing.T, c *client.Client, k, want string) {
	t.Helper()
	got, err := c.Get(ctx(t), k)
	if err != nil {
		t.Fatalf("get %s: %v", k, err)
	}
	if string(got) != want {
		t.Fatalf("get %s = %q, want %q", k, got, want)
	}
}

func mustMissing(t *testing.T, c *client.Client, k string) {
	t.Helper()
	if _, err := c.Get(ctx(t), k); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("get %s: want ErrNotFound, got %v", k, err)
	}
}

func replicasOf(tc *testcluster.Cluster, key string) []string {
	var out []string
	for _, r := range tc.Coord.Locate(key).Replicas {
		out = append(out, r.Addr)
	}
	return out
}

func stateOf(tc *testcluster.Cluster, addr string) cluster.State {
	for _, n := range tc.Coord.Stats(context.Background(), false).Nodes {
		if n.Addr == addr {
			return n.State
		}
	}
	return -1
}

func TestCRUD(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, CacheCapacity: 100})
	c := tc.Client()

	mustMissing(t, c, "nope")
	mustPut(t, c, "k1", "v1")
	mustGet(t, c, "k1", "v1")
	mustPut(t, c, "k1", "v2")
	mustGet(t, c, "k1", "v2")
	if err := c.Delete(ctx(t), "k1"); err != nil {
		t.Fatal(err)
	}
	mustMissing(t, c, "k1")
	// Deleting a missing key is fine.
	if err := c.Delete(ctx(t), "never-existed"); err != nil {
		t.Fatal(err)
	}
	// Arbitrary bytes: newlines, NULs, empty value.
	bin := []byte("line1\nline2\x00\r\n\xff")
	if err := c.Put(ctx(t), "bin\nkey", bin); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Get(ctx(t), "bin\nkey"); err != nil || !bytes.Equal(got, bin) {
		t.Fatalf("binary round trip: %q %v", got, err)
	}
	mustPut(t, c, "empty", "")
	mustGet(t, c, "empty", "")
}

func TestLimitsEnforcedEndToEnd(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 1})
	c := tc.Client()
	big := make([]byte, protocol.DefaultMaxValueSize+1)
	err := c.Put(ctx(t), "big", big)
	if err == nil {
		t.Fatal("oversized value accepted")
	}
	if err := c.Put(ctx(t), string(make([]byte, protocol.DefaultMaxKeySize+1)), nil); err == nil {
		t.Fatal("oversized key accepted")
	}
	mustPut(t, c, "ok", "still works")
}

func TestMalformedMessagesToCoordinator(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 2})
	nc, err := net.DialTimeout("tcp", tc.Addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.SetDeadline(time.Now().Add(5 * time.Second))
	protocol.WriteFrame(nc, []byte("GET key\n")) // text protocol is not accepted
	p, err := protocol.ReadFrame(nc, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := protocol.DecodeResponse(p)
	if r.Status != protocol.StatusBadRequest {
		t.Fatalf("want BAD_REQUEST, got %v %s", r.Status, r.Value)
	}
	// SCAN is node-internal; the coordinator must refuse it.
	protocol.WriteFrame(nc, protocol.AppendRequest(nil, &protocol.Request{Op: protocol.OpScan}))
	p, _ = protocol.ReadFrame(nc, 1<<20)
	r, _ = protocol.DecodeResponse(p)
	if r.Status != protocol.StatusBadRequest {
		t.Fatalf("want BAD_REQUEST for SCAN, got %v", r.Status)
	}
}

// TestReplicationPlacement checks every key lands on exactly its 3 replicas
// and that deletes propagate to all of them.
func TestReplicationPlacement(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4})
	c := tc.Client()
	nodes := make([]*client.Client, 4)
	for i := range nodes {
		nodes[i] = tc.NodeClient(i)
	}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%d", i)
		mustPut(t, c, k, "v")
	}
	// Writes are acknowledged after W=2; the third replica may lag slightly.
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%d", i)
		reps := replicasOf(tc, k)
		if len(reps) != 3 {
			t.Fatalf("replicas(%s) = %v", k, reps)
		}
		for j, addr := range tc.Addrs {
			_, err := nodes[j].Get(ctx(t), k)
			holds := err == nil
			shouldHold := contains(reps, addr)
			if holds != shouldHold {
				t.Fatalf("%s on %s: holds=%v, in replica set=%v (err %v)", k, addr, holds, shouldHold, err)
			}
		}
	}
	if err := c.Delete(ctx(t), "key-0"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	for j := range tc.Addrs {
		if _, err := nodes[j].Get(ctx(t), "key-0"); !errors.Is(err, client.ErrNotFound) {
			t.Fatalf("delete did not propagate to %s: %v", tc.Addrs[j], err)
		}
	}
	var st coordinator.Stats
	st = tc.Coord.Stats(ctx(t), true)
	var replOps int64
	for _, n := range st.Nodes {
		replOps += n.Stats.ReplicationOps
	}
	// 201 writes x 2 non-primary replicas.
	if replOps != 201*2 {
		t.Fatalf("replication ops = %d, want %d", replOps, 201*2)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestClusterSizes(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("nodes=%d", n), func(t *testing.T) {
			tc := testcluster.Start(t, testcluster.Options{Nodes: n, CacheCapacity: 50})
			c := tc.Client()
			for i := 0; i < 300; i++ {
				mustPut(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
			}
			for i := 0; i < 300; i++ {
				mustGet(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
			}
			wantRF := min(3, n)
			if got := len(replicasOf(tc, "k1")); got != wantRF {
				t.Fatalf("effective RF = %d, want %d", got, wantRF)
			}
			time.Sleep(50 * time.Millisecond)
			st := tc.Coord.Stats(ctx(t), true)
			total := 0
			for _, ns := range st.Nodes {
				total += ns.Stats.Storage.Keys
			}
			if total != 300*wantRF {
				t.Fatalf("total stored copies = %d, want %d", total, 300*wantRF)
			}
		})
	}
}

// TestPrimaryCrashFallback kills a node and checks every read and write
// keeps succeeding, both before and after failure detection marks it down.
func TestPrimaryCrashFallback(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, CacheCapacity: 1000})
	c := tc.Client()
	const n = 1000
	for i := 0; i < n; i++ {
		mustPut(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	victim := tc.Addrs[0]
	tc.Kill(0)

	// Immediately — before any health check has noticed.
	for i := 0; i < n; i++ {
		mustGet(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	tc.WaitFor(5*time.Second, "victim marked unhealthy", func() bool { return stateOf(tc, victim) == cluster.Unhealthy })
	for i := 0; i < n; i++ {
		mustPut(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d-new", i))
	}
	for i := 0; i < n; i++ {
		mustGet(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d-new", i))
	}
	st := tc.Coord.Stats(ctx(t), false)
	if st.Replication.ReadFallbacks == 0 {
		t.Fatal("expected read fallbacks to be counted")
	}
	if st.Health.UnhealthyNodes != 1 || st.Health.Status != "degraded" {
		t.Fatalf("health = %+v", st.Health)
	}
}

// TestHungNodeTimesOut simulates a frozen process (accepts, never answers).
// Reads whose primary is hung must fail over after the request timeout,
// and once detection marks it unhealthy they must stop paying that timeout.
func TestHungNodeTimesOut(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, Coord: func(c *coordinator.Config) {
		c.HealthInterval = 200 * time.Millisecond
	}})
	c := tc.Client()
	// Find a key whose primary is node 0.
	var key string
	for i := 0; ; i++ {
		key = fmt.Sprintf("k%d", i)
		if replicasOf(tc, key)[0] == tc.Addrs[0] {
			break
		}
	}
	mustPut(t, c, key, "value")
	tc.Hang(0)

	start := time.Now()
	mustGet(t, c, key, "value")
	first := time.Since(start)
	if first < 150*time.Millisecond {
		t.Logf("first read took %v (expected ~request timeout of 200ms)", first)
	}
	tc.WaitFor(5*time.Second, "hung node marked unhealthy", func() bool { return stateOf(tc, tc.Addrs[0]) == cluster.Unhealthy })
	start = time.Now()
	mustGet(t, c, key, "value")
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("read after detection took %v; unhealthy node should be skipped", d)
	}
	// Writes still meet quorum (2 of 3) with the hung replica.
	mustPut(t, c, key, "value2")
	mustGet(t, c, key, "value2")
}

func TestWriteQuorumNotMet(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3})
	c := tc.Client()
	mustPut(t, c, "k", "v")
	tc.Kill(0)
	tc.Kill(1)
	err := c.Put(ctx(t), "k", []byte("v2"))
	if err == nil {
		t.Fatal("write succeeded with only 1 of 3 replicas alive (W=2)")
	}
	t.Logf("quorum failure: %v", err)
	if st := tc.Coord.Stats(ctx(t), false); st.Replication.WriteQuorumFailures == 0 {
		t.Fatal("quorum failure not counted")
	}
}

// TestRestartResyncs: a node crashes, writes and deletes happen while it is
// down, it restarts empty, and resync must restore exactly the latest state
// of every key it replicates before it serves reads again.
func TestRestartResyncs(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, CacheCapacity: 100})
	c := tc.Client()
	const n = 2000
	for i := 0; i < n; i++ {
		mustPut(t, c, fmt.Sprintf("k%d", i), "old")
	}
	victim := tc.Addrs[1]
	tc.Kill(1)
	tc.WaitFor(5*time.Second, "victim unhealthy", func() bool { return stateOf(tc, victim) == cluster.Unhealthy })
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%d", i)
		if i%3 == 0 {
			if err := c.Delete(ctx(t), k); err != nil {
				t.Fatal(err)
			}
		} else {
			mustPut(t, c, k, "new")
		}
	}
	tc.Restart(1)
	tc.WaitFor(10*time.Second, "victim healthy again", func() bool { return stateOf(tc, victim) == cluster.Healthy })

	direct := tc.NodeClient(1)
	checked := 0
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%d", i)
		if !contains(replicasOf(tc, k), victim) {
			continue
		}
		checked++
		v, err := direct.Get(ctx(t), k)
		if i%3 == 0 {
			if !errors.Is(err, client.ErrNotFound) {
				t.Fatalf("%s: deleted while node was down, but node has %q (%v)", k, v, err)
			}
		} else if err != nil || string(v) != "new" {
			t.Fatalf("%s: node has %q (%v), want \"new\"", k, v, err)
		}
	}
	if checked < n/2 {
		t.Fatalf("only %d keys replicated to victim; expected ~3/4 of %d", checked, n)
	}
	// Reads through the coordinator are correct everywhere.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%d", i)
		if i%3 == 0 {
			mustMissing(t, c, k)
		} else {
			mustGet(t, c, k, "new")
		}
	}
}

// TestFastRestartDetectedByBootID: the node restarts faster than failure
// detection can notice. Only the boot-ID change reveals that it lost data.
func TestFastRestartDetectedByBootID(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Coord: func(c *coordinator.Config) {
		c.FailureThreshold = 1000 // never mark unhealthy via failures
	}})
	c := tc.Client()
	for i := 0; i < 500; i++ {
		mustPut(t, c, fmt.Sprintf("k%d", i), "v")
	}
	victim := tc.Addrs[0]
	tc.Kill(0)
	tc.Restart(0)
	tc.WaitFor(5*time.Second, "resync after boot ID change", func() bool {
		for _, n := range tc.Coord.Stats(context.Background(), false).Nodes {
			if n.Addr == victim {
				return n.State == cluster.Healthy && n.StateTransitions >= 2
			}
		}
		return false
	})
	direct := tc.NodeClient(0)
	for i := 0; i < 500; i++ {
		// 3 nodes, RF=3: every node holds every key.
		if v, err := direct.Get(ctx(t), fmt.Sprintf("k%d", i)); err != nil || string(v) != "v" {
			t.Fatalf("k%d missing after fast restart resync: %v", i, err)
		}
	}
}

// TestReadQuorumRepairsStaleReplica uses R=2: a read that sees divergent
// versions returns the newest and repairs the stale replica.
func TestReadQuorumRepairsStaleReplica(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Coord: func(c *coordinator.Config) {
		c.ReadQuorum = 2
	}})
	c := tc.Client()
	mustPut(t, c, "k", "v1")
	reps := replicasOf(tc, "k")
	// Write a newer version directly to the primary only (unversioned
	// direct writes get the node's clock, which is later than v1's).
	time.Sleep(time.Millisecond)
	primary := tc.NodeClient(tc.Index(reps[0]))
	if err := primary.Put(ctx(t), "k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	// Several reads: each hits R=2 of 3 replicas, the primary among them
	// at least in the replica set; the newest value must win.
	tc.WaitFor(5*time.Second, "read repair converges all replicas", func() bool {
		v, err := c.Get(context.Background(), "k")
		if err != nil || string(v) != "v2" {
			// R=2 may have picked the two stale replicas on this read.
			return false
		}
		for _, addr := range reps {
			v, err := tc.NodeClient(tc.Index(addr)).Get(context.Background(), "k")
			if err != nil || string(v) != "v2" {
				return false
			}
		}
		return true
	})
	if st := tc.Coord.Stats(ctx(t), false); st.Replication.ReadRepairs == 0 {
		t.Fatal("no read repairs counted")
	}
}

// TestAddNodeAtRuntime adds a fifth node: only ~1/5 of primaries move, the
// new node is populated by resync, and every key stays readable throughout.
func TestAddNodeAtRuntime(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4})
	c := tc.Client()
	const n = 3000
	before := map[string]string{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%d", i)
		mustPut(t, c, k, "v")
		before[k] = replicasOf(tc, k)[0]
	}
	addr := tc.StartExtraNode()
	if err := tc.Coord.AddNode(addr); err != nil {
		t.Fatal(err)
	}
	// Reads keep working while the new node is SYNCING.
	for i := 0; i < n; i += 7 {
		mustGet(t, c, fmt.Sprintf("k%d", i), "v")
	}
	tc.WaitFor(10*time.Second, "new node healthy", func() bool { return stateOf(tc, addr) == cluster.Healthy })
	moved := 0
	for k, old := range before {
		now := replicasOf(tc, k)[0]
		if now != old {
			moved++
			if now != addr {
				t.Fatalf("%s moved from %s to %s, not to the new node", k, old, now)
			}
		}
		mustGet(t, c, k, "v")
	}
	frac := float64(moved) / n
	t.Logf("primaries moved after adding 5th node: %d/%d (%.1f%%)", moved, n, frac*100)
	if frac < 0.10 || frac > 0.32 {
		t.Fatalf("moved fraction %.3f, expected about 0.2", frac)
	}
	if err := tc.Coord.AddNode(addr); err == nil {
		t.Fatal("adding the same node twice should fail")
	}
}

func TestConcurrentClients(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, CacheCapacity: 200})
	c := tc.Client()
	var wg sync.WaitGroup
	const workers, per = 32, 100
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				k := fmt.Sprintf("w%d-k%d", w, i)
				v := []byte(fmt.Sprintf("value-%d-%d", w, i))
				if err := c.Put(context.Background(), k, v); err != nil {
					t.Error(err)
					return
				}
				got, err := c.Get(context.Background(), k)
				if err != nil || !bytes.Equal(got, v) {
					t.Errorf("read-your-write %s: %q %v", k, got, err)
					return
				}
				if i%4 == 0 {
					if err := c.Delete(context.Background(), k); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}(w)
	}
	// Shared hot key contended by everyone.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				c.Put(context.Background(), "hot", []byte(fmt.Sprintf("%d", w)))
				c.Get(context.Background(), "hot")
			}
		}(w)
	}
	wg.Wait()
	for w := 0; w < workers; w++ {
		for i := 0; i < per; i++ {
			k := fmt.Sprintf("w%d-k%d", w, i)
			if i%4 == 0 {
				mustMissing(t, c, k)
			} else {
				mustGet(t, c, k, fmt.Sprintf("value-%d-%d", w, i))
			}
		}
	}
	// All replicas of the hot key must converge on the same value.
	time.Sleep(100 * time.Millisecond)
	var vals []string
	for _, addr := range replicasOf(tc, "hot") {
		v, err := tc.NodeClient(tc.Index(addr)).Get(ctx(t), "hot")
		if err != nil {
			t.Fatal(err)
		}
		vals = append(vals, string(v))
	}
	if vals[0] != vals[1] || vals[1] != vals[2] {
		t.Fatalf("replicas of hot key diverged: %v", vals)
	}
}

func TestStatsAndLocate(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, CacheCapacity: 100})
	c := tc.Client()
	mustPut(t, c, "a", "1")
	mustGet(t, c, "a", "1")
	mustGet(t, c, "a", "1")
	mustMissing(t, c, "b")

	raw, err := c.Stats(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	var st coordinator.Stats
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	if st.Gets != 3 || st.Puts != 1 || st.NotFound != 1 || st.Health.HealthyNodes != 4 {
		t.Fatalf("stats: gets=%d puts=%d notfound=%d health=%+v", st.Gets, st.Puts, st.NotFound, st.Health)
	}
	if st.Cache.Hits != 1 || st.Cache.Misses != 2 {
		t.Fatalf("cache totals %+v", st.Cache)
	}
	if st.Latency["GET"].Count != 3 {
		t.Fatalf("latency %+v", st.Latency)
	}
	raw, err = c.Locate(ctx(t), "a")
	if err != nil {
		t.Fatal(err)
	}
	var loc coordinator.Location
	if err := json.Unmarshal(raw, &loc); err != nil || len(loc.Replicas) != 3 || loc.Replicas[0].Role != "primary" {
		t.Fatalf("locate: %s %v", raw, err)
	}
}
