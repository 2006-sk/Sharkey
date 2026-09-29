// Integration tests: a real coordinator and real storage nodes talking over
// loopback TCP, exercised through the public client.
//
// Unit tests (in each internal/ package) prove that one component is correct
// in isolation: the ring routes deterministically, the LRU evicts in order,
// the registry's state machine has the right thresholds. They cannot prove the
// components *compose* correctly: that a write really lands on the three
// nodes the ring names, that a crashed node's keys stay readable, that a
// restarted node gets back exactly what it missed. These tests do, by
// building a whole cluster with internal/testcluster (in-process, but over
// real sockets) and checking end-to-end properties from the outside.
//
// Two observation points are used throughout:
//
//   - tc.Client() talks to the coordinator, i.e. what a user sees.
//   - tc.NodeClient(i) talks to one storage node directly, bypassing routing,
//     i.e. ground truth about what each replica actually stores.
//
// Timing: the cluster uses fast test timeouts (50ms health interval, 200ms
// request timeout; see testcluster.Start). Asynchronous outcomes are awaited
// with tc.WaitFor(condition) rather than fixed sleeps, except for the short
// sleeps that let the third, un-awaited replica write land (explained where
// they occur).
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

// ctx returns a context that expires after 30s and is cancelled when the test
// ends. It is a safety net: if a request hangs because of a bug, the test
// fails with a deadline error instead of blocking `go test` until its global
// timeout.
func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

// mustPut writes k=v through c and fails the test on any error.
// t.Helper() makes a failure point at the caller's line, which is the line
// that tells you which scenario step broke.
func mustPut(t *testing.T, c *client.Client, k, v string) {
	t.Helper()
	if err := c.Put(ctx(t), k, []byte(v)); err != nil {
		t.Fatalf("put %s: %v", k, err)
	}
}

// mustGet reads k through c and fails unless it succeeds with exactly want.
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

// mustMissing fails unless reading k returns client.ErrNotFound. Any other
// error (e.g. UNAVAILABLE) is a failure too: "missing" must be a definite
// answer from a replica, not the absence of an answer.
func mustMissing(t *testing.T, c *client.Client, k string) {
	t.Helper()
	if _, err := c.Get(ctx(t), k); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("get %s: want ErrNotFound, got %v", k, err)
	}
}

// replicasOf asks the coordinator's ring which nodes own key, in preference
// order: index 0 is the primary, then the other replicas. This is the
// coordinator's *intended* placement; tests compare it against what the
// nodes actually hold.
func replicasOf(tc *testcluster.Cluster, key string) []string {
	var out []string
	for _, r := range tc.Coord.Locate(key).Replicas {
		out = append(out, r.Addr)
	}
	return out
}

// stateOf returns the coordinator's current opinion of node addr (HEALTHY,
// UNHEALTHY or SYNCING), or -1 if the coordinator does not know the node.
// Stats(..., false) skips fetching per-node stats, so this is cheap enough to
// call from a WaitFor polling loop.
func stateOf(tc *testcluster.Cluster, addr string) cluster.State {
	for _, n := range tc.Coord.Stats(context.Background(), false).Nodes {
		if n.Addr == addr {
			return n.State
		}
	}
	return -1
}

// TestCRUD proves the basic contract of a key-value store through the full
// stack (client -> coordinator -> replicas -> back):
//
//   - reading a key that never existed is a clean NOT_FOUND, not an error;
//   - PUT then GET returns the value, and a second PUT overwrites it;
//   - DELETE makes the key NOT_FOUND, and deleting a missing key succeeds
//     (idempotent deletes make client retries safe);
//   - keys and values are arbitrary bytes. Because the protocol is
//     length-prefixed binary, not line-based text, newlines, NUL and 0xff
//     bytes must survive untouched. A text protocol that split on "\n"
//     would corrupt exactly these values;
//   - an empty value is a real, stored value, distinct from "missing".
func TestCRUD(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, CacheCapacity: 100})
	c := tc.Client()

	mustMissing(t, c, "nope")
	mustPut(t, c, "k1", "v1")
	mustGet(t, c, "k1", "v1")
	// Overwrite: the second write gets a newer version from the coordinator's
	// clock, so last-writer-wins on every replica keeps "v2". With the cache
	// enabled this also checks that the cached "v1" was not served stale.
	mustPut(t, c, "k1", "v2")
	mustGet(t, c, "k1", "v2")
	if err := c.Delete(ctx(t), "k1"); err != nil {
		t.Fatal(err)
	}
	// A delete is stored as a versioned tombstone, so the key reads as
	// missing everywhere.
	mustMissing(t, c, "k1")
	// Deleting a missing key is fine.
	if err := c.Delete(ctx(t), "never-existed"); err != nil {
		t.Fatal(err)
	}
	// Arbitrary bytes: newlines, NULs, empty value.
	bin := []byte("line1\nline2\x00\r\n\xff")
	// The key itself contains a newline too.
	if err := c.Put(ctx(t), "bin\nkey", bin); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Get(ctx(t), "bin\nkey"); err != nil || !bytes.Equal(got, bin) {
		t.Fatalf("binary round trip: %q %v", got, err)
	}
	mustPut(t, c, "empty", "")
	mustGet(t, c, "empty", "")
}

// TestLimitsEnforcedEndToEnd proves the configured size limits (1 MiB values,
// 1 KiB keys by default; protocol.DefaultMaxValueSize/DefaultMaxKeySize) are
// actually enforced on the real request path, and that a rejected request
// does not break anything: the next normal PUT on the same client succeeds.
// Without limits, one client could make a node allocate arbitrary memory;
// without the follow-up check, a limit violation might silently poison the
// connection and fail every later request.
func TestLimitsEnforcedEndToEnd(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 1})
	c := tc.Client()
	// One byte over the value limit. The whole frame (about 1 MiB) still fits
	// under the 4 MiB frame limit, so this exercises the per-field value check
	// rather than the frame-size check.
	big := make([]byte, protocol.DefaultMaxValueSize+1)
	err := c.Put(ctx(t), "big", big)
	if err == nil {
		t.Fatal("oversized value accepted")
	}
	// One byte over the key limit (the key is 1025 NUL bytes; content does
	// not matter, only length).
	if err := c.Put(ctx(t), string(make([]byte, protocol.DefaultMaxKeySize+1)), nil); err == nil {
		t.Fatal("oversized key accepted")
	}
	mustPut(t, c, "ok", "still works")
}

// TestMalformedMessagesToCoordinator speaks raw frames to the coordinator,
// below the client library, to prove two robustness properties:
//
//  1. A well-framed but garbage payload gets a BAD_REQUEST *response*; the
//     server does not crash or silently drop the connection. Framing (the
//     length prefix) is intact, so the server knows where the bad message
//     ends and can keep reading the next one.
//  2. The coordinator refuses SCAN. SCAN is a node-internal operation used by
//     resync to page through a node's whole store. If the coordinator passed
//     it through, any client could dump data from arbitrary nodes and bypass
//     routing. Coordinator.Handle's default case rejects every opcode it does
//     not explicitly support.
//
// Both requests go over the same connection, so the second check also proves
// the connection is still usable after the first malformed message.
func TestMalformedMessagesToCoordinator(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 2})
	// A plain TCP connection: no client library, no request IDs added for us.
	nc, err := net.DialTimeout("tcp", tc.Addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	// Bound the whole exchange so a server that never answers fails the test
	// instead of hanging it.
	nc.SetDeadline(time.Now().Add(5 * time.Second))
	// A valid frame whose payload is a text command, as a user might type
	// into telnet. It cannot be decoded as a binary request.
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
	// This time the payload is a correctly encoded request, so the refusal
	// is a policy decision, not a parse error.
	protocol.WriteFrame(nc, protocol.AppendRequest(nil, &protocol.Request{Op: protocol.OpScan}))
	p, _ = protocol.ReadFrame(nc, 1<<20)
	r, _ = protocol.DecodeResponse(p)
	if r.Status != protocol.StatusBadRequest {
		t.Fatalf("want BAD_REQUEST for SCAN, got %v", r.Status)
	}
}

// TestReplicationPlacement checks every key lands on exactly its 3 replicas
// and that deletes propagate to all of them.
//
// "Exactly" is checked in both directions for each of 200 keys and each of the
// 4 nodes: every node in the ring's replica set holds the key (no replica was
// skipped), and every node outside it does not (no copies sprayed to the
// wrong place, which would waste memory and could later serve stale data).
// Finally the per-node replication counters are checked for an exact total,
// which catches duplicate sends or retries that the value checks alone
// would miss.
func TestReplicationPlacement(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4})
	c := tc.Client()
	// Direct clients for all 4 nodes: the ground truth of what each stores.
	nodes := make([]*client.Client, 4)
	for i := range nodes {
		nodes[i] = tc.NodeClient(i)
	}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%d", i)
		mustPut(t, c, k, "v")
	}
	// Writes are acknowledged after W=2; the third replica may lag slightly.
	// (The coordinator returns once W acks, including the primary's, are in;
	// the remaining replica's write finishes in the background.)
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%d", i)
		reps := replicasOf(tc, k)
		// 4 nodes and N=3: the replica set must have exactly 3 distinct nodes.
		if len(reps) != 3 {
			t.Fatalf("replicas(%s) = %v", k, reps)
		}
		for j, addr := range tc.Addrs {
			// Any error (normally NOT_FOUND) means node j does not hold it.
			_, err := nodes[j].Get(ctx(t), k)
			holds := err == nil
			shouldHold := contains(reps, addr)
			if holds != shouldHold {
				t.Fatalf("%s on %s: holds=%v, in replica set=%v (err %v)", k, addr, holds, shouldHold, err)
			}
		}
	}
	// A delete is a replicated write of a tombstone; it must reach all three
	// replicas, not just the two that formed the quorum.
	if err := c.Delete(ctx(t), "key-0"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	// Non-replicas never had the key, so they also report NOT_FOUND; the
	// interesting assertions are on the 3 replicas.
	for j := range tc.Addrs {
		if _, err := nodes[j].Get(ctx(t), "key-0"); !errors.Is(err, client.ErrNotFound) {
			t.Fatalf("delete did not propagate to %s: %v", tc.Addrs[j], err)
		}
	}
	var st coordinator.Stats
	// includeNodes=true fetches each node's own counters.
	st = tc.Coord.Stats(ctx(t), true)
	var replOps int64
	for _, n := range st.Nodes {
		// ReplicationOps counts writes that arrived flagged FlagReplica,
		// i.e. those sent to a non-primary replica.
		replOps += n.Stats.ReplicationOps
	}
	// 201 writes x 2 non-primary replicas.
	// (200 PUTs + 1 DELETE; the primary's copy is not flagged, so it is not
	// counted.) An exact match proves each write was fanned out once per
	// replica: no duplicate sends, no retries, no missed replica.
	if replOps != 201*2 {
		t.Fatalf("replication ops = %d, want %d", replOps, 201*2)
	}
}

// contains reports whether s includes v.
func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// TestClusterSizes runs the same write/read workload on 1, 2, 4 and 8 nodes.
// It proves the system works at every size the scaling benchmark (Test A)
// uses, and in particular that the replication factor is capped at the node
// count: with N=3 requested, a 1-node cluster stores 1 copy and a 2-node
// cluster 2, rather than failing or storing two copies on the same node. The
// total-copies check (sum of keys over all nodes == keys x effective RF)
// proves every key is stored exactly RF times, no more and no fewer.
func TestClusterSizes(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8} {
		// Subtests get their own name ("nodes=4") and their own cluster, so
		// a failure pinpoints the size and one size cannot affect another.
		t.Run(fmt.Sprintf("nodes=%d", n), func(t *testing.T) {
			// A small cache (50 entries/node) with 300 keys also forces
			// evictions, so reads exercise both cache hits and misses.
			tc := testcluster.Start(t, testcluster.Options{Nodes: n, CacheCapacity: 50})
			c := tc.Client()
			for i := 0; i < 300; i++ {
				mustPut(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
			}
			for i := 0; i < 300; i++ {
				mustGet(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
			}
			// Effective replication factor = min(requested N, nodes).
			wantRF := min(3, n)
			if got := len(replicasOf(tc, "k1")); got != wantRF {
				t.Fatalf("effective RF = %d, want %d", got, wantRF)
			}
			// Let background (post-quorum) replica writes land before
			// counting stored copies.
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
//
// There are two distinct windows after a crash, and both must be covered:
//
//  1. Before detection: the coordinator still believes the node is HEALTHY
//     and routes reads to it as primary. Each such read gets a fast transport
//     error (connection refused/reset, see testcluster.Kill) and must fall
//     back to the next replica in preference order. Those same failures
//     feed passive failure detection: 3 consecutive failures mark the node
//     UNHEALTHY without waiting for a health check.
//  2. After detection: the node is skipped up front. Reads go straight to
//     the next replica; writes skip it and form the W=2 quorum from the two
//     surviving replicas.
//
// Zero client-visible errors across both windows is the availability promise
// of N=3 replication. The kill -9 run of the failure benchmark (Test D)
// measures the same scenario under load: 0 client errors, detection in
// ~15 ms (README).
func TestPrimaryCrashFallback(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, CacheCapacity: 1000})
	c := tc.Client()
	const n = 1000
	for i := 0; i < n; i++ {
		mustPut(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	// Node 0 is primary for roughly a quarter of the 1000 keys and a
	// replica for about three quarters.
	victim := tc.Addrs[0]
	tc.Kill(0)

	// Immediately — before any health check has noticed.
	// Window 1: every key must still read correctly via fallback.
	for i := 0; i < n; i++ {
		mustGet(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	tc.WaitFor(5*time.Second, "victim marked unhealthy", func() bool { return stateOf(tc, victim) == cluster.Unhealthy })
	// Window 2: overwrite every key with only 3 of 4 nodes alive. Keys that
	// had the victim in their replica set are written to 2 replicas, which
	// still meets W=2.
	for i := 0; i < n; i++ {
		mustPut(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d-new", i))
	}
	// Reads must return the new values, proving the writes really landed on
	// the replicas that now serve reads.
	for i := 0; i < n; i++ {
		mustGet(t, c, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d-new", i))
	}
	st := tc.Coord.Stats(ctx(t), false)
	// Fallbacks must have happened: otherwise the reads above did not go
	// through the code path this test claims to exercise.
	if st.Replication.ReadFallbacks == 0 {
		t.Fatal("expected read fallbacks to be counted")
	}
	// The cluster must report itself honestly: serving, but degraded.
	if st.Health.UnhealthyNodes != 1 || st.Health.Status != "degraded" {
		t.Fatalf("health = %+v", st.Health)
	}
}

// TestHungNodeTimesOut simulates a frozen process (accepts, never answers).
// Reads whose primary is hung must fail over after the request timeout,
// and once detection marks it unhealthy they must stop paying that timeout.
//
// This is the "SIGSTOP" counterpart of TestPrimaryCrashFallback. A crash is
// easy because failures are instant; a hang is hard because the only signal
// is silence (see testcluster.Hang). The two properties proved:
//
//  1. Correctness during the hang: a read whose primary is the hung node
//     still succeeds. It waits out the per-replica request timeout (200ms in
//     tests, 1s by default) and then falls back to the next replica.
//  2. The penalty is temporary: once timeouts (and failed health checks)
//     mark the node UNHEALTHY, reads skip it and are fast again. Without this,
//     every request touching the frozen node would pay the full timeout
//     forever. Test D in the README shows this as a ~1s throughput dip that
//     ends when the node is marked down.
func TestHungNodeTimesOut(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, Coord: func(c *coordinator.Config) {
		// Health checks every 200ms instead of 50ms, so the first read
		// below is very likely to happen before detection and really pays
		// the request timeout, exercising window 1.
		c.HealthInterval = 200 * time.Millisecond
	}})
	c := tc.Client()
	// Find a key whose primary is node 0.
	// With R=1 reads go to the primary first, so only a key whose primary
	// is the hung node is guaranteed to hit the hang.
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
	// Logged rather than asserted: the exact duration depends on goroutine
	// scheduling and on how far failure detection has progressed, and a
	// timing assertion here would make the test flaky. The correctness
	// assertion (mustGet succeeded) is the one that matters.
	if first < 150*time.Millisecond {
		t.Logf("first read took %v (expected ~request timeout of 200ms)", first)
	}
	tc.WaitFor(5*time.Second, "hung node marked unhealthy", func() bool { return stateOf(tc, tc.Addrs[0]) == cluster.Unhealthy })
	start = time.Now()
	mustGet(t, c, key, "value")
	// 100ms is half the request timeout: a read that still waited on the
	// hung node could not be this fast.
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("read after detection took %v; unhealthy node should be skipped", d)
	}
	// Writes still meet quorum (2 of 3) with the hung replica.
	// The UNHEALTHY primary is skipped rather than waited on, so the write
	// does not pay the timeout either, and the next read sees it.
	mustPut(t, c, key, "value2")
	mustGet(t, c, key, "value2")
}

// TestWriteQuorumNotMet proves the coordinator refuses to acknowledge a write
// it cannot make durable on W replicas. With 3 nodes (every key on all 3) and
// two of them killed, only 1 replica can accept the write, which is below
// W=2, so the client must get an error instead of a false "OK". Returning OK
// here would be a lie: losing the one surviving node would then lose an
// acknowledged write. The failure must also be visible in the coordinator's
// write_quorum_failures counter for operators.
//
// Note: failed writes are not rolled back (Dynamo-style quorums, see
// internal/replication), so the surviving replica may still hold "v2". The
// test asserts only what is promised: the write is not acknowledged.
func TestWriteQuorumNotMet(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3})
	c := tc.Client()
	mustPut(t, c, "k", "v")
	tc.Kill(0)
	tc.Kill(1)
	// Issued immediately, possibly before detection: the two dead replicas
	// then fail fast with transport errors, and the coordinator stops as
	// soon as W acks are impossible rather than waiting on them.
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
//
// The scenario, step by step:
//
//  1. Write 2000 keys = "old" (all replicas, including the victim, have them).
//  2. Kill node 1 and wait until it is UNHEALTHY, so the coordinator skips it
//     and it genuinely misses everything that follows.
//  3. While it is down: delete every third key, overwrite the rest with "new".
//  4. Restart it: same address, empty store, new boot ID. The next health
//     check succeeds, so the node moves UNHEALTHY -> SYNCING. Resync scans
//     the other readable nodes and pushes every entry whose replica set
//     includes the victim, tombstones included, then marks it HEALTHY.
//  5. Read the victim *directly* and check every key it replicates.
//
// Why each check matters:
//
//   - "new" on non-deleted keys proves resync delivered the writes the node
//     missed. Without resync the restarted node would be a silent hole: as
//     primary it would answer NOT_FOUND for keys that exist.
//   - NOT_FOUND on deleted keys proves resync did not resurrect "old" for
//     them. Deletes are versioned tombstones, and last-writer-wins by version
//     means an older value can never overwrite a newer delete.
//   - checked >= n/2 guards against a vacuous pass: with 4 nodes and RF=3 the
//     victim should replicate about 3/4 of all keys, so if (almost) none were
//     checked, the loop proved nothing.
//
// Finally, all reads through the coordinator are checked too, since the
// restarted node is now back in the read path.
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
	// Mutations the victim will miss: 1/3 deletes, 2/3 overwrites.
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
	// HEALTHY is only reached after resync completes (SYNCING -> HEALTHY),
	// so this wait covers detection, resync and promotion.
	tc.WaitFor(10*time.Second, "victim healthy again", func() bool { return stateOf(tc, victim) == cluster.Healthy })

	// Read the restarted node directly: the coordinator's answers could be
	// correct via other replicas even if the victim were empty.
	direct := tc.NodeClient(1)
	checked := 0
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%d", i)
		// Only keys the victim is supposed to replicate.
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
//
// This is the subtle case the boot ID exists for. Health checks alone only
// see "did the node answer?". If a process crashes and restarts between two
// checks (a supervisor restarting it within milliseconds, say), every check
// succeeds, the node is never marked UNHEALTHY, and the coordinator would keep
// routing primary reads to an *empty* node: silent data loss, with NOT_FOUND
// for keys that exist. Each node incarnation generates a random boot ID and
// reports it in its health response. The coordinator remembers the last one,
// and a change means "different process, memory wiped", which forces
// HEALTHY -> SYNCING -> resync -> HEALTHY.
//
// To make sure the boot ID is the *only* thing that can trigger the resync,
// the failure threshold is set absurdly high, so no number of failed requests
// or health checks during the brief outage can mark the node UNHEALTHY.
func TestFastRestartDetectedByBootID(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Coord: func(c *coordinator.Config) {
		c.FailureThreshold = 1000 // never mark unhealthy via failures
	}})
	c := tc.Client()
	for i := 0; i < 500; i++ {
		mustPut(t, c, fmt.Sprintf("k%d", i), "v")
	}
	victim := tc.Addrs[0]
	// Kill and restart back to back: the outage is shorter than one 50ms
	// health interval.
	tc.Kill(0)
	tc.Restart(0)
	tc.WaitFor(5*time.Second, "resync after boot ID change", func() bool {
		for _, n := range tc.Coord.Stats(context.Background(), false).Nodes {
			if n.Addr == victim {
				// The node starts HEALTHY (set without a transition), so
				// ending HEALTHY with >= 2 transitions means it went
				// HEALTHY -> SYNCING -> HEALTHY, i.e. a resync happened and
				// finished. Checking State alone would pass trivially,
				// before the boot-ID change is even noticed.
				return n.State == cluster.Healthy && n.StateTransitions >= 2
			}
		}
		return false
	})
	direct := tc.NodeClient(0)
	for i := 0; i < 500; i++ {
		// 3 nodes, RF=3: every node holds every key.
		// So the restarted node must have regained all 500.
		if v, err := direct.Get(ctx(t), fmt.Sprintf("k%d", i)); err != nil || string(v) != "v" {
			t.Fatalf("k%d missing after fast restart resync: %v", i, err)
		}
	}
}

// TestReadQuorumRepairsStaleReplica uses R=2: a read that sees divergent
// versions returns the newest and repairs the stale replica.
//
// Setup: all three replicas hold "v1". Then "v2" is written *directly* to the
// primary, bypassing the coordinator, so the replicas diverge (one new, two
// stale), much like a write that reached only one replica.
//
// With R=2 the coordinator sends the GET to every readable replica and uses
// the first two answers. If the primary's answer is among them, the highest
// version ("v2") wins, and any replica that answered with an older version is
// repaired: the coordinator pushes the winning version to it in the
// background. This is read repair: replicas converge as a side effect of
// reads, without a separate anti-entropy pass.
//
// Because the two fastest answers may both come from stale replicas, one read
// may return "v1", and one read repairs only the replicas that answered. The
// test therefore polls until a read returns "v2" *and* all three replicas,
// read directly, hold "v2", then checks that repairs were actually counted.
func TestReadQuorumRepairsStaleReplica(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Coord: func(c *coordinator.Config) {
		c.ReadQuorum = 2
	}})
	c := tc.Client()
	mustPut(t, c, "k", "v1")
	reps := replicasOf(tc, "k")
	// Write a newer version directly to the primary only (unversioned
	// direct writes get the node's clock, which is later than v1's).
	// Versions are wall-clock nanoseconds, and the node's clock has also
	// observed v1's version, so its next version is strictly larger. The 1ms
	// sleep just makes that obvious.
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
		// Converged only when every replica, read directly, has v2.
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
//
// This is consistent hashing's headline property, tested on a live cluster
// instead of the offline experiment (Test E, internal/bench/rebalance.go):
//
//   - Minimal movement: going from 4 to 5 nodes, the ideal is that the new
//     node takes a fair share, 1/5 = 20% of keys, and nothing else moves.
//     With `hash % N` about 80% would move instead (README, Test E), meaning
//     a near-total reshuffle of data.
//   - Movement only *to* the new node: every key whose primary changed must
//     now have the new node as primary. A key moving between two old nodes
//     would be pure waste.
//   - Availability during the change: the new node starts in SYNCING, so it
//     receives writes but not reads until resync fills it. Reads for keys it
//     now owns fall back to the other replicas, which still have the data.
//   - Duplicate registration is rejected, so membership stays a set.
func TestAddNodeAtRuntime(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4})
	c := tc.Client()
	const n = 3000
	// Remember each key's primary before the change.
	before := map[string]string{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%d", i)
		mustPut(t, c, k, "v")
		before[k] = replicasOf(tc, k)[0]
	}
	addr := tc.StartExtraNode()
	// Registers the node as SYNCING, adds it to the ring (so ownership
	// shifts immediately), and triggers a resync.
	if err := tc.Coord.AddNode(addr); err != nil {
		t.Fatal(err)
	}
	// Reads keep working while the new node is SYNCING.
	// (Every 7th key is sampled, to check quickly while the resync is
	// probably still running.)
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
		// Keys whose primary is now the new node are served by it, so this
		// also proves resync populated it.
		mustGet(t, c, k, "v")
	}
	frac := float64(moved) / n
	t.Logf("primaries moved after adding 5th node: %d/%d (%.1f%%)", moved, n, frac*100)
	// Wide bounds around 0.2: with 128 vnodes per node the new node's share
	// varies by a few percent (Test E measured 21.5%). The upper bound is
	// still far below modulo hashing's ~80%.
	if frac < 0.10 || frac > 0.32 {
		t.Fatalf("moved fraction %.3f, expected about 0.2", frac)
	}
	if err := tc.Coord.AddNode(addr); err == nil {
		t.Fatal("adding the same node twice should fail")
	}
}

// TestConcurrentClients runs 64 goroutines sharing one client (and therefore
// its pooled, multiplexed connections to the coordinator) to prove two
// properties under concurrency, ideally under `go test -race` (make race):
//
//  1. Read-your-writes per key: each of 32 workers PUTs its own keys and
//     immediately GETs them back. That holds because a write is acknowledged
//     only after the primary has applied it, and R=1 reads go to the primary.
//     Every 4th key is then deleted, and a final pass checks all 3200 keys
//     for the exact expected final state (value or NOT_FOUND).
//  2. Convergence under contention: 32 more goroutines hammer one "hot" key
//     with conflicting PUTs. Which value wins is unspecified, but all three
//     replicas must end up with the *same* one. Every write carries a version
//     from the coordinator's clock, and nodes apply last-writer-wins by
//     version, so the arrival order at each replica does not matter.
//
// A lost update, a response delivered to the wrong caller, or a replica that
// applied writes in arrival order instead of version order would all fail it.
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
				// Keys are private to worker w, so the expected value is known.
				k := fmt.Sprintf("w%d-k%d", w, i)
				v := []byte(fmt.Sprintf("value-%d-%d", w, i))
				// t.Error (not Fatal): Fatal must not be called from a
				// goroutine other than the test's own.
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
	// Errors are ignored: this half of the test is about the final state,
	// checked after wg.Wait.
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
	// Final state of every private key: deleted ones missing, others intact.
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
	// Wait briefly for the last background (post-quorum) replica writes,
	// then read each replica directly.
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

// TestStatsAndLocate proves the observability endpoints report exact numbers,
// which matters because the benchmark trusts them: cache hit rates and
// frames-per-write in the README are deltas of these very counters.
//
// The workload is tiny so every counter can be predicted:
//
//   - 1 PUT "a", GET "a" twice, GET "b" (missing) once
//     => gets=3, puts=1, notFound=1, and 3 GET latency samples.
//   - Cache: PUT does not write-allocate (it does not insert into the
//     cache), and R=1 reads touch only the primary's cache. So the first
//     GET "a" is a miss that fills the cache (read-through), the second is
//     a hit, and GET "b" is a miss => hits=1, misses=2.
//
// LOCATE must return the full replica set (3 of 4 nodes) with the primary
// first, since tools like `client locate` and verify-replication rely on it.
func TestStatsAndLocate(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 4, CacheCapacity: 100})
	c := tc.Client()
	mustPut(t, c, "a", "1")
	mustGet(t, c, "a", "1")
	mustGet(t, c, "a", "1")
	mustMissing(t, c, "b")

	// Fetched over the wire as JSON, exactly as `bin/client stats` and the
	// benchmark see it, rather than by calling tc.Coord.Stats directly.
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
	// Cluster-wide cache totals, summed over the nodes.
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
