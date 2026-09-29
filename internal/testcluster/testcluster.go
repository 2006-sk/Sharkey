// Package testcluster starts an in-process coordinator and storage nodes on
// loopback ports for integration tests. Everything runs over real TCP.
//
// Why in-process?
//
// A "real" cluster (scripts/run_cluster.sh, scripts/validate.sh) needs built
// binaries, fixed ports, pid files and seconds of start-up per scenario. Here
// every node is just a *node.Node value serving on a net.Listener inside the
// test binary, so a whole 4-node cluster starts in milliseconds and a test
// file can build dozens of them. Nothing is mocked on the wire, though: the
// coordinator dials the nodes over 127.0.0.1 TCP and tests talk to the
// coordinator through the public client, so the length-prefixed protocol,
// the multiplexed transport, timeouts and failure detection all run exactly
// as in production.
//
// Being in the same process also gives tests two powers a shell script lacks:
//
//   - They can inspect the coordinator directly (tc.Coord.Locate, Stats,
//     AddNode) to compute the expected answer, e.g. "which nodes should hold
//     this key?".
//   - They can inject faults deterministically and instantly: Kill (a crash),
//     Hang (a frozen process) and Restart (a fresh, empty incarnation on the
//     same address).
//
// The price: a Kill is not literally a kill -9 of a process (the Go objects
// still exist until garbage-collected). What matters for correctness is what
// peers observe on the network, and Kill/Hang reproduce exactly that. The
// real-process version of these scenarios lives in scripts/validate.sh and the
// failure benchmark (scripts/bench_failure.sh).
package testcluster

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"distkv/internal/client"
	"distkv/internal/coordinator"
	"distkv/internal/node"
)

// Cluster is a running test cluster.
//
// Index i is the identity of a storage node for the whole test: Addrs[i] never
// changes, even across Kill/Hang/Restart, while Nodes[i] is replaced by a new
// *node.Node on Restart. That mirrors reality, where the coordinator knows a
// node only by its host:port and a restarted process reuses the same port.
//
// Fields:
//
//   - t: the owning test; used for Fatal on setup errors and to register
//     Cleanup functions so nothing leaks between tests.
//   - Addrs: storage node addresses ("127.0.0.1:<port>"), one per node index.
//     They are the coordinator's membership keys.
//   - Nodes: the current incarnation of each storage node.
//   - Coord: the in-process coordinator. Tests call its methods directly
//     (Locate, Stats, AddNode) to observe or change cluster state.
//   - Addr: where clients connect. Tests use Client() rather than dialing it
//     by hand, except when they deliberately send malformed frames.
//   - Log: shared by all components; it discards output unless the
//     DISTKV_TEST_LOG environment variable is set (see Start).
//   - nodeCfg: remembered so Restart and StartExtraNode build nodes with the
//     same settings (cache capacity, logger) as the originals.
//   - mu: guards hung and hungCon, which the accept goroutine of Hang appends
//     to concurrently with Restart/Close.
//   - hung: node index -> the black-hole listener currently squatting on that
//     node's address (see Hang).
//   - hungCon: every connection the black hole accepted. Holding a reference
//     keeps them open (never read, never written, never closed) until Close,
//     which is exactly what a frozen process looks like to its peers.
type Cluster struct {
	t     testing.TB
	Addrs []string // storage node addresses, stable across restarts
	Nodes []*node.Node
	Coord *coordinator.Coordinator
	Addr  string // coordinator address
	Log   *log.Logger

	nodeCfg node.Config
	mu      sync.Mutex
	hung    map[int]net.Listener
	hungCon []net.Conn
}

// Options tweak the cluster.
//
//   - Nodes: the number of storage nodes to start.
//   - CacheCapacity: the per-node LRU size in entries; 0 disables the cache.
//   - Coord: if set, may override any coordinator setting after the fast test
//     defaults are applied, e.g. ReadQuorum = 2 or a slower HealthInterval
//     for one scenario.
type Options struct {
	Nodes         int
	CacheCapacity int
	Coord         func(*coordinator.Config)
}

// Start launches opts.Nodes storage nodes and a coordinator with fast health
// checking suitable for tests.
//
// Order matters: the nodes are listening before the coordinator is created,
// because coordinator.Start runs one synchronous health round. That round
// finds every node reachable and records each node's boot ID, so the cluster
// is fully HEALTHY by the time Start returns and the test can issue requests
// immediately without sleeping.
func Start(t testing.TB, opts Options) *Cluster {
	// Report failures at the caller's line, not inside this helper.
	t.Helper()
	// Components log a lot (state transitions, resyncs). Silence them by
	// default so `go test` output stays readable; set DISTKV_TEST_LOG=1 to
	// see a microsecond-stamped trace when debugging a failing test.
	logger := log.New(io.Discard, "", 0)
	if os.Getenv("DISTKV_TEST_LOG") != "" {
		logger = log.New(os.Stderr, "", log.Lmicroseconds)
	}
	ncfg := node.DefaultConfig()
	ncfg.CacheCapacity = opts.CacheCapacity
	ncfg.Logger = logger
	c := &Cluster{t: t, Log: logger, nodeCfg: ncfg, hung: map[int]net.Listener{}}
	for i := 0; i < opts.Nodes; i++ {
		// Port 0 asks the kernel for any free ephemeral port. No hard-coded
		// ports means tests (and parallel `go test` packages) never collide
		// with each other or with a cluster the developer has running.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		n := node.New(ncfg)
		// Serve blocks in an accept loop, so it gets its own goroutine. It
		// returns when the node is shut down (Kill or Close).
		go n.Serve(ln)
		c.Nodes = append(c.Nodes, n)
		// Read back the concrete port the kernel chose.
		c.Addrs = append(c.Addrs, ln.Addr().String())
	}

	cfg := coordinator.DefaultConfig()
	cfg.Nodes = c.Addrs
	// Production defaults are a 2s health interval, 500ms health timeout,
	// 500ms dial timeout and 1s request timeout. Scaled down 5-40x here so
	// crash detection, hang timeouts and resync all complete in well under
	// a second and the whole suite stays fast. The ratios are preserved:
	// the health timeout is still shorter than the interval, and the
	// request timeout is still longer than the dial timeout.
	cfg.HealthInterval = 50 * time.Millisecond
	cfg.HealthTimeout = 100 * time.Millisecond
	cfg.RequestTimeout = 200 * time.Millisecond
	cfg.DialTimeout = 100 * time.Millisecond
	cfg.Logger = logger
	cfg.Server.Logger = logger
	// Per-test overrides are applied last so they win over the defaults.
	if opts.Coord != nil {
		opts.Coord(&cfg)
	}
	coord, err := coordinator.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Runs the first health round synchronously, then starts the periodic
	// health checker goroutine.
	coord.Start()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go coord.Serve(ln)
	c.Coord = coord
	c.Addr = ln.Addr().String()
	// Cleanup functions run in LIFO order when the test ends. Clients made
	// later by Client()/NodeClient() register their own Close after this
	// one, so they are closed first and the cluster is torn down last.
	t.Cleanup(c.Close)
	return c
}

// Client returns a client connected to the coordinator.
func (c *Cluster) Client() *client.Client {
	o := client.DefaultOptions()
	// Generous: a client request may legitimately wait for a replica's
	// 200ms request timeout (hung node) plus fallback. 5s only trips if
	// something is genuinely stuck, turning a hang into a clear failure.
	o.RequestTimeout = 5 * time.Second
	cl := client.New(c.Addr, o)
	c.t.Cleanup(cl.Close)
	return cl
}

// NodeClient returns a client talking directly to storage node i.
//
// Storage nodes speak the same protocol as the coordinator, so a client can
// bypass routing and ask one node "what do *you* hold?". Tests use this as
// ground truth for replica placement, delete propagation and resync.
func (c *Cluster) NodeClient(i int) *client.Client {
	o := client.DefaultOptions()
	o.RequestTimeout = time.Second
	cl := client.New(c.Addrs[i], o)
	c.t.Cleanup(cl.Close)
	return cl
}

// Index returns the index of the node with address addr, or -1.
// It translates the addresses returned by Coord.Locate back into node indexes
// usable with NodeClient, Kill, Hang and Restart.
func (c *Cluster) Index(addr string) int {
	for i, a := range c.Addrs {
		if a == addr {
			return i
		}
	}
	return -1
}

// Kill stops node i abruptly (closing its listener and connections), like a
// crashed process: peers see connection refused / reset.
//
// It uses the node's graceful Shutdown but with only a 10ms grace period:
// Shutdown closes the listener at once (new dials are refused by the kernel),
// wakes idle connections, and when the 10ms context expires it force-closes
// whatever connections remain. From the coordinator's side this is
// indistinguishable from a crash: in-flight and new requests fail *fast* with
// a transport error, which is what drives passive failure detection (three
// consecutive transport failures mark the node UNHEALTHY).
func (c *Cluster) Kill(i int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	c.Nodes[i].Shutdown(ctx)
}

// Hang replaces node i with a listener that accepts connections but never
// replies, like a frozen (SIGSTOPped) process: peers only see timeouts.
//
// Why a "black-hole listener" rather than just killing the node? The two
// failures look completely different on the wire:
//
//   - Closed port (crash): the kernel answers a SYN with RST, so dial fails
//     with "connection refused" in microseconds, and an established
//     connection is reset. The caller learns about the failure immediately.
//   - Frozen process (SIGSTOP, GC pause, deadlock, overloaded box): the
//     kernel still completes TCP handshakes and buffers incoming bytes, so
//     dial succeeds and writes succeed, but no response ever comes back.
//     The only way to notice is a timeout.
//
// A hang is the nastier case: every request routed to the node pays the full
// request timeout before falling back, and health checks only fail once
// their own timeout expires. The black hole reproduces exactly that: it
// accepts, then never reads or writes.
func (c *Cluster) Hang(i int) {
	// Free the address first: shut the real node down (its connections are
	// closed, which the coordinator sees as ordinary transport errors)...
	c.Kill(i)
	// ...then bind the same host:port again, so the coordinator's next dial
	// to that address lands in the black hole instead of being refused.
	ln := c.listen(c.Addrs[i])
	c.mu.Lock()
	c.hung[i] = ln
	c.mu.Unlock()
	go func() {
		for {
			// Accept returns an error once Restart or Close closes the
			// listener; that ends this goroutine.
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Keep the connection open and untouched. If it were dropped
			// (or garbage-collected and finalized), the peer would see EOF
			// or a reset, which is a crash, not a hang.
			c.mu.Lock()
			c.hungCon = append(c.hungCon, conn)
			c.mu.Unlock()
		}
	}()
}

// Restart starts a fresh (empty, new boot ID) node on node i's address.
//
// This models a process that crashed (or was hung) and came back: in-memory
// data is gone, and node.New generates a new random boot ID for the new
// incarnation. Reusing the same address is essential, because the coordinator
// identifies members by host:port. The coordinator therefore sees "the same
// node" answer health checks again and must detect that it lost its data:
// either because the node was UNHEALTHY and now answers (UNHEALTHY ->
// SYNCING), or, if it restarted too fast for any check to fail, because the
// boot ID in its health response changed. Both paths trigger a resync before
// the node serves reads again.
func (c *Cluster) Restart(i int) {
	// If the node was hung, close the black-hole listener so the port is
	// free. Connections the black hole already accepted stay open until
	// Close; the coordinator drops its own ends when it marks the node
	// unhealthy or syncing.
	c.mu.Lock()
	if ln, ok := c.hung[i]; ok {
		ln.Close()
		delete(c.hung, i)
	}
	c.mu.Unlock()
	// Rebind the original address (retrying briefly; see listen).
	ln := c.listen(c.Addrs[i])
	// Same config as the original node, but a brand-new, empty store.
	n := node.New(c.nodeCfg)
	go n.Serve(ln)
	c.Nodes[i] = n
}

// StartExtraNode starts a node on a new port without registering it with
// the coordinator, returning its address.
//
// Tests then call Coord.AddNode(addr) themselves, which is the operation under
// test (runtime membership change). The node is appended to Nodes/Addrs so it
// gets an index usable with NodeClient/Index and is shut down by Close.
func (c *Cluster) StartExtraNode() string {
	ln := c.listen("127.0.0.1:0")
	n := node.New(c.nodeCfg)
	go n.Serve(ln)
	c.Nodes = append(c.Nodes, n)
	c.Addrs = append(c.Addrs, ln.Addr().String())
	return ln.Addr().String()
}

// listen binds addr, retrying for up to ~1s (50 attempts x 20ms).
//
// When re-binding the address of a node that was just shut down, the old
// listener's close may not have fully released the port yet, and Listen fails
// with "address already in use". A short retry loop absorbs that race instead
// of making Restart/Hang flaky. If the port still cannot be bound, the test
// fails with a clear message.
func (c *Cluster) listen(addr string) net.Listener {
	var err error
	for i := 0; i < 50; i++ {
		var ln net.Listener
		if ln, err = net.Listen("tcp", addr); err == nil {
			return ln
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("listen %s: %v", addr, err)
	return nil
}

// WaitFor polls cond until it is true or timeout elapses.
//
// Distributed state changes (failure detection, resync, read repair) happen
// asynchronously, so tests must wait for them. Polling a condition every 10ms
// is both faster and less flaky than a fixed sleep: the test proceeds the
// moment the state is reached, and the generous timeout only matters when the
// system is actually broken, in which case `what` names the missing event.
func (c *Cluster) WaitFor(timeout time.Duration, what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Close shuts everything down.
//
// It is registered with t.Cleanup by Start, so tests never call it directly.
// The coordinator goes first: that stops its health checker and background
// resyncs, so they do not log failures against nodes that are about to
// disappear. Then every node incarnation is shut down, and finally the
// black-hole listeners and the connections they are holding open are closed
// so no goroutine or socket outlives the test.
func (c *Cluster) Close() {
	// One shared 1s budget for the whole teardown.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.Coord.Shutdown(ctx)
	for _, n := range c.Nodes {
		n.Shutdown(ctx)
	}
	c.mu.Lock()
	for _, ln := range c.hung {
		ln.Close()
	}
	for _, conn := range c.hungCon {
		conn.Close()
	}
	c.mu.Unlock()
}
