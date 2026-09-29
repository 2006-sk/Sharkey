// Command coordinator runs the request router.
//
//	coordinator --port 7100 --nodes localhost:7101,localhost:7102,localhost:7103,localhost:7104
//
// The coordinator is the single entry point clients talk to. For every key it
// consults a consistent-hash ring (with --vnodes virtual nodes per storage
// node) to find the key's N replicas (--replication), sends reads to the
// primary (falling back to replicas) and writes to all replicas, answering
// once W of them (--write-quorum) have acknowledged. A background health
// checker pings every node each --health-interval; --failure-threshold
// consecutive failures mark a node UNHEALTHY, and a recovered node is
// resynchronised before it serves reads again.
//
// This file is only the command-line shell around internal/coordinator:
//
//  1. Define flags, with defaults taken from coordinator.DefaultConfig() so
//     that the library and the CLI can never disagree on a default.
//  2. Build the Config, clamping N/W/R when there are fewer nodes than N.
//  3. Start the coordinator (initial health check + health loop), the
//     optional HTTP admin server, and the TCP listener.
//  4. Wait for SIGINT/SIGTERM and shut down gracefully.
//
// Design note on the CLI: every tuning knob is a flag with a working default,
// so `bin/coordinator` with no arguments talks to the standard 4-node local
// cluster. Scripts and docker-compose override only what they need.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"distkv/internal/cmdutil"
	"distkv/internal/coordinator"
	"distkv/internal/metrics"
)

func main() {
	// Library defaults (vnodes=128, N=3, W=2, R=1, health 2s/500ms, ...).
	// They are used as flag defaults below, so `-h` shows the real values.
	d := coordinator.DefaultConfig()
	// Named FlagSet; ExitOnError prints usage and exits 2 on a bad flag.
	fs := flag.NewFlagSet("coordinator", flag.ExitOnError)
	// Bind address; empty = all interfaces (needed inside Docker).
	host := fs.String("host", "", "interface to bind (empty = all interfaces)")
	// Not 7000: macOS AirPlay Receiver listens there by default.
	port := fs.Int("port", 7100, "TCP port for client connections")
	// -1 is the "unset" sentinel, resolved to port+1000 after parsing
	// (a default cannot refer to another flag's parsed value); 0 disables.
	httpPort := fs.Int("http-port", -1, "HTTP port for /health, /stats, /locate, /nodes (default port+1000, 0 disables)")
	// The initial cluster membership, as one comma-separated string. More
	// nodes can be added at runtime through POST /nodes (see below).
	nodes := fs.String("nodes", "localhost:7101,localhost:7102,localhost:7103,localhost:7104", "comma-separated storage node addresses")
	// --- hash ring and replication ---
	// Virtual nodes per physical node. More vnodes = more even key spread
	// (Test E: 128 vnodes keep the busiest of 4 nodes within ~4% of ideal).
	vnodes := fs.Int("vnodes", d.VirtualNodes, "virtual nodes per physical node")
	// N: how many distinct nodes store each key.
	rf := fs.Int("replication", d.ReplicationN, "replication factor N")
	// W: how many replica acknowledgements a PUT/DELETE waits for.
	wq := fs.Int("write-quorum", d.WriteQuorum, "write quorum W (acks required)")
	// R: how many replicas a GET consults.
	rq := fs.Int("read-quorum", d.ReadQuorum, "read quorum R (1 = read from primary, fall back to replicas)")
	// --- failure detection ---
	// How often each node is pinged, and how long one ping may take.
	hi := fs.Duration("health-interval", d.HealthInterval, "health check interval")
	ht := fs.Duration("health-timeout", d.HealthTimeout, "health check timeout")
	// Consecutive failures (health checks or requests) needed to declare a
	// node down. More than 1 avoids flapping on a single slow response.
	ft := fs.Int("failure-threshold", d.FailureThreshold, "consecutive failures before a node is marked UNHEALTHY")
	// --- node RPC timeouts ---
	dt := fs.Duration("dial-timeout", d.DialTimeout, "timeout for connecting to a storage node")
	// The request timeout sets the length of the SIGSTOP dip in Test D: a
	// frozen node is only suspected after requests to it time out.
	rt := fs.Duration("request-timeout", d.RequestTimeout, "timeout for one request to one storage node")
	// --- client-facing server ---
	idle := fs.Duration("idle-timeout", 2*time.Minute, "close client connections idle this long")
	maxConns := fs.Int("max-conns", 4096, "maximum concurrent client connections")
	// Multiplexed connections per node. The README found one connection per
	// node batches best (more frames per write syscall).
	connsPerNode := fs.Int("conns-per-node", d.ConnsPerNode, "multiplexed TCP connections to each storage node")
	statsEvery := fs.Duration("log-stats-interval", 30*time.Second, "log a stats summary this often (0 disables)")
	// Shared size-limit flags; the pointer is filled in by Parse.
	limits := cmdutil.LimitFlags(fs)
	// Skip os.Args[0], the program name.
	fs.Parse(os.Args[1:])

	// Microsecond timestamps help line logs up with benchmark timelines.
	logger := log.New(os.Stderr, "coordinator ", log.LstdFlags|log.Lmicroseconds)
	// The coordinator holds one connection per client plus node connections,
	// so the default macOS limit of 256 descriptors would be far too low.
	cmdutil.RaiseFileLimit(logger)

	// cfg is a copy of the defaults (Config is a value type), which is then
	// overwritten field by field from the parsed flags.
	cfg := d
	// Split the node list, tolerating spaces and empty entries such as a
	// trailing comma ("a:1, b:2,").
	for _, n := range strings.Split(*nodes, ",") {
		if n = strings.TrimSpace(n); n != "" {
			cfg.Nodes = append(cfg.Nodes, n)
		}
	}
	cfg.VirtualNodes, cfg.ReplicationN, cfg.WriteQuorum, cfg.ReadQuorum = *vnodes, *rf, *wq, *rq
	cfg.HealthInterval, cfg.HealthTimeout, cfg.FailureThreshold = *hi, *ht, *ft
	cfg.DialTimeout, cfg.RequestTimeout, cfg.ConnsPerNode = *dt, *rt, *connsPerNode
	cfg.Server.Limits = *limits
	cfg.Server.IdleTimeout = *idle
	cfg.Server.MaxConns = *maxConns
	cfg.Logger = logger

	// With fewer nodes than N, every node holds every key; clamp quorums so
	// e.g. a 1-node dev cluster works with default flags.
	//
	// Without the clamp, N=3 W=2 on one node could never collect 2 acks, so
	// every write would fail with "quorum not met". This is also why the
	// Test A table shows "RF=3 (effective 1)" for the 1-node configuration.
	if len(cfg.Nodes) < cfg.ReplicationN {
		logger.Printf("note: %d nodes < replication factor %d; effective N=%d", len(cfg.Nodes), cfg.ReplicationN, len(cfg.Nodes))
		cfg.ReplicationN = len(cfg.Nodes)
		cfg.WriteQuorum = min(cfg.WriteQuorum, cfg.ReplicationN)
		cfg.ReadQuorum = min(cfg.ReadQuorum, cfg.ReplicationN)
	}

	// New validates the configuration (e.g. W > N is an error) and builds the
	// ring and node registry. A config error is fatal: better to refuse to
	// start than to run with settings that cannot work.
	c, err := coordinator.New(cfg)
	if err != nil {
		logger.Fatalf("config: %v", err)
	}
	// Start runs one synchronous health check of every node (so the first
	// requests already know which nodes are up) and then launches the
	// periodic health-check loop in the background.
	c.Start()

	// Resolve the sentinel: 7100 -> 8100.
	if *httpPort < 0 {
		*httpPort = *port + 1000
	}
	if *httpPort > 0 {
		// The admin server gets two callbacks:
		//   health: c.Health() returns (doc, ok) with ok = "at least one node
		//           is HEALTHY"; ok=false turns /health into HTTP 503, which is
		//           what load balancers and readiness checks look for.
		//   stats:  the full stats document including per-node stats, which
		//           requires asking every node; the 2 s timeout keeps a hung
		//           node from hanging the HTTP request.
		admin := metrics.NewAdmin(
			func() (any, bool) { return c.Health() },
			func() any {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				return c.Stats(ctx, true)
			})
		// GET /locate?key=k -> which nodes hold k (primary first) and their
		// states. Uses Go 1.22+ method-and-path patterns in ServeMux.
		admin.Mux.HandleFunc("GET /locate", func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			// 400 for a missing parameter: it is the caller's mistake.
			if key == "" {
				http.Error(w, "missing ?key=", http.StatusBadRequest)
				return
			}
			metrics.WriteJSON(w, http.StatusOK, c.Locate(key))
		})
		// POST /nodes?addr=host:port -> add a storage node at runtime. POST
		// because it changes state. The node enters as SYNCING (receives
		// writes, no reads) until resync copies its share of the data, hence
		// 202 Accepted: the request was accepted, the work is still going on.
		admin.Mux.HandleFunc("POST /nodes", func(w http.ResponseWriter, r *http.Request) {
			if err := c.AddNode(r.URL.Query().Get("addr")); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			metrics.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "added, syncing"})
		})
		addr := fmt.Sprintf("%s:%d", *host, *httpPort)
		// Start binds synchronously, so "address in use" is fatal right here.
		if err := admin.Start(addr, logger); err != nil {
			logger.Fatalf("admin http: %v", err)
		}
		defer admin.Close()
		logger.Printf("admin HTTP on %s (/health, /stats, /locate?key=, POST /nodes?addr=)", addr)
	}

	// Cancelled on the first SIGINT/SIGTERM; stop() restores default signal
	// handling so a second Ctrl-C terminates immediately.
	ctx, stop := cmdutil.SignalContext()
	defer stop()
	go logStats(ctx, logger, c, *statsEvery)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	// Serve in a goroutine; the buffered channel lets it deliver its error
	// and exit even if main is not listening any more (no goroutine leak).
	errc := make(chan error, 1)
	go func() { errc <- c.ListenAndServe(addr) }()
	logger.Printf("coordinator listening on %s", addr)

	// Block until the listener fails (fatal) or a signal asks us to stop.
	select {
	case err := <-errc:
		logger.Fatalf("serve: %v", err)
	case <-ctx.Done():
	}
	logger.Printf("shutting down")
	// Fresh 5 s context (ctx is already cancelled). Shutdown stops accepting,
	// lets in-flight client requests complete, then stops the health checker
	// and closes the node connections.
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Running out of the 5 s budget is expected with slow clients; only
	// report other errors.
	if err := c.Shutdown(sctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		logger.Printf("shutdown: %v", err)
	}
}

// logStats prints one summary line every `every` (0 disables it). As on the
// storage node, a periodic summary replaces per-request logging, which at
// tens of thousands of requests per second would cost a syscall per request
// and distort the benchmark. The line shows request rate, errors, node
// states, client-observed GET/PUT p99 and how many reads fell back to a
// replica, i.e. the numbers an operator needs to spot a failure.
func logStats(ctx context.Context, logger *log.Logger, c *coordinator.Coordinator, every time.Duration) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	// Previous value of the cumulative request counter, for a per-interval
	// rate.
	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// includeNodes=false: coordinator-local counters only, so this
			// never makes network calls to the storage nodes.
			s := c.Stats(ctx, false)
			rate := float64(s.Requests-last) / every.Seconds()
			last = s.Requests
			logger.Printf("stats: %.0f req/s, errors=%d, nodes healthy=%d unhealthy=%d syncing=%d, GET p99=%dus PUT p99=%dus, fallbacks=%d",
				rate, s.Errors, s.Health.HealthyNodes, s.Health.UnhealthyNodes, s.Health.SyncingNodes,
				s.Latency["GET"].P99US, s.Latency["PUT"].P99US, s.Replication.ReadFallbacks)
		}
	}
}
