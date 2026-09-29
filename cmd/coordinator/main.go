// Command coordinator runs the request router.
//
//	coordinator --port 7100 --nodes localhost:7101,localhost:7102,localhost:7103,localhost:7104
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
	d := coordinator.DefaultConfig()
	fs := flag.NewFlagSet("coordinator", flag.ExitOnError)
	host := fs.String("host", "", "interface to bind (empty = all interfaces)")
	// Not 7000: macOS AirPlay Receiver listens there by default.
	port := fs.Int("port", 7100, "TCP port for client connections")
	httpPort := fs.Int("http-port", -1, "HTTP port for /health, /stats, /locate, /nodes (default port+1000, 0 disables)")
	nodes := fs.String("nodes", "localhost:7101,localhost:7102,localhost:7103,localhost:7104", "comma-separated storage node addresses")
	vnodes := fs.Int("vnodes", d.VirtualNodes, "virtual nodes per physical node")
	rf := fs.Int("replication", d.ReplicationN, "replication factor N")
	wq := fs.Int("write-quorum", d.WriteQuorum, "write quorum W (acks required)")
	rq := fs.Int("read-quorum", d.ReadQuorum, "read quorum R (1 = read from primary, fall back to replicas)")
	hi := fs.Duration("health-interval", d.HealthInterval, "health check interval")
	ht := fs.Duration("health-timeout", d.HealthTimeout, "health check timeout")
	ft := fs.Int("failure-threshold", d.FailureThreshold, "consecutive failures before a node is marked UNHEALTHY")
	dt := fs.Duration("dial-timeout", d.DialTimeout, "timeout for connecting to a storage node")
	rt := fs.Duration("request-timeout", d.RequestTimeout, "timeout for one request to one storage node")
	idle := fs.Duration("idle-timeout", 2*time.Minute, "close client connections idle this long")
	maxConns := fs.Int("max-conns", 4096, "maximum concurrent client connections")
	connsPerNode := fs.Int("conns-per-node", d.ConnsPerNode, "multiplexed TCP connections to each storage node")
	statsEvery := fs.Duration("log-stats-interval", 30*time.Second, "log a stats summary this often (0 disables)")
	limits := cmdutil.LimitFlags(fs)
	fs.Parse(os.Args[1:])

	logger := log.New(os.Stderr, "coordinator ", log.LstdFlags|log.Lmicroseconds)
	cmdutil.RaiseFileLimit(logger)

	cfg := d
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
	if len(cfg.Nodes) < cfg.ReplicationN {
		logger.Printf("note: %d nodes < replication factor %d; effective N=%d", len(cfg.Nodes), cfg.ReplicationN, len(cfg.Nodes))
		cfg.ReplicationN = len(cfg.Nodes)
		cfg.WriteQuorum = min(cfg.WriteQuorum, cfg.ReplicationN)
		cfg.ReadQuorum = min(cfg.ReadQuorum, cfg.ReplicationN)
	}

	c, err := coordinator.New(cfg)
	if err != nil {
		logger.Fatalf("config: %v", err)
	}
	c.Start()

	if *httpPort < 0 {
		*httpPort = *port + 1000
	}
	if *httpPort > 0 {
		admin := metrics.NewAdmin(
			func() (any, bool) { return c.Health() },
			func() any {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				return c.Stats(ctx, true)
			})
		admin.Mux.HandleFunc("GET /locate", func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			if key == "" {
				http.Error(w, "missing ?key=", http.StatusBadRequest)
				return
			}
			metrics.WriteJSON(w, http.StatusOK, c.Locate(key))
		})
		admin.Mux.HandleFunc("POST /nodes", func(w http.ResponseWriter, r *http.Request) {
			if err := c.AddNode(r.URL.Query().Get("addr")); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			metrics.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "added, syncing"})
		})
		addr := fmt.Sprintf("%s:%d", *host, *httpPort)
		if err := admin.Start(addr, logger); err != nil {
			logger.Fatalf("admin http: %v", err)
		}
		defer admin.Close()
		logger.Printf("admin HTTP on %s (/health, /stats, /locate?key=, POST /nodes?addr=)", addr)
	}

	ctx, stop := cmdutil.SignalContext()
	defer stop()
	go logStats(ctx, logger, c, *statsEvery)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	errc := make(chan error, 1)
	go func() { errc <- c.ListenAndServe(addr) }()
	logger.Printf("coordinator listening on %s", addr)

	select {
	case err := <-errc:
		logger.Fatalf("serve: %v", err)
	case <-ctx.Done():
	}
	logger.Printf("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Shutdown(sctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		logger.Printf("shutdown: %v", err)
	}
}

func logStats(ctx context.Context, logger *log.Logger, c *coordinator.Coordinator, every time.Duration) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := c.Stats(ctx, false)
			rate := float64(s.Requests-last) / every.Seconds()
			last = s.Requests
			logger.Printf("stats: %.0f req/s, errors=%d, nodes healthy=%d unhealthy=%d syncing=%d, GET p99=%dus PUT p99=%dus, fallbacks=%d",
				rate, s.Errors, s.Health.HealthyNodes, s.Health.UnhealthyNodes, s.Health.SyncingNodes,
				s.Latency["GET"].P99US, s.Latency["PUT"].P99US, s.Replication.ReadFallbacks)
		}
	}
}
