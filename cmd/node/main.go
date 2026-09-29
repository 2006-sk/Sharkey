// Command node runs a storage node.
//
//	node --port 7101 [--http-port 8101] [--cache-capacity 10000]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"distkv/internal/cmdutil"
	"distkv/internal/metrics"
	"distkv/internal/node"
)

func main() {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	host := fs.String("host", "", "interface to bind (empty = all interfaces)")
	port := fs.Int("port", 7101, "TCP port for the KV protocol")
	httpPort := fs.Int("http-port", -1, "HTTP port for /health and /stats (default port+1000, 0 disables)")
	cacheCap := fs.Int("cache-capacity", 10000, "LRU cache capacity in entries (0 disables the cache)")
	idle := fs.Duration("idle-timeout", 2*time.Minute, "close client connections idle this long")
	writeTimeout := fs.Duration("write-timeout", 5*time.Second, "deadline for writing one response")
	maxConns := fs.Int("max-conns", 4096, "maximum concurrent client connections")
	statsEvery := fs.Duration("log-stats-interval", 30*time.Second, "log a stats summary this often (0 disables)")
	limits := cmdutil.LimitFlags(fs)
	fs.Parse(os.Args[1:])

	logger := log.New(os.Stderr, fmt.Sprintf("node:%d ", *port), log.LstdFlags|log.Lmicroseconds)
	cmdutil.RaiseFileLimit(logger)

	cfg := node.DefaultConfig()
	cfg.CacheCapacity = *cacheCap
	cfg.Server.Limits = *limits
	cfg.Server.IdleTimeout = *idle
	cfg.Server.WriteTimeout = *writeTimeout
	cfg.Server.MaxConns = *maxConns
	cfg.Logger = logger
	n := node.New(cfg)

	if *httpPort < 0 {
		*httpPort = *port + 1000
	}
	if *httpPort > 0 {
		admin := metrics.NewAdmin(func() (any, bool) { return n.Health(), true }, func() any { return n.Stats() })
		addr := fmt.Sprintf("%s:%d", *host, *httpPort)
		if err := admin.Start(addr, logger); err != nil {
			logger.Fatalf("admin http: %v", err)
		}
		defer admin.Close()
		logger.Printf("admin HTTP on %s (/health, /stats)", addr)
	}

	ctx, stop := cmdutil.SignalContext()
	defer stop()
	go logStats(ctx, logger, n, *statsEvery)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	errc := make(chan error, 1)
	go func() { errc <- n.ListenAndServe(addr) }()
	logger.Printf("storage node listening on %s (boot_id=%s, cache=%d entries)", addr, n.BootID(), *cacheCap)

	select {
	case err := <-errc:
		logger.Fatalf("serve: %v", err)
	case <-ctx.Done():
	}
	logger.Printf("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Shutdown(sctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		logger.Printf("shutdown: %v", err)
	}
}

// logStats prints one summary line periodically instead of a line per
// request, so benchmarks don't drown in logs.
func logStats(ctx context.Context, logger *log.Logger, n *node.Node, every time.Duration) {
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
			s := n.Stats()
			rate := float64(s.Requests-last) / every.Seconds()
			last = s.Requests
			logger.Printf("stats: %.0f req/s, keys=%d, cache hit_rate=%.3f size=%d evictions=%d, repl_ops=%d, conns=%d",
				rate, s.Storage.Keys, s.Cache.HitRate, s.Cache.Size, s.Cache.Evictions, s.ReplicationOps, s.Connections.ActiveConns)
		}
	}
}
