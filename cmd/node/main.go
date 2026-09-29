// Command node runs a storage node.
//
//	node --port 7101 [--http-port 8101] [--cache-capacity 10000]
//
// A storage node is the process that actually holds data. It keeps keys in a
// sharded in-memory map, optionally fronted by an LRU cache, and serves the
// binary TCP protocol (GET/PUT/DELETE plus the internal replication, scan and
// stats operations the coordinator uses). A node knows nothing about the hash
// ring or about other nodes: routing, replication and failure handling all
// live in the coordinator. That keeps this binary small; main only has to
// turn flags into a node.Config, start two listeners, and shut down cleanly.
//
// Lifecycle of the process:
//
//  1. Parse flags (flag.ExitOnError: a bad flag prints usage and exits 2).
//  2. Raise the open-file limit so many connections can be accepted.
//  3. Build the node and, unless disabled, an HTTP admin server
//     (/health, /stats, /debug/pprof) on port+1000 by default.
//  4. Serve the KV protocol in a goroutine while main waits for either a
//     fatal serve error or SIGINT/SIGTERM.
//  5. On a signal, shut down gracefully with a 5 s budget: stop accepting,
//     let in-flight requests finish and write their responses, then exit.
//
// Each node generates a fresh random boot ID at start-up (logged below). The
// coordinator compares boot IDs across health checks, so a node that crashed
// and restarted between two checks is still recognised as a NEW incarnation
// with empty memory and gets resynchronised.
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
	// A named FlagSet rather than the global flag package: the name "node"
	// appears in -h output, and ExitOnError means a malformed flag prints
	// usage and exits with status 2 instead of returning an error to handle.
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	// Bind address. Empty means all interfaces (0.0.0.0 / ::), which Docker
	// needs so that other containers can reach the node.
	host := fs.String("host", "", "interface to bind (empty = all interfaces)")
	// KV protocol port. The default cluster uses 7101..7104 for four nodes.
	port := fs.Int("port", 7101, "TCP port for the KV protocol")
	// -1 is a sentinel meaning "not set by the user"; it is resolved to
	// port+1000 after parsing, because a flag default cannot depend on
	// another flag's parsed value. 0 is a real, distinct choice: disable.
	httpPort := fs.Int("http-port", -1, "HTTP port for /health and /stats (default port+1000, 0 disables)")
	// Number of entries in the LRU read cache; Test B toggles this between
	// 0 and 10000 to measure the cache's effect.
	cacheCap := fs.Int("cache-capacity", 10000, "LRU cache capacity in entries (0 disables the cache)")
	// Connections with no traffic for this long are closed, so abandoned
	// clients do not hold file descriptors forever.
	idle := fs.Duration("idle-timeout", 2*time.Minute, "close client connections idle this long")
	// Bound on writing one response. A peer that stops reading (full TCP
	// window) would otherwise block the writer indefinitely.
	writeTimeout := fs.Duration("write-timeout", 5*time.Second, "deadline for writing one response")
	// Hard cap on simultaneous connections; excess connections are refused
	// rather than exhausting file descriptors and memory.
	maxConns := fs.Int("max-conns", 4096, "maximum concurrent client connections")
	// Period of the one-line stats summary (see logStats).
	statsEvery := fs.Duration("log-stats-interval", 30*time.Second, "log a stats summary this often (0 disables)")
	// Shared --max-key-size / --max-value-size / --max-frame-size flags. The
	// returned pointer is filled in by fs.Parse below.
	limits := cmdutil.LimitFlags(fs)
	// os.Args[0] is the program name, so parsing starts at index 1. The
	// return value is ignored because ExitOnError never returns an error.
	fs.Parse(os.Args[1:])

	// Prefix every log line with the port so that, when several nodes share
	// one terminal or log directory, each line says which node wrote it.
	// Lmicroseconds gives sub-millisecond timestamps, which matter when
	// lining up logs against a failure test's timeline.
	logger := log.New(os.Stderr, fmt.Sprintf("node:%d ", *port), log.LstdFlags|log.Lmicroseconds)
	// Do this before any listener exists, so the higher limit applies to
	// every connection this process will ever accept.
	cmdutil.RaiseFileLimit(logger)

	// Start from the library defaults and override only what flags control;
	// any field without a flag keeps its tested default.
	cfg := node.DefaultConfig()
	cfg.CacheCapacity = *cacheCap
	cfg.Server.Limits = *limits
	cfg.Server.IdleTimeout = *idle
	cfg.Server.WriteTimeout = *writeTimeout
	cfg.Server.MaxConns = *maxConns
	cfg.Logger = logger
	// Construct the node (storage, cache, boot ID). Nothing listens yet.
	n := node.New(cfg)

	// Resolve the "unset" sentinel to the port+1000 convention (7101 ->
	// 8101), which gives each node a predictable admin port.
	if *httpPort < 0 {
		*httpPort = *port + 1000
	}
	if *httpPort > 0 {
		// The admin server takes two callbacks. The health callback returns
		// (document, ok); ok=false would make /health answer 503. A node that
		// is running is by definition healthy from its own point of view, so
		// it always reports true. Liveness judgements are the coordinator's
		// job, made from its own health checks.
		admin := metrics.NewAdmin(func() (any, bool) { return n.Health(), true }, func() any { return n.Stats() })
		addr := fmt.Sprintf("%s:%d", *host, *httpPort)
		// Start binds synchronously, so a port clash fails here, loudly, at
		// start-up rather than silently in a background goroutine.
		if err := admin.Start(addr, logger); err != nil {
			logger.Fatalf("admin http: %v", err)
		}
		// Deferred calls run when main returns normally (after shutdown).
		// Note that log.Fatalf calls os.Exit, which skips deferred calls.
		defer admin.Close()
		logger.Printf("admin HTTP on %s (/health, /stats)", addr)
	}

	// ctx is cancelled on the first SIGINT/SIGTERM. stop un-registers the
	// signal handler so that a second Ctrl-C kills the process outright.
	ctx, stop := cmdutil.SignalContext()
	defer stop()
	// The stats logger exits on its own when ctx is cancelled.
	go logStats(ctx, logger, n, *statsEvery)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	// ListenAndServe blocks for the lifetime of the server, so it runs in a
	// goroutine and reports its result on a channel. The buffer of 1 lets
	// that goroutine send and exit even if main is no longer receiving
	// (e.g. main already chose the signal branch), so it never leaks.
	errc := make(chan error, 1)
	go func() { errc <- n.ListenAndServe(addr) }()
	logger.Printf("storage node listening on %s (boot_id=%s, cache=%d entries)", addr, n.BootID(), *cacheCap)

	// Wait for whichever happens first:
	//   - the server failed (typically "address already in use"): fatal;
	//   - an interrupt/terminate signal arrived: fall through to shutdown.
	select {
	case err := <-errc:
		logger.Fatalf("serve: %v", err)
	case <-ctx.Done():
	}
	logger.Printf("shutting down")
	// A fresh context for shutdown, NOT ctx: ctx is already cancelled, and a
	// cancelled context would make Shutdown give up immediately. 5 s is the
	// budget for in-flight requests to finish and their responses to be
	// written; after it, remaining connections are abandoned.
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Hitting the deadline is an expected outcome with stubborn clients and
	// is not worth reporting; any other error is logged.
	if err := n.Shutdown(sctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		logger.Printf("shutdown: %v", err)
	}
}

// logStats prints one summary line periodically instead of a line per
// request, so benchmarks don't drown in logs.
//
// Logging every request would cost a formatted write (a syscall) per
// operation. At ~50k ops/s that alone would distort the very latency being
// measured and fill the disk. A periodic summary gives an operator the same
// picture (request rate, key count, cache behaviour, connections) at a cost
// of one line every `every`.
func logStats(ctx context.Context, logger *log.Logger, n *node.Node, every time.Duration) {
	// --log-stats-interval 0 disables the summary.
	if every <= 0 {
		return
	}
	// A Ticker fires every `every`; Stop releases its resources when the
	// function returns.
	t := time.NewTicker(every)
	defer t.Stop()
	// s.Requests is a cumulative counter since start-up. The rate over the
	// last interval is the difference from the previous reading divided by
	// the interval length, so the previous value is remembered in `last`.
	var last int64
	for {
		select {
		case <-ctx.Done():
			// Shutdown began: stop logging.
			return
		case <-t.C:
			s := n.Stats()
			// Requests per second over the last interval.
			rate := float64(s.Requests-last) / every.Seconds()
			last = s.Requests
			logger.Printf("stats: %.0f req/s, keys=%d, cache hit_rate=%.3f size=%d evictions=%d, repl_ops=%d, conns=%d",
				rate, s.Storage.Keys, s.Cache.HitRate, s.Cache.Size, s.Cache.Evictions, s.ReplicationOps, s.Connections.ActiveConns)
		}
	}
}
