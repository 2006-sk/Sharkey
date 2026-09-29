package metrics

// This file is the per-process HTTP admin surface. Each process (the
// coordinator and every storage node) serves it on its data port + 1000
// (README: coordinator 7100 -> 8100, nodes 7101-7104 -> 8101-8104).
//
// Why a separate HTTP server next to the binary TCP protocol: admin traffic
// (health probes, stats scraping, profiling) is low volume, human- and
// tool-facing, and benefits from curl, browsers and `go tool pprof`, all of
// which speak HTTP. Keeping it on its own port also means a flood of data
// requests cannot hide the health endpoint, and the data protocol stays
// small and specialised.

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// Admin is the HTTP surface of a process: GET /health, GET /stats and
// /debug/pprof/, plus any extra handlers the process registers.
//
// Mux is exported so the owning process can add its own routes (the
// coordinator registers, for example, GET /locate and POST /nodes on it).
// srv is unexported: callers control its lifecycle only through Start and
// Close.
type Admin struct {
	Mux *http.ServeMux
	srv *http.Server
}

// NewAdmin builds an admin server. health returns (document, healthy);
// unhealthy responses use status 503 so load balancers and scripts can rely
// on the HTTP code alone.
//
// health and stats are callbacks rather than values: the admin server knows
// nothing about nodes or coordinators, and each request calls back into the
// owning process for a fresh answer. This keeps package metrics free of any
// dependency on the rest of the system (dependency inversion).
func NewAdmin(health func() (any, bool), stats func() any) *Admin {
	// A private mux, not http.DefaultServeMux: the default mux is global, so
	// any imported package could register handlers on it, and two Admins in
	// one process would collide (registering a pattern twice panics).
	mux := http.NewServeMux()
	// "GET /health" is a Go 1.22+ routing pattern: the method is part of the
	// pattern, so other methods get 405 Method Not Allowed automatically.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		doc, ok := health()
		// 200 when healthy, 503 Service Unavailable when not. The JSON body
		// explains why, but automation only needs the status code.
		code := http.StatusOK
		if !ok {
			code = http.StatusServiceUnavailable
		}
		WriteJSON(w, code, doc)
	})
	// /stats: counters and snapshots (cache hit rate, key counts, latency
	// percentiles, ...) as JSON, computed on demand by the callback.
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, stats())
	})
	// Go runtime profiling (CPU, heap, goroutines, mutex/block contention).
	//
	// Importing net/http/pprof registers these handlers on
	// http.DefaultServeMux only; since this server uses its own mux they
	// must be registered explicitly. pprof.Index serves the listing page and
	// every named profile under /debug/pprof/<name> (heap, goroutine, allocs,
	// mutex, block, threadcreate). The mutex and block profiles stay empty
	// unless the program enables them with runtime.SetMutexProfileFraction /
	// runtime.SetBlockProfileRate. The other four need dedicated handlers:
	// profile (a CPU profile over ?seconds=N, the one the README's
	// scripts/profile.sh collects), trace (an execution trace), symbol
	// (address -> function name lookups for the pprof tool) and cmdline.
	// Note: these endpoints expose internals and can cost CPU while a
	// profile runs, so an admin port must not be exposed publicly.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	// ReadHeaderTimeout bounds how long a client may take to send request
	// headers. Without it, a client that opens connections and trickles
	// headers byte by byte (a "Slowloris" attack, or just a stuck client)
	// could hold connections and goroutines forever. No WriteTimeout is set,
	// which matters: a CPU profile deliberately takes many seconds to respond.
	return &Admin{Mux: mux, srv: &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}}
}

// Start serves on addr in the background.
//
// Listening happens synchronously, before the goroutine starts, so that a
// bind failure (port already in use, bad address) is returned to the caller
// at start-up instead of being lost in a background log line. Once the
// listener exists, the port is bound and connections queue even before
// Serve begins accepting them.
func (a *Admin) Start(addr string, logger *log.Logger) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		// Serve blocks until the server stops. After Close it returns
		// http.ErrServerClosed, which is the normal shutdown path and not
		// worth logging; anything else is a real failure.
		if err := a.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("admin http: %v", err)
		}
	}()
	return nil
}

// Close stops the admin server. http.Server.Close closes the listener and all
// connections immediately (unlike Shutdown, which waits for in-flight
// requests to finish); for an admin endpoint an abrupt stop is fine.
func (a *Admin) Close() error { return a.srv.Close() }

// WriteJSON writes v as indented JSON.
//
// Exported so that extra handlers registered on Admin.Mux respond in the
// same format.
func WriteJSON(w http.ResponseWriter, code int, v any) {
	// Headers must be set before WriteHeader: once the status line is sent,
	// later header changes are silently ignored.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// Stream straight into the response instead of marshalling to a buffer
	// first. Indentation makes curl output readable by humans.
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	// The error is deliberately ignored: the status code has already been
	// sent, so there is no way to report a failure to the client (and the
	// usual cause is the client having disconnected).
	_ = enc.Encode(v)
}
