package metrics

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
type Admin struct {
	Mux *http.ServeMux
	srv *http.Server
}

// NewAdmin builds an admin server. health returns (document, healthy);
// unhealthy responses use status 503 so load balancers and scripts can rely
// on the HTTP code alone.
func NewAdmin(health func() (any, bool), stats func() any) *Admin {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		doc, ok := health()
		code := http.StatusOK
		if !ok {
			code = http.StatusServiceUnavailable
		}
		WriteJSON(w, code, doc)
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, stats())
	})
	// Go runtime profiling (CPU, heap, goroutines, mutex/block contention).
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	return &Admin{Mux: mux, srv: &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}}
}

// Start serves on addr in the background.
func (a *Admin) Start(addr string, logger *log.Logger) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		if err := a.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("admin http: %v", err)
		}
	}()
	return nil
}

// Close stops the admin server.
func (a *Admin) Close() error { return a.srv.Close() }

// WriteJSON writes v as indented JSON.
func WriteJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
