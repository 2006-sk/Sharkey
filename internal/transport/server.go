// Package transport implements the TCP server loop and pooled client used by
// every component. Both sides speak the framed protocol from package protocol.
package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"distkv/internal/protocol"
)

// Handler processes one decoded, validated request. It must be safe for
// concurrent use: every connection is served by its own goroutine.
type Handler func(ctx context.Context, req *protocol.Request) *protocol.Response

// ServerConfig configures a Server.
type ServerConfig struct {
	Limits protocol.Limits
	// IdleTimeout bounds how long a connection may sit idle between requests
	// and how long reading a single frame may take.
	IdleTimeout time.Duration
	// WriteTimeout bounds writing one response.
	WriteTimeout time.Duration
	// MaxConns caps concurrently open client connections (0 = unlimited).
	MaxConns int
	// InlineWhenIdle runs a request on the connection's reader goroutine when
	// nothing else is in flight or buffered on that connection, saving a
	// goroutine handoff per request. Only safe when handlers never block for
	// long (storage nodes): a blocking handler would delay the next request
	// on the connection until it finished.
	InlineWhenIdle bool
	// MaxInFlightPerConn caps requests being processed concurrently for one
	// connection. When reached, the server stops reading from that
	// connection, pushing back on the client through TCP flow control.
	MaxInFlightPerConn int
	Logger             *log.Logger
}

// DefaultServerConfig returns sensible defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Limits:             protocol.DefaultLimits(),
		IdleTimeout:        2 * time.Minute,
		WriteTimeout:       5 * time.Second,
		MaxConns:           4096,
		MaxInFlightPerConn: 256,
		Logger:             log.Default(),
	}
}

// Server accepts TCP connections and dispatches framed requests to a Handler.
type Server struct {
	cfg     ServerConfig
	handler Handler

	mu       sync.Mutex
	ln       net.Listener
	conns    map[net.Conn]struct{}
	closing  bool
	stopping atomic.Bool // read by connection loops without taking mu
	wg       sync.WaitGroup
	// ctx is passed to handlers. It is cancelled only when Shutdown gives up
	// waiting, so graceful shutdown lets in-flight requests finish normally.
	ctx      context.Context
	cancel   context.CancelFunc
	accepted atomic.Int64
	rejected atomic.Int64
	badReqs  atomic.Int64
}

// NewServer creates a server; call Serve to start it.
func NewServer(cfg ServerConfig, h Handler) *Server {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.MaxInFlightPerConn <= 0 {
		cfg.MaxInFlightPerConn = 256
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{cfg: cfg, handler: h, conns: make(map[net.Conn]struct{}), ctx: ctx, cancel: cancel}
}

// ServerStats are connection-level counters.
type ServerStats struct {
	ActiveConns   int   `json:"active_conns"`
	AcceptedConns int64 `json:"accepted_conns"`
	RejectedConns int64 `json:"rejected_conns"`
	BadRequests   int64 `json:"bad_requests"`
}

// Stats returns connection counters.
func (s *Server) Stats() ServerStats {
	s.mu.Lock()
	active := len(s.conns)
	s.mu.Unlock()
	return ServerStats{
		ActiveConns:   active,
		AcceptedConns: s.accepted.Load(),
		RejectedConns: s.rejected.Load(),
		BadRequests:   s.badReqs.Load(),
	}
}

// Addr returns the listening address (useful with port 0 in tests).
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// ListenAndServe listens on addr and serves until Shutdown.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until Shutdown is called. It returns nil
// after a clean shutdown.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		ln.Close()
		return nil
	}
	s.ln = ln
	s.mu.Unlock()

	var backoff time.Duration
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() || isTemporaryAccept(err) {
				// e.g. EMFILE: back off instead of spinning.
				backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
				s.cfg.Logger.Printf("accept error (retrying in %v): %v", backoff, err)
				time.Sleep(backoff)
				continue
			}
			return fmt.Errorf("accept: %w", err)
		}
		backoff = 0
		if !s.track(nc) {
			continue
		}
		s.accepted.Add(1)
		go s.serveConn(nc)
	}
}

// track registers a connection, enforcing MaxConns. It reports whether the
// connection should be served.
func (s *Server) track(nc net.Conn) bool {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		nc.Close()
		return false
	}
	if s.cfg.MaxConns > 0 && len(s.conns) >= s.cfg.MaxConns {
		s.mu.Unlock()
		s.rejected.Add(1)
		// Best-effort explanation before closing.
		_ = nc.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, _ = nc.Write(protocol.AppendResponseFrame(nil,
			protocol.ErrorResponse(protocol.StatusUnavailable, "server at connection limit (%d)", s.cfg.MaxConns)))
		nc.Close()
		return false
	}
	s.conns[nc] = struct{}{}
	s.wg.Add(1)
	s.mu.Unlock()
	return true
}

func (s *Server) untrack(nc net.Conn) {
	s.mu.Lock()
	delete(s.conns, nc)
	s.mu.Unlock()
	nc.Close()
	s.wg.Done()
}

// serveConn reads requests from one connection and handles each in its own
// goroutine, so a slow request never blocks later ones on the same
// connection (no head-of-line blocking) and a multiplexing client can keep
// many requests in flight. Responses carry the request ID and may be written
// in any order; frameWriter coalesces them into few syscalls.
func (s *Server) serveConn(nc net.Conn) {
	defer s.untrack(nc)
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	sem := make(chan struct{}, s.cfg.MaxInFlightPerConn)
	w := &frameWriter{nc: nc, timeout: s.cfg.WriteTimeout, linger: lingerDur, busy: func() int32 { return int32(len(sem)) }}
	var inflight sync.WaitGroup
	// Runs before untrack closes the connection: in-flight handlers get to
	// write their responses.
	defer inflight.Wait()

	br := bufio.NewReaderSize(nc, 64<<10)
	for !s.stopping.Load() {
		if s.cfg.IdleTimeout > 0 {
			_ = nc.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		}
		payload, err := protocol.ReadFrame(br, s.cfg.Limits.MaxFrameSize)
		if err != nil {
			if errors.Is(err, protocol.ErrFrameTooLarge) {
				// The stream cannot be resynchronised after an oversized
				// frame (we refuse to read its body), so reply and close.
				s.badReqs.Add(1)
				s.respond(w, protocol.ErrorResponse(protocol.StatusBadRequest, "%v", err))
			} else if !isClosedConnErr(err) {
				s.cfg.Logger.Printf("conn %s: read: %v", nc.RemoteAddr(), err)
			}
			return
		}

		req, err := protocol.DecodeRequest(payload)
		if err == nil {
			err = req.Validate(s.cfg.Limits)
		}
		if err != nil {
			// The frame boundary is intact, so the connection stays usable.
			s.badReqs.Add(1)
			resp := protocol.ErrorResponse(protocol.StatusBadRequest, "%v", err)
			resp.ID = protocol.RequestID(payload)
			if !s.respond(w, resp) {
				return
			}
			continue
		}

		if s.cfg.InlineWhenIdle && len(sem) == 0 && br.Buffered() == 0 {
			s.handle(w, req)
			continue
		}
		sem <- struct{}{}
		inflight.Add(1)
		go func() {
			defer func() { <-sem; inflight.Done() }()
			s.handle(w, req)
		}()
	}
}

func (s *Server) handle(w *frameWriter, req *protocol.Request) {
	resp := s.handler(s.ctx, req)
	if resp == nil {
		resp = protocol.ErrorResponse(protocol.StatusError, "handler returned no response")
	}
	resp.ID = req.ID
	s.respond(w, resp)
}

func (s *Server) respond(w *frameWriter, resp *protocol.Response) bool {
	err := w.write(func(b []byte) []byte { return protocol.AppendResponseFrame(b, resp) })
	if err != nil && !isClosedConnErr(err) {
		s.cfg.Logger.Printf("conn %s: write: %v", w.nc.RemoteAddr(), err)
	}
	return err == nil
}

// Shutdown stops accepting connections, lets in-flight requests finish and
// closes idle connections. If ctx expires first, remaining connections are
// closed forcibly.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.stopping.Store(true)
	if s.ln != nil {
		s.ln.Close()
	}
	// Wake connections blocked waiting for their next request. Requests
	// already being handled finish and write their responses.
	for nc := range s.conns {
		_ = nc.SetReadDeadline(time.Now())
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		s.cancel()
		return nil
	case <-ctx.Done():
		s.cancel() // tell handlers to give up
		s.mu.Lock()
		for nc := range s.conns {
			nc.Close()
		}
		s.mu.Unlock()
		<-done
		return ctx.Err()
	}
}

func isClosedConnErr(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout() // idle timeout or shutdown wake-up
}
