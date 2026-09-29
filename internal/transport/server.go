// Package transport implements the TCP server loop and pooled client used by
// every component. Both sides speak the framed protocol from package protocol.
//
// Package protocol decides WHAT bytes go on the wire; this package decides
// HOW they get there: sockets, goroutines, deadlines, buffering, connection
// limits, retries and shutdown.
//
// # Files
//
//   - server.go: Server, the accept loop and per-connection request loop used
//     by the coordinator (serving clients) and storage nodes (serving the
//     coordinator).
//   - client.go: three client flavours. Conn is one-request-at-a-time;
//     MuxConn multiplexes many concurrent requests over one socket; Pool
//     spreads callers over a few MuxConns and retries once on a stale one.
//   - writer.go: frameWriter, which merges frames from many goroutines into
//     few write syscalls (write coalescing).
//   - rlimit.go: raises the open-file limit so many sockets can be open.
//
// # Request flow through the cluster
//
//	client (bin/client uses a Pool; benchmark workers use one Conn each)
//	   │ framed requests over TCP
//	   ▼
//	coordinator Server: one goroutine per connection + one per request
//	   │ handler picks replicas, calls each node through its Pool
//	   ▼
//	Pool ─▶ MuxConn: one shared socket per node, many requests in flight
//	   │
//	   ▼
//	storage node Server: handler runs inline when the connection is idle
//
// # Concurrency model (README, "Concurrency model")
//
// One goroutine per connection reads frames ("goroutine-per-connection"). In
// Go this is cheap: goroutines start with small stacks and the runtime parks
// a goroutine blocked on a socket read without tying up an OS thread (the
// netpoller wakes it when data arrives). So plain blocking code scales to
// thousands of connections without writing an event loop by hand.
//
// Each request is then handled in its own goroutine (or inline, see
// ServerConfig.InlineWhenIdle), so a slow request never holds up the next
// one on the same connection. Responses carry the request's ID and may be
// written in any order.
//
// # Where the time goes
//
// The README's profiling found the system syscall-bound: on macOS loopback a
// read or write syscall costs roughly 10-20 µs of CPU, far more than the
// user-space work per request. Much of this package's design (bufio readers
// that pull many frames per read, frameWriter that pushes many frames per
// write, one multiplexed socket per node instead of several) exists to cut
// the number of syscalls per request.
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
//
// (And within one connection, requests usually run in their own goroutines
// too, so a Handler is called concurrently even for a single client.)
//
// The contract: the Handler receives a request that already passed
// protocol.Request.Validate, and returns the response to send. It does not
// set resp.ID; the server copies the request's ID in. Returning nil is a bug,
// but the server tolerates it by sending StatusError. ctx is cancelled only
// when a Shutdown gives up waiting, so long-running handlers should watch it.
type Handler func(ctx context.Context, req *protocol.Request) *protocol.Response

// ServerConfig configures a Server.
//
// Timeouts exist because a network peer can misbehave in ways that would
// otherwise pin resources forever: stop sending mid-frame, stop reading our
// responses, or open connections and never use them. Every blocking socket
// operation in the server therefore has a deadline.
type ServerConfig struct {
	// Limits: key/value/frame size limits, checked on every incoming frame.
	Limits protocol.Limits
	// IdleTimeout bounds how long a connection may sit idle between requests
	// and how long reading a single frame may take.
	//
	// It is implemented as a read deadline re-armed before each frame. It also
	// defends against "slowloris"-style clients that trickle a frame one byte at
	// a time to hold a connection (and its goroutine and buffers) open forever.
	IdleTimeout time.Duration
	// WriteTimeout bounds writing one response.
	//
	// A client that stops reading lets our socket send buffer fill up, and then
	// Write blocks. The deadline turns that into an error that closes the
	// connection instead of stalling a goroutine forever.
	WriteTimeout time.Duration
	// MaxConns caps concurrently open client connections (0 = unlimited).
	//
	// Each connection costs a file descriptor, a goroutine and a 64 KiB read
	// buffer. Without a cap, a flood of connections could exhaust the process's
	// file-descriptor limit (see rlimit.go), after which even healthy peers
	// could not connect. Excess connections get an UNAVAILABLE error frame and
	// are closed immediately.
	MaxConns int
	// InlineWhenIdle runs a request on the connection's reader goroutine when
	// nothing else is in flight or buffered on that connection, saving a
	// goroutine handoff per request. Only safe when handlers never block for
	// long (storage nodes): a blocking handler would delay the next request
	// on the connection until it finished.
	//
	// Why it helps: starting a goroutine and having the scheduler run it costs
	// real time on every request. When a connection is otherwise idle there is
	// nothing to overlap with, so running the handler right here is simply
	// faster. Storage-node handlers only touch in-memory maps, so they finish
	// quickly; coordinator handlers make network calls to nodes and must NOT run
	// inline, or they would reintroduce head-of-line blocking.
	InlineWhenIdle bool
	// MaxInFlightPerConn caps requests being processed concurrently for one
	// connection. When reached, the server stops reading from that
	// connection, pushing back on the client through TCP flow control.
	//
	// This is backpressure: once the server stops calling read, the bytes the
	// client sends pile up in the server's kernel receive buffer; when that is
	// full, TCP advertises a zero window, the client's send buffer fills, and
	// the client's writes start to block. The overload is pushed back to its
	// source instead of the server spawning unbounded goroutines and running out
	// of memory. Nothing is dropped; the client just slows down.
	//
	// Logger (below) receives connection-level errors; if nil, log.Default().
	MaxInFlightPerConn int
	Logger             *log.Logger
}

// DefaultServerConfig returns sensible defaults.
//
// Values: default protocol limits (1 KiB keys, 1 MiB values, 4 MiB frames),
// 2-minute idle timeout, 5-second write timeout, at most 4096 connections,
// 256 requests in flight per connection (README: "A per-connection
// semaphore (256 in flight) provides backpressure"). InlineWhenIdle is off;
// storage nodes turn it on.
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
//
// Lifecycle: NewServer -> Serve (or ListenAndServe) blocks accepting
// connections -> Shutdown stops it gracefully. One Server serves one
// listener.
//
// Fields:
//   - cfg, handler: configuration and the request handler (read-only after
//     NewServer, so they need no lock).
type Server struct {
	cfg     ServerConfig
	handler Handler

	// mu guards ln, conns and closing (and is held while registering a
	// connection, so registration cannot race with Shutdown):
	//   - ln: the listener, set by Serve, closed by Shutdown to stop Accept.
	//   - conns: every live connection, so Shutdown can wake or close them all
	//     and Stats can count them. A map used as a set (struct{} values take
	//     no memory).
	//   - closing: Shutdown has started; no new connections are accepted.
	//   - stopping: the same fact as an atomic.Bool, so the hot per-request loop
	//     in serveConn can check it without contending on mu.
	//   - wg: counts live connection goroutines; Shutdown waits on it.
	mu       sync.Mutex
	ln       net.Listener
	conns    map[net.Conn]struct{}
	closing  bool
	stopping atomic.Bool // read by connection loops without taking mu
	wg       sync.WaitGroup
	// ctx is passed to handlers. It is cancelled only when Shutdown gives up
	// waiting, so graceful shutdown lets in-flight requests finish normally.
	//
	// cancel cancels ctx. accepted, rejected and badReqs are lock-free atomic
	// counters for Stats: connections served, connections refused at MaxConns,
	// and requests answered with BAD_REQUEST.
	ctx      context.Context
	cancel   context.CancelFunc
	accepted atomic.Int64
	rejected atomic.Int64
	badReqs  atomic.Int64
}

// NewServer creates a server; call Serve to start it.
//
// It fills in defaults for two zero values: a nil Logger and a
// non-positive MaxInFlightPerConn. Other zero values are meaningful (a zero
// IdleTimeout, WriteTimeout or MaxConns means "no limit"), so start from
// DefaultServerConfig rather than an empty ServerConfig.
func NewServer(cfg ServerConfig, h Handler) *Server {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	// Must be at least 1: a zero-capacity semaphore channel would block every
	// request forever.
	if cfg.MaxInFlightPerConn <= 0 {
		cfg.MaxInFlightPerConn = 256
	}
	// The server-wide handler context: lives until Shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{cfg: cfg, handler: h, conns: make(map[net.Conn]struct{}), ctx: ctx, cancel: cancel}
}

// ServerStats are connection-level counters.
// Reported in the node's and coordinator's /stats output (the json tags
// name the keys).
type ServerStats struct {
	// ActiveConns: open now; AcceptedConns/RejectedConns: totals since start;
	// BadRequests: frames answered with BAD_REQUEST (malformed or oversized).
	ActiveConns   int   `json:"active_conns"`
	AcceptedConns int64 `json:"accepted_conns"`
	RejectedConns int64 `json:"rejected_conns"`
	BadRequests   int64 `json:"bad_requests"`
}

// Stats returns connection counters.
func (s *Server) Stats() ServerStats {
	// len(conns) needs mu; the counters are atomics and need no lock.
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
//
// Listening on port 0 asks the OS for any free port; Addr reveals which one
// it picked. Returns nil before Serve has been called.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// ListenAndServe listens on addr and serves until Shutdown.
// addr is "host:port", e.g. ":7100" for all interfaces.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until Shutdown is called. It returns nil
// after a clean shutdown.
//
// The accept loop does as little as possible: accept, register, start a
// goroutine. Anything slow here (such as reading a request) would stop
// every other client from connecting.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	// Shutdown already ran: don't start serving at all.
	if s.closing {
		s.mu.Unlock()
		ln.Close()
		return nil
	}
	// Publish the listener under mu so Shutdown can close it.
	s.ln = ln
	s.mu.Unlock()

	// Backoff for transient accept errors, reset after every success.
	var backoff time.Duration
	for {
		// Blocks until a client connects, or until the listener is closed.
		nc, err := ln.Accept()
		if err != nil {
			// Shutdown closes the listener to unblock Accept; the
			// resulting error is expected, not a failure.
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return nil
			}
			// Transient errors: a timeout, or resource exhaustion such
			// as EMFILE (out of file descriptors; see isTemporaryAccept
			// in client.go). These may clear up once some connections
			// close, so retry instead of killing the server.
			// Note && binds tighter than ||.
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() || isTemporaryAccept(err) {
				// e.g. EMFILE: back off instead of spinning.
				// Retrying immediately would spin at 100% CPU (Accept
				// fails instantly while fds are exhausted) and flood the
				// log. Exponential backoff: 5ms, 10ms, 20ms, ... up to 1s.
				backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
				s.cfg.Logger.Printf("accept error (retrying in %v): %v", backoff, err)
				time.Sleep(backoff)
				continue
			}
			// Anything else is unexpected and fatal for this listener.
			return fmt.Errorf("accept: %w", err)
		}
		backoff = 0
		// track refuses the connection (and has already closed it) if we are at
		// MaxConns or shutting down.
		if !s.track(nc) {
			continue
		}
		s.accepted.Add(1)
		// Goroutine-per-connection: the loop immediately goes back to Accept.
		go s.serveConn(nc)
	}
}

// track registers a connection, enforcing MaxConns. It reports whether the
// connection should be served.
//
// It runs on the accept goroutine, so everything in it must be quick or
// bounded (hence the 100ms write deadline on the rejection message below).
func (s *Server) track(nc net.Conn) bool {
	s.mu.Lock()
	// Racing with Shutdown: the closing check and the conns/wg registration are
	// under the same mu that Shutdown holds when it sets closing. So either
	// Shutdown sees this connection in conns, or we see closing and refuse it;
	// a connection can never slip in unnoticed. It also guarantees wg.Add is
	// never called after Shutdown has started waiting on wg with a zero count,
	// which sync.WaitGroup forbids.
	if s.closing {
		s.mu.Unlock()
		nc.Close()
		return false
	}
	if s.cfg.MaxConns > 0 && len(s.conns) >= s.cfg.MaxConns {
		s.mu.Unlock()
		s.rejected.Add(1)
		// Best-effort explanation before closing.
		// Tell the client why, instead of a bare disconnect. The frame has ID 0,
		// which clients treat as a connection-level error (see MuxConn.readLoop).
		// The short deadline keeps a client that is not reading from stalling the
		// accept loop; any error is ignored because we are closing anyway.
		_ = nc.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, _ = nc.Write(protocol.AppendResponseFrame(nil,
			protocol.ErrorResponse(protocol.StatusUnavailable, "server at connection limit (%d)", s.cfg.MaxConns)))
		nc.Close()
		return false
	}
	// Registered. wg.Done is called by untrack when the connection ends.
	s.conns[nc] = struct{}{}
	s.wg.Add(1)
	s.mu.Unlock()
	return true
}

// untrack is the inverse of track, deferred by serveConn. It closes the
// socket (releasing its file descriptor) and marks the connection's
// goroutine as finished for Shutdown's wg.Wait.
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
//
// Structure of the loop:
//
//	read deadline -> ReadFrame -> DecodeRequest + Validate
//	                    |               |
//	            oversized: reply   malformed: reply BAD_REQUEST,
//	            and close          keep going
//	                                    |
//	                 idle? run inline : acquire sem slot, go handle()
//
// Only this goroutine reads from the socket, so frames are always consumed
// whole and in order. Writing is shared (many handler goroutines respond)
// and goes through the frameWriter.
func (s *Server) serveConn(nc net.Conn) {
	// Deferred calls run last-in-first-out: inflight.Wait (registered below)
	// runs FIRST, then untrack closes the socket. Order matters; see below.
	defer s.untrack(nc)
	// Disable Nagle's algorithm. Nagle holds back small segments while earlier
	// data is unacknowledged, hoping to merge them; for request/response
	// traffic that adds latency. Go already enables TCP_NODELAY by default; this
	// makes the choice explicit. Batching is done deliberately in user space by
	// frameWriter instead, where we control the delay.
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	// Counting semaphore implemented as a buffered channel: sending takes a
	// slot (blocks when all MaxInFlightPerConn are taken), receiving frees it.
	// len(sem) is the number of requests currently being handled in goroutines.
	sem := make(chan struct{}, s.cfg.MaxInFlightPerConn)
	// One writer per connection, shared by all its handler goroutines. busy
	// reports len(sem), so the writer lingers only when many requests are in
	// flight here (and more responses are imminent).
	w := &frameWriter{nc: nc, timeout: s.cfg.WriteTimeout, linger: lingerDur, busy: func() int32 { return int32(len(sem)) }}
	// Tracks this connection's handler goroutines.
	var inflight sync.WaitGroup
	// Runs before untrack closes the connection: in-flight handlers get to
	// write their responses.
	defer inflight.Wait()

	// A 64 KiB buffered reader: one read syscall pulls in as many bytes as are
	// available (often many frames), and the subsequent small ReadFull calls in
	// ReadFrame are served from memory. Without it, every frame would cost at
	// least two read syscalls (header, then body).
	br := bufio.NewReaderSize(nc, 64<<10)
	// Stop reading new requests once Shutdown has begun. The atomic load is
	// cheap, which matters since this runs once per request.
	for !s.stopping.Load() {
		// Re-arm the idle deadline before each frame. A deadline is an absolute
		// point in time (not a duration), so it must be refreshed every
		// iteration. (A connection that re-arms it just after Shutdown's wake-up
		// would sit out its IdleTimeout; Shutdown's ctx bounds that case.)
		if s.cfg.IdleTimeout > 0 {
			_ = nc.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		}
		// Blocks until a whole frame arrives, the deadline passes, or the peer
		// closes.
		payload, err := protocol.ReadFrame(br, s.cfg.Limits.MaxFrameSize)
		if err != nil {
			if errors.Is(err, protocol.ErrFrameTooLarge) {
				// The stream cannot be resynchronised after an oversized
				// frame (we refuse to read its body), so reply and close.
				s.badReqs.Add(1)
				s.respond(w, protocol.ErrorResponse(protocol.StatusBadRequest, "%v", err))
				// Clean closes, idle timeouts and shutdown wake-ups are
				// normal, not worth logging; anything else is.
			} else if !isClosedConnErr(err) {
				s.cfg.Logger.Printf("conn %s: read: %v", nc.RemoteAddr(), err)
			}
			// Any read error ends the connection (deferred cleanup runs).
			return
		}

		// Two-step check: syntax first (DecodeRequest), then semantics (Validate).
		req, err := protocol.DecodeRequest(payload)
		if err == nil {
			err = req.Validate(s.cfg.Limits)
		}
		if err != nil {
			// The frame boundary is intact, so the connection stays usable.
			// (ReadFrame consumed exactly this frame's bytes, so the next
			// read starts at the next frame's length prefix.) Echo the ID
			// if we can recover it, so a multiplexing client fails only
			// that one request.
			s.badReqs.Add(1)
			resp := protocol.ErrorResponse(protocol.StatusBadRequest, "%v", err)
			resp.ID = protocol.RequestID(payload)
			// respond returns false if the connection can no longer be written to.
			if !s.respond(w, resp) {
				return
			}
			continue
		}

		// Inline fast path (storage nodes only). Both conditions must hold:
		//   - len(sem) == 0: no handler goroutine is running, so running this one
		//     inline cannot reorder anything or starve other work;
		//   - br.Buffered() == 0: no further request is already sitting in our read
		//     buffer. If one were, running inline would make it wait for this
		//     handler: head-of-line blocking. Dispatch to a goroutine instead.
		// Bytes still in the kernel (not yet read into br) are invisible here,
		// which is acceptable because node handlers are fast.
		if s.cfg.InlineWhenIdle && len(sem) == 0 && br.Buffered() == 0 {
			s.handle(w, req)
			continue
		}
		// Acquire a slot. If MaxInFlightPerConn handlers are already running this
		// BLOCKS, and while it blocks we are not reading the socket: that is the
		// backpressure described in ServerConfig.
		sem <- struct{}{}
		inflight.Add(1)
		// Each iteration declares its own `req`, so the goroutine captures this
		// request, not a later one.
		go func() {
			// Release the slot and mark done even if the handler panics.
			defer func() { <-sem; inflight.Done() }()
			s.handle(w, req)
		}()
	}
}

// handle runs the Handler for one request and sends its response.
func (s *Server) handle(w *frameWriter, req *protocol.Request) {
	// s.ctx, not a per-request context: handlers see cancellation only when
	// Shutdown gives up waiting.
	resp := s.handler(s.ctx, req)
	// Defensive: never leave a client waiting forever because of a nil
	// response; it would otherwise hang until its own timeout.
	if resp == nil {
		resp = protocol.ErrorResponse(protocol.StatusError, "handler returned no response")
	}
	// Stamp the response with the request's ID so the client can route it:
	// this is the server's half of multiplexing.
	resp.ID = req.ID
	s.respond(w, resp)
}

// respond encodes resp straight into the connection's shared frameWriter
// buffer and makes sure it gets flushed. It reports whether the write
// succeeded (as far as this goroutine can tell; see frameWriter.write).
func (s *Server) respond(w *frameWriter, resp *protocol.Response) bool {
	err := w.write(func(b []byte) []byte { return protocol.AppendResponseFrame(b, resp) })
	// Errors because the connection is already closed are expected (e.g. the
	// client went away) and not logged.
	if err != nil && !isClosedConnErr(err) {
		s.cfg.Logger.Printf("conn %s: write: %v", w.nc.RemoteAddr(), err)
	}
	return err == nil
}

// Shutdown stops accepting connections, lets in-flight requests finish and
// closes idle connections. If ctx expires first, remaining connections are
// closed forcibly.
//
// Graceful shutdown in phases:
//
//  1. closing/stopping = true; close the listener (Accept loop exits).
//  2. Set every connection's read deadline to "now". Each reader blocked
//     in ReadFrame (idle or not: handlers run in other goroutines) wakes
//     with a timeout and leaves its loop, then waits for its in-flight
//     handlers (inflight.Wait) to write their responses before closing. A
//     reader busy running a handler inline finishes it, sees stopping and
//     leaves.
//  3. Wait for all connection goroutines (wg), bounded by ctx.
//  4. If ctx expires first: cancel the handler context, force-close all
//     sockets, and still wait for the goroutines to exit.
//
// Why bother: a plain close would cut off requests mid-flight, and the
// client could not tell whether a write had been applied. Letting them
// finish means every request that was accepted also gets its answer.
//
// Caveat: in phase 4 the final wait (<-done) still waits for handlers to
// return. A handler that ignores ctx and blocks forever would block even a
// forced Shutdown; handlers are expected to honour ctx.
func (s *Server) Shutdown(ctx context.Context) error {
	// Under mu so that track (which checks closing under the same lock) cannot
	// register a connection after we snapshot conns below.
	s.mu.Lock()
	s.closing = true
	s.stopping.Store(true)
	// Unblocks the Accept in Serve, which then returns nil.
	if s.ln != nil {
		s.ln.Close()
	}
	// Wake connections blocked waiting for their next request. Requests
	// already being handled finish and write their responses.
	//
	// A read deadline in the past makes any blocked or future Read return a
	// timeout error immediately. That is how one goroutine can interrupt
	// another goroutine's blocking read without closing the socket (closing
	// would also prevent the pending responses from being written).
	for nc := range s.conns {
		_ = nc.SetReadDeadline(time.Now())
	}
	s.mu.Unlock()

	// wg.Wait has no timeout, so run it in a goroutine and turn completion
	// into a channel close that select can wait on alongside ctx.
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	// Everything finished in time. Cancel ctx anyway to release its resources.
	case <-done:
		s.cancel()
		return nil
	// Out of patience: escalate.
	case <-ctx.Done():
		s.cancel() // tell handlers to give up
		s.mu.Lock()
		// Closing makes every blocked Read/Write on these sockets fail at once.
		for nc := range s.conns {
			nc.Close()
		}
		s.mu.Unlock()
		// Wait for the goroutines to actually exit (they will soon, their sockets
		// are closed), then report that the graceful deadline was missed.
		<-done
		return ctx.Err()
	}
}

// isClosedConnErr reports "normal" ways for a connection to end, which the
// server does not log: the peer closed (EOF, or ErrUnexpectedEOF mid-frame),
// we closed it (net.ErrClosed), or a read deadline fired (idle timeout, or
// Shutdown's wake-up). errors.Is/As look through wrapped errors.
func isClosedConnErr(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout() // idle timeout or shutdown wake-up
}
