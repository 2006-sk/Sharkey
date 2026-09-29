package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"distkv/internal/protocol"
)

// Tests for the transport. Unlike the protocol tests these use real TCP
// sockets on the loopback interface (127.0.0.1), because the behaviours
// under test (deadlines, connection limits, shutdown, reconnection,
// out-of-order multiplexing) only exist with real connections.

// echoHandler answers every request with "key=value" and echoes the
// version, so a test can check that each caller got the response to ITS
// request and not someone else's.
func echoHandler(_ context.Context, req *protocol.Request) *protocol.Response {
	return &protocol.Response{Status: protocol.StatusOK, Version: req.Version, Value: append([]byte(req.Key+"="), req.Value...)}
}

// startServer runs a Server on an OS-assigned free port ("127.0.0.1:0") so
// tests never collide on a fixed port, silences its logging, and registers
// a Shutdown for when the test ends. It returns the address to dial.
func startServer(t *testing.T, cfg ServerConfig, h Handler) (*Server, string) {
	t.Helper()
	cfg.Logger = log.New(io.Discard, "", 0)
	s := NewServer(cfg, h)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// Serve blocks, so it runs in its own goroutine.
	go s.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
	})
	return s, ln.Addr().String()
}

// ctxTimeout returns a context that expires after d and is cleaned up
// automatically when the test finishes.
func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// TestRoundTripAndConcurrency: 50 goroutines x 100 requests through one
// Pool (8 connections). It proves the pool, multiplexing and write
// coalescing deliver correct responses under concurrency; run with -race it
// also checks the shared state (pending maps, frameWriter buffers, slot
// locks) is properly synchronised. The value contains '\n' to confirm
// binary-safe framing end to end.
func TestRoundTripAndConcurrency(t *testing.T) {
	_, addr := startServer(t, DefaultServerConfig(), echoHandler)
	pool := NewPool(addr, time.Second, 8, 0)
	defer pool.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				resp, err := pool.Do(ctxTimeout(t, 2*time.Second), &protocol.Request{Op: protocol.OpPut, Key: "k", Value: []byte("v\n")})
				if err != nil {
					t.Error(err)
					return
				}
				if string(resp.Value) != "k=v\n" {
					t.Errorf("bad echo %q", resp.Value)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// rawConn sends hand-crafted bytes to exercise malformed-input handling.
// It bypasses the client library entirely, the way a buggy or malicious
// peer would, and sets a 2s deadline so a failing test cannot hang.
func rawConn(t *testing.T, addr string) net.Conn {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	nc.SetDeadline(time.Now().Add(2 * time.Second))
	return nc
}

// readResp reads and decodes one response frame from a raw connection.
func readResp(t *testing.T, nc net.Conn) *protocol.Response {
	t.Helper()
	p, err := protocol.ReadFrame(nc, 1<<20)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	r, err := protocol.DecodeResponse(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestMalformedPayloadKeepsConnectionUsable proves the "recoverable error"
// half of the error model: a frame whose payload is garbage (or names an
// unknown op) gets BAD_REQUEST, but because the frame itself was complete
// the stream is still in sync, so the SAME connection then serves a valid
// request. Killing the connection on every bad request would needlessly
// fail every other request multiplexed on it.
func TestMalformedPayloadKeepsConnectionUsable(t *testing.T) {
	s, addr := startServer(t, DefaultServerConfig(), echoHandler)
	nc := rawConn(t, addr)

	// A correctly framed but garbage payload.
	// Two bytes cannot hold the 22-byte request header, so decoding fails.
	if err := protocol.WriteFrame(nc, []byte{0xde, 0xad}); err != nil {
		t.Fatal(err)
	}
	if r := readResp(t, nc); r.Status != protocol.StatusBadRequest {
		t.Fatalf("want BAD_REQUEST, got %v", r.Status)
	}
	// Unknown op.
	// Decodes fine but fails Validate: the semantic check.
	protocol.WriteFrame(nc, protocol.AppendRequest(nil, &protocol.Request{Op: 200, Key: "k"}))
	if r := readResp(t, nc); r.Status != protocol.StatusBadRequest {
		t.Fatalf("want BAD_REQUEST for unknown op, got %v", r.Status)
	}
	// The same connection still serves valid requests.
	protocol.WriteFrame(nc, protocol.AppendRequest(nil, &protocol.Request{Op: protocol.OpGet, Key: "ok"}))
	if r := readResp(t, nc); r.Status != protocol.StatusOK {
		t.Fatalf("want OK after bad requests, got %v", r.Status)
	}
	// Exactly the two bad requests were counted.
	if got := s.Stats().BadRequests; got != 2 {
		t.Fatalf("bad request counter = %d, want 2", got)
	}
}

// TestOversizedFrameClosesConnection proves the "unrecoverable" half: a
// header claiming a 1 GiB frame (limit 64 bytes) is refused from the header
// alone (no gigantic allocation), answered with BAD_REQUEST, and then the
// connection is closed, because the unread body would desynchronise the
// stream. The final Read returning an error shows the close happened.
func TestOversizedFrameClosesConnection(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.Limits.MaxFrameSize = 64
	_, addr := startServer(t, cfg, echoHandler)
	nc := rawConn(t, addr)

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 1<<30) // claims 1 GiB
	// Only the 4-byte header is sent; the server must not wait for the body.
	nc.Write(hdr[:])
	if r := readResp(t, nc); r.Status != protocol.StatusBadRequest {
		t.Fatalf("want BAD_REQUEST, got %v", r.Status)
	}
	if _, err := nc.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected connection to be closed after oversized frame")
	}
}

// TestOversizedValueRejected proves Validate's size limits are enforced
// server-side. Note err == nil: a BAD_REQUEST is an application-level
// response delivered normally, not a transport failure, because the frame
// itself was fine.
func TestOversizedValueRejected(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.Limits.MaxValueSize = 8
	_, addr := startServer(t, cfg, echoHandler)
	pool := NewPool(addr, time.Second, 1, 0)
	defer pool.Close()
	resp, err := pool.Do(ctxTimeout(t, time.Second), &protocol.Request{Op: protocol.OpPut, Key: "k", Value: make([]byte, 9)})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != protocol.StatusBadRequest {
		t.Fatalf("want BAD_REQUEST, got %v", resp.Status)
	}
}

// TestRequestTimeoutAgainstUnresponsiveServer proves a request to a hung
// process fails at the caller's deadline instead of hanging forever, and
// that the error is classed as a transport error (so failure detection
// counts it). This is why every wait in the client is bounded by ctx.
func TestRequestTimeoutAgainstUnresponsiveServer(t *testing.T) {
	// A listener that accepts but never answers simulates a hung process.
	// The kernel completes the TCP handshake and buffers our request bytes, so
	// from the client's side the connection looks perfectly healthy; only the
	// missing response reveals the problem. Timeouts are the only defence.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// Keep accepted connections open (and close them at the end), so the
	// client does not see a reset instead of silence.
	var held []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	defer func() {
		mu.Lock()
		for _, c := range held {
			c.Close()
		}
		mu.Unlock()
	}()

	pool := NewPool(ln.Addr().String(), time.Second, 1, 0)
	defer pool.Close()
	start := time.Now()
	_, err = pool.Do(ctxTimeout(t, 150*time.Millisecond), &protocol.Request{Op: protocol.OpGet, Key: "k"})
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	if !IsTransportError(err) {
		t.Fatalf("timeout should count as transport error")
	}
	// Generous bound: it must fail around 150ms, certainly not seconds later.
	if elapsed > time.Second {
		t.Fatalf("request took %v, deadline not enforced", elapsed)
	}
}

// TestDialRefused: dialing a port where nothing listens (we bind a port and
// immediately close it) fails fast with ECONNREFUSED, which must count as a
// transport error: that is how a crashed node gets detected.
func TestDialRefused(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	pool := NewPool(addr, 200*time.Millisecond, 1, 0)
	_, err := pool.Do(ctxTimeout(t, time.Second), &protocol.Request{Op: protocol.OpPing})
	if err == nil || !IsTransportError(err) {
		t.Fatalf("want transport error, got %v", err)
	}
}

// TestIdleTimeoutClosesConnection proves the server reclaims connections
// that never send anything: after IdleTimeout the read deadline fires and
// the connection (its fd, goroutine and buffers) is released. Without this,
// abandoned clients would leak resources until MaxConns or the fd limit is
// hit. It polls Stats because the close happens asynchronously.
func TestIdleTimeoutClosesConnection(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.IdleTimeout = 100 * time.Millisecond
	s, addr := startServer(t, cfg, echoHandler)
	nc := rawConn(t, addr)
	_ = nc
	deadline := time.Now().Add(2 * time.Second)
	for s.Stats().ActiveConns != 0 {
		if time.Now().After(deadline) {
			t.Fatal("idle connection was not closed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPoolRetriesStaleConnectionAfterServerRestart simulates a node
// restarting on the same address: the pool's existing connection dies with
// the old server, and the next request must still succeed against the new
// server, transparently to the caller.
//
// Two mechanisms can make that happen: Pool.conn notices a connection
// already marked Broken (its readLoop saw the close) and dials a new one,
// and Pool.Do retries once on a stale-connection error for the window where
// the break has not been noticed yet. (With 4 slots, the second request
// actually lands on a different, never-dialed slot, so this test mainly
// proves recovery after a restart rather than forcing the retry path.)
func TestPoolRetriesStaleConnectionAfterServerRestart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	quiet := log.New(io.Discard, "", 0)
	cfg := DefaultServerConfig()
	cfg.Logger = quiet
	s1 := NewServer(cfg, echoHandler)
	go s1.Serve(ln)

	pool := NewPool(addr, time.Second, 4, 0)
	defer pool.Close()
	if _, err := pool.Do(ctxTimeout(t, time.Second), &protocol.Request{Op: protocol.OpPing}); err != nil {
		t.Fatal(err)
	}
	s1.Shutdown(ctxTimeout(t, time.Second)) // pooled conn is now dead

	// Re-listen on the exact same port. The OS can refuse briefly after a close,
	// in which case the test is skipped rather than failing spuriously.
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("could not rebind %s: %v", addr, err)
	}
	s2 := NewServer(cfg, echoHandler)
	go s2.Serve(ln2)
	defer s2.Shutdown(ctxTimeout(t, time.Second))

	if _, err := pool.Do(ctxTimeout(t, time.Second), &protocol.Request{Op: protocol.OpPing}); err != nil {
		t.Fatalf("request after restart should succeed via retry: %v", err)
	}
}

// TestGracefulShutdownWaitsForInFlight proves graceful shutdown: while a
// request is inside a slow handler, Shutdown must NOT return, and once the
// handler finishes the client must receive a normal successful response.
// Channels (started, release) make the timing deterministic instead of
// relying on sleeps for correctness.
func TestGracefulShutdownWaitsForInFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slow := func(ctx context.Context, req *protocol.Request) *protocol.Response {
		close(started)
		<-release
		return &protocol.Response{Status: protocol.StatusOK}
	}
	cfg := DefaultServerConfig()
	cfg.Logger = log.New(io.Discard, "", 0)
	s := NewServer(cfg, slow)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go s.Serve(ln)

	pool := NewPool(ln.Addr().String(), time.Second, 1, 0)
	errc := make(chan error, 1)
	go func() {
		_, err := pool.Do(ctxTimeout(t, 3*time.Second), &protocol.Request{Op: protocol.OpPing})
		errc <- err
	}()
	// The handler is now running.
	<-started
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- s.Shutdown(ctxTimeout(t, 3*time.Second)) }()
	// Give Shutdown time to (wrongly) finish if it were not waiting.
	time.Sleep(50 * time.Millisecond)
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned while a request was in flight")
	default:
	}
	// Let the handler finish; its response must still be delivered.
	close(release)
	if err := <-errc; err != nil {
		t.Fatalf("in-flight request failed during graceful shutdown: %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}

// TestMaxConns proves the connection cap: with MaxConns = 1 and one
// connection held open by a pool, a second connection is told UNAVAILABLE
// (via a frame with ID 0) and closed, instead of being served. This protects
// the server's file descriptors and memory from a connection flood.
func TestMaxConns(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.MaxConns = 1
	_, addr := startServer(t, cfg, echoHandler)
	c1 := NewPool(addr, time.Second, 1, 0)
	defer c1.Close()
	if _, err := c1.Do(ctxTimeout(t, time.Second), &protocol.Request{Op: protocol.OpPing}); err != nil {
		t.Fatal(err)
	}
	// c1 keeps its connection idle in the pool, so a second one is rejected.
	nc := rawConn(t, addr)
	if r := readResp(t, nc); r.Status != protocol.StatusUnavailable {
		t.Fatalf("want UNAVAILABLE, got %v", r.Status)
	}
}

// TestMuxOutOfOrderResponses sends many concurrent requests over ONE
// multiplexed connection to a handler with random delays, so responses come
// back out of order; every caller must still get its own response.
//
// The delay (0-6ms, based on key length) makes later requests often finish
// first. Each caller checks both the echoed key and the echoed version, so a
// response routed to the wrong caller would be detected. AcceptedConns == 1
// proves all 200 really shared one socket: this is the core property of
// request-ID multiplexing.
func TestMuxOutOfOrderResponses(t *testing.T) {
	slowEcho := func(_ context.Context, req *protocol.Request) *protocol.Response {
		time.Sleep(time.Duration(len(req.Key)%7) * time.Millisecond)
		return echoHandler(context.Background(), req)
	}
	s, addr := startServer(t, DefaultServerConfig(), slowEcho)
	pool := NewPool(addr, time.Second, 1, 0)
	defer pool.Close()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d-%s", i, strings.Repeat("x", i%7))
			resp, err := pool.Do(ctxTimeout(t, 5*time.Second), &protocol.Request{Op: protocol.OpGet, Key: key, Version: uint64(i)})
			if err != nil {
				t.Error(err)
				return
			}
			if string(resp.Value) != key+"=" || resp.Version != uint64(i) {
				t.Errorf("request %d got response for %q (version %d)", i, resp.Value, resp.Version)
			}
		}(i)
	}
	wg.Wait()
	if got := s.Stats().AcceptedConns; got != 1 {
		t.Fatalf("expected all requests on 1 connection, server accepted %d", got)
	}
}

// TestMuxTimeoutDoesNotPoisonConnection: a caller that times out must not
// receive (or cause anyone else to receive) the late response.
//
// The "slow" request times out after 50ms, but its response still arrives
// about 300ms after it was sent. Because Do removed its pending entry,
// readLoop drops it. The loop of fast requests runs for about 400ms (20 x
// 20ms), spanning the moment the stray response arrives, and every one must
// get "fast=". Without ID matching, the stray response would be handed to
// whichever fast request was waiting at that moment.
func TestMuxTimeoutDoesNotPoisonConnection(t *testing.T) {
	h := func(_ context.Context, req *protocol.Request) *protocol.Response {
		if req.Key == "slow" {
			time.Sleep(300 * time.Millisecond)
		}
		return echoHandler(context.Background(), req)
	}
	_, addr := startServer(t, DefaultServerConfig(), h)
	pool := NewPool(addr, time.Second, 1, 0)
	defer pool.Close()

	_, err := pool.Do(ctxTimeout(t, 50*time.Millisecond), &protocol.Request{Op: protocol.OpGet, Key: "slow"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	// While the slow response is still pending server-side, and after it
	// arrives, other requests on the same connection get their own answers.
	for i := 0; i < 20; i++ {
		resp, err := pool.Do(ctxTimeout(t, time.Second), &protocol.Request{Op: protocol.OpGet, Key: "fast"})
		if err != nil || string(resp.Value) != "fast=" {
			t.Fatalf("iteration %d: %q %v", i, resp.Value, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMuxConnectionFailureFailsAllPending: when the server dies, every
// request in flight on the connection fails promptly (not at its timeout).
//
// Ten requests block in the handler with a 10s deadline. A forced Shutdown
// (10ms grace) closes the connection; MuxConn.fail must then wake all ten
// with a transport error well within 2s. Some may instead succeed, because
// Shutdown also cancels the handlers' ctx, which lets them answer before the
// socket closes; either outcome is acceptable, hanging is not.
func TestMuxConnectionFailureFailsAllPending(t *testing.T) {
	block := make(chan struct{})
	h := func(ctx context.Context, req *protocol.Request) *protocol.Response {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return &protocol.Response{Status: protocol.StatusOK}
	}
	cfg := DefaultServerConfig()
	cfg.Logger = log.New(io.Discard, "", 0)
	s := NewServer(cfg, h)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go s.Serve(ln)
	defer close(block)

	pool := NewPool(ln.Addr().String(), time.Second, 1, 0)
	defer pool.Close()
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, err := pool.Do(ctxTimeout(t, 10*time.Second), &protocol.Request{Op: protocol.OpPing})
			errs <- err
		}()
	}
	// Let all ten requests reach the handler.
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	s.Shutdown(sctx) // forced: closes connections under the blocked handlers
	for i := 0; i < 10; i++ {
		err := <-errs
		if err == nil {
			continue // a handler may have been released by ctx cancellation and answered
		}
		if !IsTransportError(err) {
			t.Fatalf("want transport error, got %v", err)
		}
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("pending requests took %v to fail", d)
	}
}

// TestInlineWhenIdleServesConcurrentMuxLoad checks the inline fast path
// (used by storage nodes) stays correct when a multiplexed client pipelines
// many requests: requests must still each get their own response.
//
// 64 goroutines share one connection, so the server constantly flips
// between running handlers inline (idle connection) and in goroutines
// (requests already buffered). The final check sanity-checks the coalescing
// counters: frames were counted, and there are never more write calls than
// frames (coalescing can only merge frames, never split them). The counters
// are process-wide, so they include the other tests' traffic too.
func TestInlineWhenIdleServesConcurrentMuxLoad(t *testing.T) {
	cfg := DefaultServerConfig()
	cfg.InlineWhenIdle = true
	_, addr := startServer(t, cfg, echoHandler)
	pool := NewPool(addr, time.Second, 1, 0)
	defer pool.Close()
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("g%d-%d", g, i)
				resp, err := pool.Do(ctxTimeout(t, 5*time.Second), &protocol.Request{Op: protocol.OpGet, Key: key})
				if err != nil || string(resp.Value) != key+"=" {
					t.Errorf("%s: %q %v", key, resp.Value, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if ws := ReadWriteStats(); ws.Frames == 0 || ws.WriteSyscalls == 0 || ws.WriteSyscalls > ws.Frames {
		t.Fatalf("write stats inconsistent: %+v", ws)
	}
}
