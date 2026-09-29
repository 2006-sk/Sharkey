package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"distkv/internal/protocol"
)

func dialTCP(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return nc, nil
}

// Conn is a sequential client connection: one request at a time, like a
// simple independent client. It is not safe for concurrent use. The
// benchmark uses one Conn per simulated client; shared, concurrent access
// goes through Pool, which multiplexes.
type Conn struct {
	nc       net.Conn
	br       *bufio.Reader
	buf      []byte
	maxFrame int
	nextID   uint32
}

// Dial opens a sequential connection, bounded by timeout and ctx.
func Dial(ctx context.Context, addr string, timeout time.Duration, maxFrame int) (*Conn, error) {
	nc, err := dialTCP(ctx, addr, timeout)
	if err != nil {
		return nil, err
	}
	if maxFrame <= 0 {
		maxFrame = protocol.DefaultMaxFrameSize
	}
	return &Conn{nc: nc, br: bufio.NewReaderSize(nc, 16<<10), maxFrame: maxFrame}, nil
}

// Do sends req and waits for the response. The deadline comes from ctx; if
// ctx has none the call may block until the server's idle timeout.
func (c *Conn) Do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dl, _ := ctx.Deadline() // zero time = no deadline
	if err := c.nc.SetDeadline(dl); err != nil {
		return nil, err
	}
	// Abort blocking I/O promptly if ctx is cancelled (not just on deadline).
	stop := context.AfterFunc(ctx, func() { _ = c.nc.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	resp, err := c.roundTrip(req)
	if err != nil {
		return nil, ctxErr(ctx, err)
	}
	return resp, nil
}

// DoDeadline is Do without a context: the request must complete by
// deadline. It avoids per-request context allocation on hot loops such as
// the benchmark's workers.
func (c *Conn) DoDeadline(req *protocol.Request, deadline time.Time) (*protocol.Response, error) {
	if err := c.nc.SetDeadline(deadline); err != nil {
		return nil, err
	}
	return c.roundTrip(req)
}

func (c *Conn) roundTrip(req *protocol.Request) (*protocol.Response, error) {
	c.nextID++
	r := *req
	r.ID = c.nextID
	c.buf = protocol.AppendRequestFrame(c.buf[:0], &r)
	if _, err := c.nc.Write(c.buf); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	payload, err := protocol.ReadFrame(c.br, c.maxFrame)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	resp, err := protocol.DecodeResponse(payload)
	if err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if resp.ID != r.ID && resp.ID != 0 { // 0: connection-level error from the server
		return nil, fmt.Errorf("%w: response id %d for request %d", protocol.ErrMalformed, resp.ID, r.ID)
	}
	return resp, nil
}

// Close closes the connection.
func (c *Conn) Close() error { return c.nc.Close() }

// ctxErr prefers the context's error (clearer than "i/o timeout") when the
// failure was caused by cancellation or deadline. The socket deadline is set
// from ctx, so a socket timeout means ctx's deadline passed even if ctx's own
// timer hasn't fired yet.
func ctxErr(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return fmt.Errorf("%w (%v)", cerr, err)
	}
	if _, has := ctx.Deadline(); has && errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("%w (%v)", context.DeadlineExceeded, err)
	}
	return err
}

// MuxConn multiplexes concurrent requests over one TCP connection. Each
// request gets a fresh ID; a reader goroutine routes responses to waiting
// callers by ID, so responses may arrive in any order and a slow request
// doesn't delay others. Writes from concurrent callers are coalesced.
type MuxConn struct {
	nc       net.Conn
	w        *frameWriter
	maxFrame int

	inflight atomic.Int32

	mu      sync.Mutex
	nextID  uint32
	pending map[uint32]chan muxResult
	err     error // non-nil once the connection is broken
}

type muxResult struct {
	resp *protocol.Response
	err  error
}

// DialMux opens a multiplexed connection.
func DialMux(ctx context.Context, addr string, dialTimeout, writeTimeout time.Duration, maxFrame int) (*MuxConn, error) {
	nc, err := dialTCP(ctx, addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	if maxFrame <= 0 {
		maxFrame = protocol.DefaultMaxFrameSize
	}
	m := &MuxConn{
		nc:       nc,
		w:        &frameWriter{nc: nc, timeout: writeTimeout, linger: lingerDur},
		maxFrame: maxFrame,
		pending:  make(map[uint32]chan muxResult),
	}
	m.w.busy = m.inflight.Load
	go m.readLoop()
	return m, nil
}

// Do sends req and waits for its response or for ctx to end. If ctx ends
// first the caller's slot is removed, so a late response is dropped instead
// of being delivered to a different caller.
func (m *MuxConn) Do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	ch := make(chan muxResult, 1)
	m.mu.Lock()
	if m.err != nil {
		err := m.err
		m.mu.Unlock()
		return nil, err
	}
	m.nextID++
	if m.nextID == 0 { // 0 is reserved for connection-level errors
		m.nextID++
	}
	r := *req
	r.ID = m.nextID
	m.pending[r.ID] = ch
	m.mu.Unlock()
	m.inflight.Add(1)
	defer m.inflight.Add(-1)

	if err := m.w.write(func(b []byte) []byte { return protocol.AppendRequestFrame(b, &r) }); err != nil {
		m.fail(err)
		return nil, fmt.Errorf("write: %w", err)
	}
	select {
	case res := <-ch:
		return res.resp, res.err
	case <-ctx.Done():
		m.mu.Lock()
		delete(m.pending, r.ID)
		m.mu.Unlock()
		return nil, fmt.Errorf("%w (waiting for response)", ctx.Err())
	}
}

func (m *MuxConn) readLoop() {
	br := bufio.NewReaderSize(m.nc, 64<<10)
	for {
		payload, err := protocol.ReadFrame(br, m.maxFrame)
		if err != nil {
			m.fail(fmt.Errorf("read: %w", err))
			return
		}
		resp, err := protocol.DecodeResponse(payload)
		if err != nil {
			m.fail(fmt.Errorf("decode response: %w", err))
			return
		}
		if resp.ID == 0 {
			// Connection-level error from the server (e.g. connection limit).
			m.fail(fmt.Errorf("server closed connection: %w", resp.Err()))
			return
		}
		m.mu.Lock()
		ch := m.pending[resp.ID]
		delete(m.pending, resp.ID)
		m.mu.Unlock()
		if ch != nil {
			ch <- muxResult{resp: resp}
		}
	}
}

// fail marks the connection broken and fails every pending request.
func (m *MuxConn) fail(err error) {
	m.mu.Lock()
	if m.err == nil {
		m.err = fmt.Errorf("connection lost: %w", err)
	}
	pending := m.pending
	m.pending = map[uint32]chan muxResult{}
	failErr := m.err
	m.mu.Unlock()
	m.nc.Close()
	for _, ch := range pending {
		ch <- muxResult{err: failErr}
	}
}

// Broken reports whether the connection has failed.
func (m *MuxConn) Broken() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err != nil
}

// Close closes the connection, failing pending requests.
func (m *MuxConn) Close() error {
	m.fail(net.ErrClosed)
	return nil
}

// Pool is a goroutine-safe client for one address. It keeps a fixed number
// of multiplexed connections and spreads requests over them round-robin, so
// thousands of concurrent callers share a handful of sockets and their
// requests/responses are batched into far fewer syscalls.
type Pool struct {
	addr         string
	dialTimeout  time.Duration
	writeTimeout time.Duration
	maxFrame     int
	slots        []poolSlot
	next         atomic.Uint32
	closed       atomic.Bool
}

type poolSlot struct {
	mu       sync.Mutex
	c        *MuxConn
	dialErr  error
	dialedAt time.Time
}

// dialBackoff: after a failed dial, callers of the same slot get the error
// immediately for this long instead of each waiting out their own dial.
const dialBackoff = 50 * time.Millisecond

// NewPool creates a pool of `conns` multiplexed connections (dialed lazily).
func NewPool(addr string, dialTimeout time.Duration, conns, maxFrame int) *Pool {
	if conns <= 0 {
		conns = 1
	}
	return &Pool{addr: addr, dialTimeout: dialTimeout, writeTimeout: 5 * time.Second, maxFrame: maxFrame, slots: make([]poolSlot, conns)}
}

// Addr returns the pool's target address.
func (p *Pool) Addr() string { return p.addr }

var errPoolClosed = errors.New("transport: pool closed")

func (p *Pool) conn(ctx context.Context) (c *MuxConn, fresh bool, err error) {
	if p.closed.Load() {
		return nil, false, errPoolClosed
	}
	s := &p.slots[int(p.next.Add(1))%len(p.slots)]
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.c != nil && !s.c.Broken() {
		return s.c, false, nil
	}
	if s.dialErr != nil && time.Since(s.dialedAt) < dialBackoff {
		return nil, false, s.dialErr
	}
	c, err = DialMux(ctx, p.addr, p.dialTimeout, p.writeTimeout, p.maxFrame)
	s.dialedAt = time.Now()
	s.c, s.dialErr = c, err
	if err != nil {
		return nil, false, err
	}
	if p.closed.Load() { // raced with Close
		c.Close()
		return nil, false, errPoolClosed
	}
	return c, true, nil
}

// Do executes req on one of the pool's connections.
//
// If the connection turns out to be dead (the peer restarted or closed it)
// the request is retried once on a fresh connection. That retry is safe
// because every request in this system is idempotent: reads trivially, and
// writes because they carry a version and nodes apply last-writer-wins.
func (p *Pool) Do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	c, fresh, err := p.conn(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(ctx, req)
	if err != nil && !fresh && ctx.Err() == nil && isStaleConnErr(err) {
		if c, _, err = p.conn(ctx); err != nil {
			return nil, err
		}
		resp, err = c.Do(ctx, req)
	}
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", p.addr, req.Op, err)
	}
	return resp, nil
}

// CloseConns closes all connections; requests in flight on them fail and
// later requests dial new ones. Used when the peer is detected as failed or
// restarted, so we never keep using sockets to a dead process.
func (p *Pool) CloseConns() {
	for i := range p.slots {
		s := &p.slots[i]
		s.mu.Lock()
		if s.c != nil {
			s.c.Close()
			s.c = nil
		}
		s.dialErr = nil
		s.mu.Unlock()
	}
}

// Close closes the pool.
func (p *Pool) Close() {
	p.closed.Store(true)
	p.CloseConns()
}

// isStaleConnErr reports errors that indicate the connection was closed
// underneath us.
func isStaleConnErr(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, net.ErrClosed)
}

// IsTransportError reports whether err is a network-level failure (as
// opposed to an application-level error response). Only transport errors
// count toward failure detection.
func IsTransportError(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	return errors.As(err, &ne) || isStaleConnErr(err) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syscall.ECONNREFUSED)
}

func isTemporaryAccept(err error) bool {
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ENOBUFS)
}
