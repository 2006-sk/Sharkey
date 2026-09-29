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

// client.go: the client side of the transport, in three layers.
//
//	Conn     one request at a time on one socket (simple; used by the
//	         benchmark to model independent clients)
//	MuxConn  many concurrent requests on one socket, matched by request ID
//	Pool     a few MuxConns per address, lazy dialing, one retry on a
//	         stale connection (used by the coordinator to reach each node,
//	         and by the client library)
//
// The progression mirrors the README's story: the original sequential
// transport paid a syscall pair per request per connection; multiplexing
// plus write coalescing let many requests share each syscall.

// dialTCP opens a TCP connection with the options every client here wants.
func dialTCP(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	// Timeout bounds the TCP handshake (connect), so dialing a machine that
	// silently drops packets fails in bounded time instead of waiting for the
	// OS's much longer default. KeepAlive makes the OS send TCP keepalive probes
	// on an idle connection (starting after about 30s idle), so a peer that
	// vanished without closing the connection (power loss, network partition)
	// is eventually detected even if we never send on the socket.
	d := net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	// DialContext also gives up early if ctx is cancelled or expires.
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	// Disable Nagle's algorithm (Go already does by default; this is explicit):
	// requests are small and latency-sensitive, and batching is done on purpose
	// in user space by frameWriter.
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return nc, nil
}

// Conn is a sequential client connection: one request at a time, like a
// simple independent client. It is not safe for concurrent use. The
// benchmark uses one Conn per simulated client; shared, concurrent access
// goes through Pool, which multiplexes.
//
// Each request is a full round trip (write request, then block reading the
// response) before the next can start, so a Conn never has more than one
// request in flight. That makes it the most realistic model of an
// independent client, and it needs neither frameWriter nor a reader
// goroutine: it writes straight to the socket and reads the reply itself.
//
// Fields: nc is the socket; br buffers reads (16 KiB) so a response usually
// costs one read syscall; buf is reused to encode every request (no
// per-request allocation); maxFrame bounds response size; nextID numbers
// requests so a response can be checked against its request.
type Conn struct {
	nc       net.Conn
	br       *bufio.Reader
	buf      []byte
	maxFrame int
	nextID   uint32
}

// Dial opens a sequential connection, bounded by timeout and ctx.
// maxFrame <= 0 selects protocol.DefaultMaxFrameSize.
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
//
// Go sockets do not take a context; they take deadlines (absolute times
// after which Read/Write fail with a timeout). Do bridges the two: it copies
// ctx's deadline onto the socket, and arranges for cancellation (which has
// no deadline) to force an immediate timeout.
//
// If Do fails part-way (timeout, cancellation) the server may still send
// the response later, which would then be read as the answer to the NEXT
// request; roundTrip's ID check catches that as ErrMalformed. So after an
// error a Conn should be treated as unusable and redialled.
func (c *Conn) Do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	// Already cancelled or expired: don't touch the network at all.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dl, _ := ctx.Deadline() // zero time = no deadline
	// One call sets both the read and the write deadline. A zero dl clears any
	// deadline left over from a previous request.
	if err := c.nc.SetDeadline(dl); err != nil {
		return nil, err
	}
	// Abort blocking I/O promptly if ctx is cancelled (not just on deadline).
	// context.AfterFunc runs the function in its own goroutine when ctx is done.
	// Setting a deadline in the past (1 second after the Unix epoch) makes any
	// blocked Read or Write return a timeout error immediately; this is the
	// standard way to interrupt blocking socket I/O in Go.
	stop := context.AfterFunc(ctx, func() { _ = c.nc.SetDeadline(time.Unix(1, 0)) })
	// stop unregisters the callback once the round trip is over, so a later
	// cancellation of ctx cannot disturb the NEXT request on this Conn.
	defer stop()
	resp, err := c.roundTrip(req)
	// Replace a bare "i/o timeout" with the context error the caller expects.
	if err != nil {
		return nil, ctxErr(ctx, err)
	}
	return resp, nil
}

// DoDeadline is Do without a context: the request must complete by
// deadline. It avoids per-request context allocation on hot loops such as
// the benchmark's workers.
//
// (context.WithTimeout allocates a context and a timer on every call; on a
// hot benchmark loop that garbage is measurable and would distort the very
// numbers being measured.)
func (c *Conn) DoDeadline(req *protocol.Request, deadline time.Time) (*protocol.Response, error) {
	if err := c.nc.SetDeadline(deadline); err != nil {
		return nil, err
	}
	return c.roundTrip(req)
}

// roundTrip does one write-then-read exchange. The caller has already set
// the socket deadlines, so neither step can block forever.
func (c *Conn) roundTrip(req *protocol.Request) (*protocol.Response, error) {
	// A fresh ID per request, so a stale response can be told apart.
	c.nextID++
	// Copy the request: the caller's struct is not mutated (its ID stays as it
	// was), so it can be safely reused or shared.
	r := *req
	r.ID = c.nextID
	// Encode the whole frame into the reused buffer ([:0] keeps the capacity)
	// and send it with a single Write: one syscall, one TCP segment for small
	// requests.
	c.buf = protocol.AppendRequestFrame(c.buf[:0], &r)
	if _, err := c.nc.Write(c.buf); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	// Blocks until a whole response frame has arrived (or the deadline hits).
	payload, err := protocol.ReadFrame(c.br, c.maxFrame)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	resp, err := protocol.DecodeResponse(payload)
	if err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	// Sequential connection, so the response must answer THIS request. A
	// mismatch means the stream is out of step (e.g. a late reply to an earlier
	// request that timed out).
	if resp.ID != r.ID && resp.ID != 0 { // 0: connection-level error from the server
		return nil, fmt.Errorf("%w: response id %d for request %d", protocol.ErrMalformed, resp.ID, r.ID)
	}
	return resp, nil
}

// Close closes the connection.
// Closing releases the socket's file descriptor; the server sees EOF.
func (c *Conn) Close() error { return c.nc.Close() }

// ctxErr prefers the context's error (clearer than "i/o timeout") when the
// failure was caused by cancellation or deadline. The socket deadline is set
// from ctx, so a socket timeout means ctx's deadline passed even if ctx's own
// timer hasn't fired yet.
//
// Both errors are kept: the result wraps the context error (so
// errors.Is(err, context.DeadlineExceeded) works for callers such as failure
// detection) and includes the original I/O error text for debugging.
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
//
// How a request flows:
//
//	caller goroutine                          readLoop goroutine
//	----------------                          ------------------
//	id := next ID; pending[id] = ch
//	frameWriter.write(frame with id) ──▶ socket ──▶ server
//	                                          ReadFrame  ◀── socket ◀── response
//	                                          ch := pending[id]; delete
//	res := <-ch  ◀─────────────────────────── ch <- response
//
// Many callers can be between those two steps at the same time: that is the
// point. Compare with Conn, where the socket is idle while the server works
// on the one outstanding request, and a second caller would have to wait
// (or open its own socket). One multiplexed socket per node carries all
// traffic, so frames concentrate on it and frameWriter can batch them (the
// README's "one connection per node (instead of four) ... gave +15%").
//
// Fields:
//   - nc, w, maxFrame: the socket, its shared coalescing writer, and the
//     largest response frame accepted.
//   - inflight: requests sent but not yet answered; lock-free because the
//     writer reads it (as w.busy) when a flush starts, to decide whether
//     to linger.
//   - mu: guards the three fields below it.
//   - nextID: the last ID handed out.
//   - pending: ID -> the channel of the caller waiting for that response.
//   - err: set once when the connection breaks; afterwards every Do fails
//     immediately with it.
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

// muxResult is what readLoop or fail delivers to a waiting caller: either
// a response or an error, never both.
type muxResult struct {
	resp *protocol.Response
	err  error
}

// DialMux opens a multiplexed connection.
//
// dialTimeout bounds the connect; writeTimeout is the per-Write deadline
// used by the frameWriter. The reader goroutine is started immediately and
// runs until the connection fails or is closed.
func DialMux(ctx context.Context, addr string, dialTimeout, writeTimeout time.Duration, maxFrame int) (*MuxConn, error) {
	nc, err := dialTCP(ctx, addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	if maxFrame <= 0 {
		maxFrame = protocol.DefaultMaxFrameSize
	}
	// Clients linger under load like the server does, using lingerDur.
	m := &MuxConn{
		nc:       nc,
		w:        &frameWriter{nc: nc, timeout: writeTimeout, linger: lingerDur},
		maxFrame: maxFrame,
		pending:  make(map[uint32]chan muxResult),
	}
	// Wire the writer's "busy" signal to the in-flight counter. m.inflight.Load
	// is a method value: a func() int32 bound to this MuxConn.
	m.w.busy = m.inflight.Load
	// Exactly one reader per connection: only it reads from the socket, so
	// frames are consumed whole and in order.
	go m.readLoop()
	return m, nil
}

// Do sends req and waits for its response or for ctx to end. If ctx ends
// first the caller's slot is removed, so a late response is dropped instead
// of being delivered to a different caller.
//
// It is safe for concurrent use; that is the whole point of MuxConn. Note
// that giving up on the response does not cancel the work on the server:
// the request may still execute there. That is one reason every operation
// in this system is designed to be idempotent.
func (m *MuxConn) Do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	// Buffered with capacity 1 so the sender (readLoop or fail) NEVER blocks,
	// even if this caller has already given up and nobody will ever receive.
	// Race example: readLoop removes our entry and is about to send just as our
	// ctx expires; we return, and the send still completes into the buffer. The
	// unread channel is then garbage-collected. Unbuffered, readLoop would
	// block forever and stall every other request on the connection.
	ch := make(chan muxResult, 1)
	m.mu.Lock()
	// Connection already broken: fail fast rather than queue a doomed frame.
	if m.err != nil {
		err := m.err
		m.mu.Unlock()
		return nil, err
	}
	// IDs are unique per connection (not globally), which is all that
	// matters: responses come back on the same connection.
	m.nextID++
	// After 2^32 requests the uint32 wraps around to 0, which is reserved.
	// (A wrapped ID could only collide with a request still pending from 4
	// billion requests ago, which is not a practical concern.)
	if m.nextID == 0 { // 0 is reserved for connection-level errors
		m.nextID++
	}
	// Copy the request so the caller's struct keeps its own ID. Pool.Do relies
	// on this to retry the same req on another connection.
	r := *req
	r.ID = m.nextID
	// Register BEFORE sending, so the response can never arrive before the
	// entry exists (the server might answer very quickly).
	m.pending[r.ID] = ch
	m.mu.Unlock()
	// Count this request as in flight while we send and wait, so the writer can
	// tell whether more frames are about to follow (linger decision).
	m.inflight.Add(1)
	defer m.inflight.Add(-1)

	// Encode directly into the shared send buffer; this may coalesce with other
	// callers' frames into a single syscall.
	if err := m.w.write(func(b []byte) []byte { return protocol.AppendRequestFrame(b, &r) }); err != nil {
		// A failed write breaks the connection for everyone: part of a frame may
		// have been sent, so the byte stream can no longer be trusted.
		m.fail(err)
		return nil, fmt.Errorf("write: %w", err)
	}
	// Wait for whichever comes first: our response (or a connection failure)
	// from readLoop/fail, or the caller's deadline/cancellation.
	select {
	case res := <-ch:
		return res.resp, res.err
	case <-ctx.Done():
		// Remove our entry so a late response is recognised as unwanted and
		// dropped. If readLoop already took it, this delete is a no-op and its send
		// lands in the buffered channel, unread.
		m.mu.Lock()
		delete(m.pending, r.ID)
		m.mu.Unlock()
		return nil, fmt.Errorf("%w (waiting for response)", ctx.Err())
	}
}

// readLoop is the single reader goroutine of a MuxConn. It reads response
// frames forever and routes each one to the caller waiting for its ID. It
// exits (failing all pending requests) on any read or decode error.
//
// It never blocks on a slow caller: channels are buffered and the pending
// lookup is a quick map operation under mu, so one slow consumer cannot
// hold up responses for everybody else.
func (m *MuxConn) readLoop() {
	// 64 KiB buffer: one read syscall can pick up many coalesced responses.
	br := bufio.NewReaderSize(m.nc, 64<<10)
	for {
		// No read deadline here: an idle multiplexed connection is fine. Per
		// request timeouts are enforced by each caller's ctx in Do; a dead peer is
		// detected by read errors, TCP keepalive, or the coordinator's failure
		// detector closing the pool (CloseConns).
		payload, err := protocol.ReadFrame(br, m.maxFrame)
		if err != nil {
			m.fail(fmt.Errorf("read: %w", err))
			return
		}
		// A response that cannot be decoded means the stream is corrupt (or the
		// peer speaks a different protocol); nothing after it can be trusted.
		resp, err := protocol.DecodeResponse(payload)
		if err != nil {
			m.fail(fmt.Errorf("decode response: %w", err))
			return
		}
		// ID 0 never belongs to a request (Do skips it). The server uses it for
		// errors about the connection itself, such as MaxConns rejection or an
		// oversized frame, after which it closes the connection.
		if resp.ID == 0 {
			// Connection-level error from the server (e.g. connection limit).
			m.fail(fmt.Errorf("server closed connection: %w", resp.Err()))
			return
		}
		// Claim the waiter: look up and delete under the lock, so exactly one of
		// readLoop / Do's timeout path / fail ends up owning the channel.
		m.mu.Lock()
		ch := m.pending[resp.ID]
		delete(m.pending, resp.ID)
		m.mu.Unlock()
		// nil: the caller timed out and removed its entry; drop the late response.
		if ch != nil {
			// Sent outside the lock; never blocks (capacity 1, exactly one send).
			ch <- muxResult{resp: resp}
		}
	}
}

// fail marks the connection broken and fails every pending request.
//
// It may be called several times and from several goroutines (readLoop on a
// read error, Do on a write error, Close); only the first error is kept, and
// each pending caller receives exactly one result because the map is
// swapped out under the lock. Failing everyone at once matters: without it,
// callers whose requests were in flight would each wait for their own
// timeout even though the answer can never come.
func (m *MuxConn) fail(err error) {
	m.mu.Lock()
	// First failure wins, so the reported cause is the original one.
	if m.err == nil {
		m.err = fmt.Errorf("connection lost: %w", err)
	}
	// Take the whole pending map and install an empty one. Any Do that runs
	// after this sees m.err and fails fast, so no new entries are added to a
	// dead connection.
	pending := m.pending
	m.pending = map[uint32]chan muxResult{}
	failErr := m.err
	m.mu.Unlock()
	// Closing the socket also wakes readLoop's blocked Read (it then calls
	// fail again, harmlessly) and makes later writes fail. Done outside mu.
	m.nc.Close()
	// Deliver the error to every waiter. Buffered channels: never blocks.
	for _, ch := range pending {
		ch <- muxResult{err: failErr}
	}
}

// Broken reports whether the connection has failed.
// Pool uses it to decide whether a slot needs a new connection.
func (m *MuxConn) Broken() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err != nil
}

// Close closes the connection, failing pending requests.
// It fails them with net.ErrClosed, which isStaleConnErr recognises.
func (m *MuxConn) Close() error {
	m.fail(net.ErrClosed)
	return nil
}

// Pool is a goroutine-safe client for one address. It keeps a fixed number
// of multiplexed connections and spreads requests over them round-robin, so
// thousands of concurrent callers share a handful of sockets and their
// requests/responses are batched into far fewer syscalls.
//
// Connections are dialed lazily (on first use) and replaced automatically
// when they break. Using a small fixed set, rather than one socket per
// caller, also keeps file-descriptor usage bounded no matter how many
// goroutines call Do.
//
// Fields:
//   - addr: target "host:port".
//   - dialTimeout / writeTimeout: passed to DialMux (writeTimeout is 5s).
//   - maxFrame: largest response frame accepted.
//   - slots: one entry per connection; each slot has its own lock, so
//     dialing in one slot never blocks callers using another.
//   - next: round-robin counter, advanced atomically by every call.
//   - closed: set by Close; later calls fail with errPoolClosed.
type Pool struct {
	addr         string
	dialTimeout  time.Duration
	writeTimeout time.Duration
	maxFrame     int
	slots        []poolSlot
	next         atomic.Uint32
	closed       atomic.Bool
}

// poolSlot holds one (possibly not yet dialed) connection of a Pool.
//
// mu serialises dialing: if the connection is missing or broken, the first
// caller dials while the others wait for its result, instead of all of them
// dialing at once (a "thundering herd" of connects). dialErr and dialedAt
// remember the last failed dial for dialBackoff.
type poolSlot struct {
	mu       sync.Mutex
	c        *MuxConn
	dialErr  error
	dialedAt time.Time
}

// dialBackoff: after a failed dial, callers of the same slot get the error
// immediately for this long instead of each waiting out their own dial.
//
// Without it, when a node is down every request for it would queue on the
// slot mutex and pay a full dial attempt in turn. Caching the failure
// briefly turns that into instant errors, which the replication layer
// handles by falling back to other replicas.
const dialBackoff = 50 * time.Millisecond

// NewPool creates a pool of `conns` multiplexed connections (dialed lazily).
// maxFrame <= 0 lets DialMux use protocol.DefaultMaxFrameSize.
func NewPool(addr string, dialTimeout time.Duration, conns, maxFrame int) *Pool {
	if conns <= 0 {
		conns = 1
	}
	return &Pool{addr: addr, dialTimeout: dialTimeout, writeTimeout: 5 * time.Second, maxFrame: maxFrame, slots: make([]poolSlot, conns)}
}

// Addr returns the pool's target address.
func (p *Pool) Addr() string { return p.addr }

// errPoolClosed is returned once the pool has been closed.
var errPoolClosed = errors.New("transport: pool closed")

// conn picks the next slot round-robin and returns its connection, dialing
// a new one if the slot is empty or its connection is broken. fresh reports
// whether the connection was just dialed (Pool.Do uses it to avoid pointless
// retries).
func (p *Pool) conn(ctx context.Context) (c *MuxConn, fresh bool, err error) {
	if p.closed.Load() {
		return nil, false, errPoolClosed
	}
	// Round-robin: an atomic counter spreads successive calls across slots
	// without any shared lock.
	s := &p.slots[int(p.next.Add(1))%len(p.slots)]
	// Held for the whole function, including a dial: see poolSlot.
	s.mu.Lock()
	defer s.mu.Unlock()
	// Fast path: a healthy connection already exists.
	if s.c != nil && !s.c.Broken() {
		return s.c, false, nil
	}
	// A recent dial failed: return that error right away (see dialBackoff).
	if s.dialErr != nil && time.Since(s.dialedAt) < dialBackoff {
		return nil, false, s.dialErr
	}
	// Dial under the slot lock; other callers of this slot wait for it.
	c, err = DialMux(ctx, p.addr, p.dialTimeout, p.writeTimeout, p.maxFrame)
	s.dialedAt = time.Now()
	// On failure s.c becomes nil and the error is cached for dialBackoff.
	s.c, s.dialErr = c, err
	if err != nil {
		return nil, false, err
	}
	// Close may have run while we were dialing; don't leak a connection that
	// no one will ever close.
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
//
// Idempotent means doing it twice has the same effect as doing it once. That
// matters because after a connection error we cannot know whether the first
// attempt reached the server: it may have been applied and only the
// response lost. Re-sending "PUT k=v at version 7" is harmless even if the
// node already applied it; a non-idempotent operation (say "increment k")
// could not be retried blindly like this.
func (p *Pool) Do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	c, fresh, err := p.conn(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(ctx, req)
	// Retry only when all of these hold:
	//   - err != nil: the attempt failed;
	//   - !fresh: the connection was an old pooled one that may have gone stale
	//     while idle (e.g. the node restarted). If a just-dialed connection
	//     fails, the peer is really in trouble and retrying would only add
	//     latency;
	//   - ctx.Err() == nil: the caller still has time;
	//   - isStaleConnErr: the error says "connection was closed", not "the
	//     server is slow" (timeouts are never retried).
	// And only once, so a retry can never turn into a loop.
	if err != nil && !fresh && ctx.Err() == nil && isStaleConnErr(err) {
		if c, _, err = p.conn(ctx); err != nil {
			return nil, err
		}
		resp, err = c.Do(ctx, req)
	}
	// Prefix the address and op so errors say which node and operation failed.
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", p.addr, req.Op, err)
	}
	return resp, nil
}

// CloseConns closes all connections; requests in flight on them fail and
// later requests dial new ones. Used when the peer is detected as failed or
// restarted, so we never keep using sockets to a dead process.
//
// It also clears any cached dial error, so the next request dials
// immediately rather than waiting out dialBackoff.
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
// closed is set first, so conn refuses new work (and closes any connection
// that finishes dialing afterwards) before existing connections are torn down.
func (p *Pool) Close() {
	p.closed.Store(true)
	p.CloseConns()
}

// isStaleConnErr reports errors that indicate the connection was closed
// underneath us.
//
//   - io.EOF: the peer closed the connection cleanly;
//   - io.ErrUnexpectedEOF: it closed in the middle of a frame;
//   - ECONNRESET: the peer's kernel answered with a TCP RST (for example the
//     process died or restarted and the old connection no longer exists);
//   - EPIPE: we wrote to a connection the peer had already closed;
//   - net.ErrClosed: we closed it ourselves (Close, CloseConns).
//
// errors.Is unwraps the fmt.Errorf("%w") chains built by MuxConn, so the
// original cause is still found.
func isStaleConnErr(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, net.ErrClosed)
}

// IsTransportError reports whether err is a network-level failure (as
// opposed to an application-level error response). Only transport errors
// count toward failure detection.
//
// A node that answers "BAD_REQUEST" or "ERROR" is alive and reachable, so it
// must not be marked down. A timeout, a refused or reset connection, or a
// dial failure suggests the node itself is sick or gone. net.Error covers
// timeouts and most dial/socket errors; ECONNREFUSED means nothing is
// listening on the port (the process is not running). context.Canceled is
// deliberately NOT included: a caller giving up is not the node's fault.
func IsTransportError(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	return errors.As(err, &ne) || isStaleConnErr(err) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syscall.ECONNREFUSED)
}

// isTemporaryAccept reports Accept errors worth retrying after a pause
// (used by Server.Serve, which lives in server.go):
//   - EMFILE: this process hit its file-descriptor limit (see rlimit.go);
//   - ENFILE: the whole system ran out of file descriptors;
//   - ECONNABORTED: a client reset its connection while it waited in the
//     accept queue; the listener itself is fine;
//   - ENOBUFS: the kernel is short of buffer memory.
//
// All may clear up on their own, unlike, say, the listener being closed.
func isTemporaryAccept(err error) bool {
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ENOBUFS)
}
