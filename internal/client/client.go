// Package client is a Go client for the coordinator (or, for debugging, a
// single storage node).
//
// What a client does, and deliberately does not do
// -------------------------------------------------
// The client is intentionally "dumb": it does not know about the hash ring,
// replica sets, quorums, versions or node health. It just frames a request,
// sends it to ONE address (normally the coordinator) and waits for the
// matching response. All distribution logic lives in the coordinator. That is
// the coordinator-based routing tradeoff from README "Tradeoffs": trivial
// clients, at the cost of an extra network hop per request.
//
// Because the wire protocol is the same everywhere, the same client can also
// point straight at a storage node (bin/client --addr :7101 ...). That skips
// replication entirely, so it is only for debugging.
//
// Under the hood each request goes through a transport.Pool of multiplexed TCP
// connections: many requests share one connection, each tagged with an ID so
// responses can come back out of order. A single Client is therefore meant to
// be shared by many goroutines.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"distkv/internal/protocol"
	"distkv/internal/transport"
)

// ErrNotFound is returned by Get for missing or deleted keys.
//
// It is a sentinel error so callers can test for it with errors.Is. A deleted
// key is stored as a tombstone on the nodes, but from the client's point of
// view it is indistinguishable from a key that never existed.
var ErrNotFound = errors.New("key not found")

// Client is safe for concurrent use; it pools connections.
//
// Fields:
//   - pool holds Conns multiplexed connections to one server address. It dials
//     lazily and redials after a connection dies.
//   - timeout is the per-call request timeout applied in Do (0 = none beyond
//     the caller's context).
type Client struct {
	pool    *transport.Pool
	timeout time.Duration
}

// Options configure a client.
//
// Fields:
//   - DialTimeout bounds establishing a TCP connection, so a dead server fails
//     fast instead of hanging in connect().
//   - RequestTimeout bounds each whole call (wait for connection + send +
//     response). The coordinator enforces its own per-replica timeouts inside,
//     so this should be comfortably larger than those.
//   - Conns is how many multiplexed connections to keep. More than one spreads
//     load when a single connection's reader or writer becomes the bottleneck.
type Options struct {
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	Conns          int // multiplexed connections to the server
}

// DefaultOptions returns sensible client defaults.
//
// 1 s to dial, 5 s per request (larger than the coordinator's 1 s per-replica
// timeout, so a request that has to wait out one hung replica can still
// succeed), and 2 connections.
func DefaultOptions() Options {
	return Options{DialTimeout: time.Second, RequestTimeout: 5 * time.Second, Conns: 2}
}

// New creates a client for addr.
//
// No connection is made here; the pool dials on first use. The frame size
// limit is the protocol default, matching what servers accept by default.
func New(addr string, o Options) *Client {
	return &Client{
		pool:    transport.NewPool(addr, o.DialTimeout, o.Conns, protocol.DefaultMaxFrameSize),
		timeout: o.RequestTimeout,
	}
}

// Close releases pooled connections.
func (c *Client) Close() { c.pool.Close() }

// Do sends a raw request, applying the client's request timeout if ctx has
// no earlier deadline.
//
// context.WithTimeout always derives a child context, but a child's deadline
// can never be later than its parent's, so the effective deadline is
// min(parent deadline, now + timeout). That is how "if ctx has no earlier
// deadline" falls out naturally. Do returns the raw response even when its
// status is an error; only transport problems (dial failure, timeout, broken
// connection) come back as err.
func (c *Client) Do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		// Always cancel: it releases the context's timer as soon as we return
		// instead of when the deadline fires.
		defer cancel()
	}
	// The pool picks a connection, tags the request with an ID, sends it and
	// waits for the response with that ID (or for ctx to expire).
	return c.pool.Do(ctx, req)
}

// do is Do plus status checking: an error status from the server (anything
// other than OK or NOT_FOUND) becomes a Go error annotated with the op and
// key. NOT_FOUND is passed through as a normal response so Get can map it to
// ErrNotFound; for Delete it simply means success.
func (c *Client) do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	resp, err := c.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	// resp.Err() is nil for OK and NOT_FOUND. %w keeps the underlying error
	// inspectable with errors.Is/As.
	if err := resp.Err(); err != nil {
		return nil, fmt.Errorf("%s %q: %w", req.Op, req.Key, err)
	}
	return resp, nil
}

// Get returns the value for key, or ErrNotFound.
//
// Against the coordinator this is a replicated read: primary first with
// replica fallback (R=1), or a quorum read (R>=2), depending on its config.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.do(ctx, &protocol.Request{Op: protocol.OpGet, Key: key})
	if err != nil {
		return nil, err
	}
	// Missing key or tombstone: translate the status into the sentinel error.
	if resp.Status == protocol.StatusNotFound {
		return nil, ErrNotFound
	}
	return resp.Value, nil
}

// Put stores value under key.
//
// A nil error means the coordinator got W acknowledgements. An error does NOT
// guarantee the write is absent: it may have reached fewer than W replicas and
// can become visible later (failed writes are not rolled back; see README
// "Replication and consistency").
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	_, err := c.do(ctx, &protocol.Request{Op: protocol.OpPut, Key: key, Value: value})
	return err
}

// Delete removes key. Deleting a missing key succeeds.
//
// Deletes are idempotent: the coordinator writes a versioned tombstone whether
// or not the key existed, so repeating a delete is harmless.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.do(ctx, &protocol.Request{Op: protocol.OpDelete, Key: key})
	return err
}

// Ping returns the raw health document.
//
// Against the coordinator this is the cluster summary (ok/degraded/down and
// node counts); against a node it is that node's own health, including its
// boot ID.
func (c *Client) Ping(ctx context.Context) (json.RawMessage, error) {
	resp, err := c.do(ctx, &protocol.Request{Op: protocol.OpPing})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// Stats returns the raw stats document.
//
// json.RawMessage keeps this package independent of the stats types; callers
// that want structure use StatsInto.
func (c *Client) Stats(ctx context.Context) (json.RawMessage, error) {
	resp, err := c.do(ctx, &protocol.Request{Op: protocol.OpStats})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// StatsInto decodes the stats document into v.
//
// For example, pass a *coordinator.Stats when talking to the coordinator or a
// *node.Stats when talking to a storage node.
func (c *Client) StatsInto(ctx context.Context, v any) error {
	raw, err := c.Stats(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// Locate returns the raw replica-set document for key (coordinator only).
//
// Storage nodes do not know the ring, so they reject LOCATE; the coordinator
// answers with the key's hash and its preference list, each entry tagged
// primary/replica and with its current health state.
func (c *Client) Locate(ctx context.Context, key string) (json.RawMessage, error) {
	resp, err := c.do(ctx, &protocol.Request{Op: protocol.OpLocate, Key: key})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}
