// Package client is a Go client for the coordinator (or, for debugging, a
// single storage node).
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
var ErrNotFound = errors.New("key not found")

// Client is safe for concurrent use; it pools connections.
type Client struct {
	pool    *transport.Pool
	timeout time.Duration
}

// Options configure a client.
type Options struct {
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	Conns          int // multiplexed connections to the server
}

// DefaultOptions returns sensible client defaults.
func DefaultOptions() Options {
	return Options{DialTimeout: time.Second, RequestTimeout: 5 * time.Second, Conns: 2}
}

// New creates a client for addr.
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
func (c *Client) Do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	return c.pool.Do(ctx, req)
}

func (c *Client) do(ctx context.Context, req *protocol.Request) (*protocol.Response, error) {
	resp, err := c.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := resp.Err(); err != nil {
		return nil, fmt.Errorf("%s %q: %w", req.Op, req.Key, err)
	}
	return resp, nil
}

// Get returns the value for key, or ErrNotFound.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.do(ctx, &protocol.Request{Op: protocol.OpGet, Key: key})
	if err != nil {
		return nil, err
	}
	if resp.Status == protocol.StatusNotFound {
		return nil, ErrNotFound
	}
	return resp.Value, nil
}

// Put stores value under key.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	_, err := c.do(ctx, &protocol.Request{Op: protocol.OpPut, Key: key, Value: value})
	return err
}

// Delete removes key. Deleting a missing key succeeds.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.do(ctx, &protocol.Request{Op: protocol.OpDelete, Key: key})
	return err
}

// Ping returns the raw health document.
func (c *Client) Ping(ctx context.Context) (json.RawMessage, error) {
	resp, err := c.do(ctx, &protocol.Request{Op: protocol.OpPing})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// Stats returns the raw stats document.
func (c *Client) Stats(ctx context.Context) (json.RawMessage, error) {
	resp, err := c.do(ctx, &protocol.Request{Op: protocol.OpStats})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// StatsInto decodes the stats document into v.
func (c *Client) StatsInto(ctx context.Context, v any) error {
	raw, err := c.Stats(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// Locate returns the raw replica-set document for key (coordinator only).
func (c *Client) Locate(ctx context.Context, key string) (json.RawMessage, error) {
	resp, err := c.do(ctx, &protocol.Request{Op: protocol.OpLocate, Key: key})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}
