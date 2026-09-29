// Package testcluster starts an in-process coordinator and storage nodes on
// loopback ports for integration tests. Everything runs over real TCP.
package testcluster

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"distkv/internal/client"
	"distkv/internal/coordinator"
	"distkv/internal/node"
)

// Cluster is a running test cluster.
type Cluster struct {
	t     testing.TB
	Addrs []string // storage node addresses, stable across restarts
	Nodes []*node.Node
	Coord *coordinator.Coordinator
	Addr  string // coordinator address
	Log   *log.Logger

	nodeCfg node.Config
	mu      sync.Mutex
	hung    map[int]net.Listener
	hungCon []net.Conn
}

// Options tweak the cluster.
type Options struct {
	Nodes         int
	CacheCapacity int
	Coord         func(*coordinator.Config)
}

// Start launches opts.Nodes storage nodes and a coordinator with fast health
// checking suitable for tests.
func Start(t testing.TB, opts Options) *Cluster {
	t.Helper()
	logger := log.New(io.Discard, "", 0)
	if os.Getenv("DISTKV_TEST_LOG") != "" {
		logger = log.New(os.Stderr, "", log.Lmicroseconds)
	}
	ncfg := node.DefaultConfig()
	ncfg.CacheCapacity = opts.CacheCapacity
	ncfg.Logger = logger
	c := &Cluster{t: t, Log: logger, nodeCfg: ncfg, hung: map[int]net.Listener{}}
	for i := 0; i < opts.Nodes; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		n := node.New(ncfg)
		go n.Serve(ln)
		c.Nodes = append(c.Nodes, n)
		c.Addrs = append(c.Addrs, ln.Addr().String())
	}

	cfg := coordinator.DefaultConfig()
	cfg.Nodes = c.Addrs
	cfg.HealthInterval = 50 * time.Millisecond
	cfg.HealthTimeout = 100 * time.Millisecond
	cfg.RequestTimeout = 200 * time.Millisecond
	cfg.DialTimeout = 100 * time.Millisecond
	cfg.Logger = logger
	cfg.Server.Logger = logger
	if opts.Coord != nil {
		opts.Coord(&cfg)
	}
	coord, err := coordinator.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	coord.Start()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go coord.Serve(ln)
	c.Coord = coord
	c.Addr = ln.Addr().String()
	t.Cleanup(c.Close)
	return c
}

// Client returns a client connected to the coordinator.
func (c *Cluster) Client() *client.Client {
	o := client.DefaultOptions()
	o.RequestTimeout = 5 * time.Second
	cl := client.New(c.Addr, o)
	c.t.Cleanup(cl.Close)
	return cl
}

// NodeClient returns a client talking directly to storage node i.
func (c *Cluster) NodeClient(i int) *client.Client {
	o := client.DefaultOptions()
	o.RequestTimeout = time.Second
	cl := client.New(c.Addrs[i], o)
	c.t.Cleanup(cl.Close)
	return cl
}

// Index returns the index of the node with address addr, or -1.
func (c *Cluster) Index(addr string) int {
	for i, a := range c.Addrs {
		if a == addr {
			return i
		}
	}
	return -1
}

// Kill stops node i abruptly (closing its listener and connections), like a
// crashed process: peers see connection refused / reset.
func (c *Cluster) Kill(i int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	c.Nodes[i].Shutdown(ctx)
}

// Hang replaces node i with a listener that accepts connections but never
// replies, like a frozen (SIGSTOPped) process: peers only see timeouts.
func (c *Cluster) Hang(i int) {
	c.Kill(i)
	ln := c.listen(c.Addrs[i])
	c.mu.Lock()
	c.hung[i] = ln
	c.mu.Unlock()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			c.mu.Lock()
			c.hungCon = append(c.hungCon, conn)
			c.mu.Unlock()
		}
	}()
}

// Restart starts a fresh (empty, new boot ID) node on node i's address.
func (c *Cluster) Restart(i int) {
	c.mu.Lock()
	if ln, ok := c.hung[i]; ok {
		ln.Close()
		delete(c.hung, i)
	}
	c.mu.Unlock()
	ln := c.listen(c.Addrs[i])
	n := node.New(c.nodeCfg)
	go n.Serve(ln)
	c.Nodes[i] = n
}

// StartExtraNode starts a node on a new port without registering it with
// the coordinator, returning its address.
func (c *Cluster) StartExtraNode() string {
	ln := c.listen("127.0.0.1:0")
	n := node.New(c.nodeCfg)
	go n.Serve(ln)
	c.Nodes = append(c.Nodes, n)
	c.Addrs = append(c.Addrs, ln.Addr().String())
	return ln.Addr().String()
}

func (c *Cluster) listen(addr string) net.Listener {
	var err error
	for i := 0; i < 50; i++ {
		var ln net.Listener
		if ln, err = net.Listen("tcp", addr); err == nil {
			return ln
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("listen %s: %v", addr, err)
	return nil
}

// WaitFor polls cond until it is true or timeout elapses.
func (c *Cluster) WaitFor(timeout time.Duration, what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Close shuts everything down.
func (c *Cluster) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.Coord.Shutdown(ctx)
	for _, n := range c.Nodes {
		n.Shutdown(ctx)
	}
	c.mu.Lock()
	for _, ln := range c.hung {
		ln.Close()
	}
	for _, conn := range c.hungCon {
		conn.Close()
	}
	c.mu.Unlock()
}
