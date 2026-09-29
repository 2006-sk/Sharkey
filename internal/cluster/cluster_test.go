package cluster

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"distkv/internal/node"
	"distkv/internal/transport"
)

var quiet = log.New(io.Discard, "", 0)

func newPool(addr string) *transport.Pool { return transport.NewPool(addr, 200*time.Millisecond, 4, 0) }

type syncRecorder struct {
	mu    sync.Mutex
	calls []int
	ch    chan int
}

func (s *syncRecorder) fn(addr string, gen int) {
	s.mu.Lock()
	s.calls = append(s.calls, gen)
	s.mu.Unlock()
	s.ch <- gen
}

func TestStateMachine(t *testing.T) {
	rec := &syncRecorder{ch: make(chan int, 10)}
	r := NewRegistry(3, newPool, rec.fn, quiet)
	r.Add("a", Healthy)
	m, _ := r.Get("a")

	r.ReportHealthy("a", "boot-1")
	errBoom := errors.New("boom")
	r.ReportFailure("a", errBoom)
	r.ReportFailure("a", errBoom)
	if m.State() != Healthy {
		t.Fatal("marked unhealthy before threshold")
	}
	// A successful request resets the failure streak.
	r.ReportRequestSuccess("a")
	r.ReportFailure("a", errBoom)
	r.ReportFailure("a", errBoom)
	if m.State() != Healthy {
		t.Fatal("failure streak was not reset by success")
	}
	r.ReportFailure("a", errBoom)
	if m.State() != Unhealthy {
		t.Fatalf("state = %v after 3 consecutive failures", m.State())
	}
	// Request successes can't revive an unhealthy node.
	r.ReportRequestSuccess("a")
	if m.State() != Unhealthy {
		t.Fatal("request success revived unhealthy node")
	}
	// A health check does, via Syncing.
	r.ReportHealthy("a", "boot-1")
	if m.State() != Syncing || m.Readable() || !m.Writable() {
		t.Fatalf("state = %v, want SYNCING (writable, not readable)", m.State())
	}
	gen := <-rec.ch
	if !r.MarkSynced("a", gen) || m.State() != Healthy {
		t.Fatalf("MarkSynced failed, state %v", m.State())
	}
}

func TestBootIDChangeTriggersSync(t *testing.T) {
	rec := &syncRecorder{ch: make(chan int, 10)}
	r := NewRegistry(3, newPool, rec.fn, quiet)
	r.Add("a", Healthy)
	m, _ := r.Get("a")
	r.ReportHealthy("a", "boot-1")
	r.ReportHealthy("a", "boot-1")
	if m.State() != Healthy {
		t.Fatal("same boot ID changed state")
	}
	r.ReportHealthy("a", "boot-2") // restarted without missing a check
	if m.State() != Syncing {
		t.Fatalf("state = %v, want SYNCING after restart", m.State())
	}
	<-rec.ch
}

func TestStaleSyncGenerationIgnored(t *testing.T) {
	rec := &syncRecorder{ch: make(chan int, 10)}
	r := NewRegistry(1, newPool, rec.fn, quiet)
	r.Add("a", Healthy)
	r.ReportHealthy("a", "b1")
	r.ReportFailure("a", errors.New("x"))
	r.ReportHealthy("a", "b1")
	gen1 := <-rec.ch
	// Fails and recovers again while the first sync is still running.
	r.ReportFailure("a", errors.New("x"))
	r.ReportHealthy("a", "b1")
	gen2 := <-rec.ch
	if r.MarkSynced("a", gen1) {
		t.Fatal("superseded sync promoted the node")
	}
	if r.SyncStillWanted("a", gen1) || !r.SyncStillWanted("a", gen2) {
		t.Fatal("SyncStillWanted wrong")
	}
	if !r.MarkSynced("a", gen2) {
		t.Fatal("current sync did not promote")
	}
}

func startNode(t *testing.T, addr string) (*node.Node, string) {
	t.Helper()
	cfg := node.DefaultConfig()
	cfg.Logger = quiet
	n := node.New(cfg)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	go n.Serve(ln)
	return n, ln.Addr().String()
}

func TestHealthCheckerDetectsFailureAndRecovery(t *testing.T) {
	n, addr := startNode(t, "127.0.0.1:0")
	rec := &syncRecorder{ch: make(chan int, 10)}
	r := NewRegistry(2, newPool, rec.fn, quiet)
	defer r.Close()
	r.Add(addr, Healthy)
	hc := NewHealthChecker(r, time.Hour, 200*time.Millisecond)
	ctx := context.Background()
	m, _ := r.Get(addr)

	hc.CheckAll(ctx)
	if m.State() != Healthy {
		t.Fatalf("state %v", m.State())
	}
	firstBoot := n.BootID()
	n.Shutdown(ctx)

	hc.CheckAll(ctx)
	if m.State() != Healthy {
		t.Fatal("unhealthy after one failure with threshold 2")
	}
	hc.CheckAll(ctx)
	if m.State() != Unhealthy {
		t.Fatalf("state %v, want UNHEALTHY", m.State())
	}

	n2, _ := startNode(t, addr) // restart on the same address
	defer n2.Shutdown(ctx)
	if n2.BootID() == firstBoot {
		t.Fatal("boot IDs should differ")
	}
	hc.CheckAll(ctx)
	if m.State() != Syncing {
		t.Fatalf("state %v, want SYNCING", m.State())
	}
	gen := <-rec.ch
	r.MarkSynced(addr, gen)
	if c := r.Counts(); c[Healthy] != 1 {
		t.Fatalf("counts %v", c)
	}
}

func TestHealthCheckTimesOutOnHungNode(t *testing.T) {
	// Accepts connections but never replies, like a SIGSTOPped process.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	r := NewRegistry(1, newPool, nil, quiet)
	defer r.Close()
	r.Add(ln.Addr().String(), Healthy)
	hc := NewHealthChecker(r, time.Hour, 100*time.Millisecond)
	start := time.Now()
	hc.CheckAll(context.Background())
	if time.Since(start) > time.Second {
		t.Fatalf("health round took %v", time.Since(start))
	}
	m, _ := r.Get(ln.Addr().String())
	if m.State() != Unhealthy {
		t.Fatalf("state %v", m.State())
	}
}
