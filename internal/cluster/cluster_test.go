// Tests for the cluster package: the registry's HEALTHY/UNHEALTHY/SYNCING state
// machine, restart detection via boot IDs, resync generations, and the
// active health checker against real (and deliberately hung) TCP servers.
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

// quiet discards the registry's state-transition log lines so test output
// stays readable.
var quiet = log.New(io.Discard, "", 0)

// newPool builds a small connection pool (200 ms dial timeout, 4
// connections, default max frame size) for each registered member.
func newPool(addr string) *transport.Pool { return transport.NewPool(addr, 200*time.Millisecond, 4, 0) }

// syncRecorder stands in for the coordinator's resync loop (the SyncFunc). It
// records every generation it is asked to sync and forwards it on ch, so a
// test can wait for the sync request (which runs in its own goroutine) and
// then decide itself when, and with which generation, to call MarkSynced.
type syncRecorder struct {
	mu    sync.Mutex
	calls []int
	ch    chan int
}

// fn is the SyncFunc handed to NewRegistry.
func (s *syncRecorder) fn(addr string, gen int) {
	s.mu.Lock()
	s.calls = append(s.calls, gen)
	s.mu.Unlock()
	s.ch <- gen
}

// TestStateMachine walks one member through the whole state machine:
// HEALTHY -> (3 consecutive failures) -> UNHEALTHY -> (health check) ->
// SYNCING -> (MarkSynced) -> HEALTHY. It proves that failures must be
// CONSECUTIVE (a success resets the streak, so one blip never trips the
// detector), that a data-request success cannot revive an UNHEALTHY node
// (only a health check can, so a returning node always resyncs first), and
// that SYNCING means writable-but-not-readable. Each of these guards against
// serving reads from a node that may be missing data.
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

// TestBootIDChangeTriggersSync proves fast-restart detection: a node whose
// boot ID changes goes to SYNCING even though no health check ever failed.
// Without this, a node that restarted (and lost its in-memory data) between
// two pings would stay HEALTHY and answer "not found" for keys it should
// hold. It also checks that repeated checks with the SAME boot ID do not
// trigger needless resyncs.
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

// TestStaleSyncGenerationIgnored proves that resync generations work: when a
// node fails and recovers again while an earlier resync is still running,
// the earlier (superseded) generation can neither promote the node via
// MarkSynced nor keep running (SyncStillWanted is false for it), while the
// newest generation can. This prevents a stale resync, whose copies may have
// gone to a now-lost incarnation, from marking the node HEALTHY. A threshold
// of 1 makes each single ReportFailure mark the node UNHEALTHY.
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

// startNode runs a real storage node on addr ("127.0.0.1:0" picks a free
// port) and returns it with its actual listen address, so health checks go
// over real TCP.
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

// TestHealthCheckerDetectsFailureAndRecovery runs the active detector end to
// end against a real node: healthy while up; still HEALTHY after one failed
// round with threshold 2 (debouncing); UNHEALTHY after the second; and, once
// a NEW process listens on the same address (new boot ID), SYNCING with a
// resync requested, then HEALTHY after MarkSynced. It shows that the health
// checker, not client traffic, is what brings a node back into service.
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

// TestHealthCheckTimesOutOnHungNode proves that a node which accepts TCP
// connections but never replies (like a SIGSTOPped process: the kernel still
// completes the handshake) is detected by the ping timeout, and that a round
// finishes in about the timeout rather than hanging forever. A health checker
// without a per-ping deadline would block on such a node indefinitely and
// never mark it down.
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
			// Deferred, not immediate: accepted connections stay open and
			// silent until the accept loop exits (when ln is closed), so
			// the ping can only end by timing out.
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
