package cluster

// health.go is the ACTIVE half of failure detection: a timer-driven loop
// that pings every node, whether or not any client traffic is flowing.
//
// Active vs passive detection
// ---------------------------
//   - Passive detection (Replicator.send -> Registry.ReportFailure) learns
//     from real requests. It is fast under load, but it is blind when a node
//     gets no traffic, and it can never bring a node BACK: nothing is routed
//     to an UNHEALTHY node, so there are no successes to observe.
//   - Active detection (this file) costs one small request per node per
//     interval, but it notices failures on idle nodes, it is the ONLY path
//     from UNHEALTHY back to SYNCING, and every ping returns the node's boot
//     ID, which is how fast restarts are caught (Registry.ReportHealthy).
//
// One round (CheckAll), all nodes pinged in parallel:
//
//	t=0 ----------------------------------------------- t=timeout
//	node A  ping ->  <- ok(boot ID)         ReportHealthy(A, id)
//	node B  ping ->        <- ok(boot ID)   ReportHealthy(B, id)
//	node C  ping -> ........ (hung) ....... ctx deadline -> ReportFailure(C)
//	                                        wg.Wait() returns here at the latest
//
// With the README defaults (2 s interval, 500 ms timeout, threshold 3), an
// idle crashed node is marked UNHEALTHY after 3 failed rounds, i.e. roughly
// 3 intervals; under load passive detection usually gets there first.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"distkv/internal/protocol"
)

// HealthChecker periodically pings every member and feeds the results into
// the registry.
//
// Fields: reg is the registry whose members are pinged and which receives
// the outcomes; interval is the time between rounds (--health-interval);
// timeout bounds each individual ping, so a hung node cannot stall a round.
type HealthChecker struct {
	reg      *Registry
	interval time.Duration
	timeout  time.Duration
}

// NewHealthChecker creates a checker pinging every interval, giving each ping
// timeout to complete.
//
// The timeout should be well below the interval; otherwise a round that waits
// out a hung node's full timeout would eat into the next round.
func NewHealthChecker(reg *Registry, interval, timeout time.Duration) *HealthChecker {
	return &HealthChecker{reg: reg, interval: interval, timeout: timeout}
}

// Run checks all members every interval until ctx is cancelled.
//
// The coordinator runs this in a background goroutine. Rounds never overlap:
// CheckAll is called synchronously, and a time.Ticker drops ticks that fire
// while a round is still running rather than queueing them up. The first
// round happens one interval after start, not immediately.
func (h *HealthChecker) Run(ctx context.Context) {
	t := time.NewTicker(h.interval)
	// Stop the ticker on exit so its resources are released.
	defer t.Stop()
	for {
		select {
		// Shutdown: the coordinator cancelled its context.
		case <-ctx.Done():
			return
		// Time for another round. Passing ctx down means in-flight pings are
		// cancelled promptly on shutdown too.
		case <-t.C:
			h.CheckAll(ctx)
		}
	}
}

// CheckAll pings every member concurrently and waits for the results. A hung
// node therefore delays a round by at most timeout, and never delays checks
// of other nodes.
//
// If the pings ran one after another, k hung nodes would stretch a round to
// k*timeout and delay detection for every node behind them. Fanning out one
// goroutine per member and joining with a WaitGroup bounds a round by the
// slowest single ping (itself capped at timeout).
func (h *HealthChecker) CheckAll(ctx context.Context) {
	var wg sync.WaitGroup
	// Members returns a snapshot slice, so no registry lock is held while the
	// pings run.
	for _, m := range h.reg.Members() {
		// Add before starting the goroutine, so Wait cannot return early.
		wg.Add(1)
		// m is passed as an argument so each goroutine gets its own member
		// (a habit from before Go 1.22 made loop variables per-iteration).
		go func(m *Member) {
			defer wg.Done()
			bootID, err := h.ping(ctx, m)
			if err != nil {
				// Any ping failure counts toward the consecutive-failure
				// threshold: refused, reset, timed out, or a bad reply.
				h.reg.ReportFailure(m.Addr, err)
				return
			}
			// Success: resets the streak and, if the node was UNHEALTHY or
			// its boot ID changed, moves it to SYNCING and starts a resync.
			h.reg.ReportHealthy(m.Addr, bootID)
		}(m)
	}
	// Block until every ping has reported. Tests rely on this: after CheckAll
	// returns, the registry reflects the whole round.
	wg.Wait()
}

// ping sends one PING to m and returns the boot ID from the node's health
// document.
//
// Unlike passive detection, which only counts transport errors, ping treats
// every kind of failure as "unhealthy": a transport error, a non-OK status,
// or a reply that does not carry a boot ID. A node that cannot answer a ping
// properly should not be trusted with data.
func (h *HealthChecker) ping(ctx context.Context, m *Member) (string, error) {
	// Per-ping deadline. Derived from ctx, so shutdown also cancels it. This
	// is what turns a hung (e.g. SIGSTOPped) node into a timely failure.
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	// Always release the timer, even on the success path.
	defer cancel()
	// Pings share the member's connection pool with data requests, so a ping
	// also exercises the exact path that real traffic uses.
	resp, err := m.Pool.Do(ctx, &protocol.Request{Op: protocol.OpPing})
	if err != nil {
		return "", err
	}
	// The node answered, but with an error status.
	if resp.Status != protocol.StatusOK {
		return "", fmt.Errorf("ping: %w", resp.Err())
	}
	// The PING reply's value is a small JSON health document from the node;
	// only its boot_id field is needed here.
	var doc struct {
		BootID string `json:"boot_id"`
	}
	// A missing or empty boot ID is treated as a failure: without it restart
	// detection would silently stop working for this node.
	if err := json.Unmarshal(resp.Value, &doc); err != nil || doc.BootID == "" {
		return "", fmt.Errorf("ping: bad health document: %q", resp.Value)
	}
	return doc.BootID, nil
}
