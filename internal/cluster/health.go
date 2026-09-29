package cluster

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
type HealthChecker struct {
	reg      *Registry
	interval time.Duration
	timeout  time.Duration
}

// NewHealthChecker creates a checker pinging every interval, giving each ping
// timeout to complete.
func NewHealthChecker(reg *Registry, interval, timeout time.Duration) *HealthChecker {
	return &HealthChecker{reg: reg, interval: interval, timeout: timeout}
}

// Run checks all members every interval until ctx is cancelled.
func (h *HealthChecker) Run(ctx context.Context) {
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.CheckAll(ctx)
		}
	}
}

// CheckAll pings every member concurrently and waits for the results. A hung
// node therefore delays a round by at most timeout, and never delays checks
// of other nodes.
func (h *HealthChecker) CheckAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, m := range h.reg.Members() {
		wg.Add(1)
		go func(m *Member) {
			defer wg.Done()
			bootID, err := h.ping(ctx, m)
			if err != nil {
				h.reg.ReportFailure(m.Addr, err)
				return
			}
			h.reg.ReportHealthy(m.Addr, bootID)
		}(m)
	}
	wg.Wait()
}

func (h *HealthChecker) ping(ctx context.Context, m *Member) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	resp, err := m.Pool.Do(ctx, &protocol.Request{Op: protocol.OpPing})
	if err != nil {
		return "", err
	}
	if resp.Status != protocol.StatusOK {
		return "", fmt.Errorf("ping: %w", resp.Err())
	}
	var doc struct {
		BootID string `json:"boot_id"`
	}
	if err := json.Unmarshal(resp.Value, &doc); err != nil || doc.BootID == "" {
		return "", fmt.Errorf("ping: bad health document: %q", resp.Value)
	}
	return doc.BootID, nil
}
