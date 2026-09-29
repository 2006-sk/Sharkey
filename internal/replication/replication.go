// Package replication implements quorum writes, reads with replica fallback
// and read repair, and anti-entropy resynchronisation of recovered nodes.
//
// Consistency model (see README for the full discussion):
//
//   - Every write gets a version from the coordinator's monotonic clock, and
//     nodes apply last-writer-wins by version. Replicas therefore converge to
//     the same value for a key regardless of message ordering or retries.
//   - A write succeeds once W of the N replicas acknowledge it, one of which
//     must be the primary unless the primary is down. It is not
//     rolled back if fewer than W acknowledge: the client sees an error but
//     the value may still exist on some replicas ("failed" writes may become
//     visible later — standard for Dynamo-style quorums without transactions).
//   - Reads with R=1 (default) go to the first readable replica in preference
//     order (normally the primary). If R + W > N, a read quorum overlaps every
//     successful write quorum, so it observes the latest acknowledged write —
//     but this is NOT linearizability (no ordering across concurrent
//     in-flight operations, no consensus).
package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"distkv/internal/clock"
	"distkv/internal/cluster"
	"distkv/internal/protocol"
	"distkv/internal/transport"
)

// Config configures the replicator.
type Config struct {
	N              int           // replication factor
	W              int           // write quorum
	R              int           // read quorum (1 = primary-first read)
	RequestTimeout time.Duration // per-replica request timeout
}

// Replicator executes replicated operations against cluster members.
type Replicator struct {
	cfg   Config
	reg   *cluster.Registry
	clock *clock.Clock
	log   *log.Logger

	readFallbacks  atomic.Int64
	readRepairs    atomic.Int64
	quorumFailures atomic.Int64
	skippedWrites  atomic.Int64 // replica writes not attempted: node unhealthy
	resyncs        atomic.Int64
	resyncedKeys   atomic.Int64
}

// New creates a replicator.
func New(cfg Config, reg *cluster.Registry, logger *log.Logger) *Replicator {
	return &Replicator{cfg: cfg, reg: reg, clock: clock.New(), log: logger}
}

// Stats are replication counters.
type Stats struct {
	ReadFallbacks       int64 `json:"read_fallbacks"`
	ReadRepairs         int64 `json:"read_repairs"`
	WriteQuorumFailures int64 `json:"write_quorum_failures"`
	SkippedWrites       int64 `json:"replica_writes_skipped_unhealthy"`
	Resyncs             int64 `json:"resyncs_completed"`
	ResyncedKeys        int64 `json:"resync_entries_pushed"`
}

// Stats returns a snapshot of the counters.
func (r *Replicator) Stats() Stats {
	return Stats{
		ReadFallbacks:       r.readFallbacks.Load(),
		ReadRepairs:         r.readRepairs.Load(),
		WriteQuorumFailures: r.quorumFailures.Load(),
		SkippedWrites:       r.skippedWrites.Load(),
		Resyncs:             r.resyncs.Load(),
		ResyncedKeys:        r.resyncedKeys.Load(),
	}
}

// send performs one request against a member with the per-replica timeout
// and feeds transport failures into failure detection.
func (r *Replicator) send(ctx context.Context, m *cluster.Member, req *protocol.Request) (*protocol.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
	defer cancel()
	resp, err := m.Pool.Do(ctx, req)
	if err != nil {
		if transport.IsTransportError(err) {
			r.reg.ReportFailure(m.Addr, err)
		}
		return nil, err
	}
	r.reg.ReportRequestSuccess(m.Addr)
	if resp.Status != protocol.StatusOK && resp.Status != protocol.StatusNotFound {
		return nil, fmt.Errorf("%s: %w", m.Addr, resp.Err())
	}
	return resp, nil
}

func quorum(want, replicas int) int {
	return max(1, min(want, replicas))
}

type result struct {
	addr string
	resp *protocol.Response
	err  error
}

// Write sends a PUT or DELETE to every writable replica in parallel and
// returns once W have acknowledged (or it is certain W cannot be reached).
// Replicas still in flight when Write returns finish in the background.
//
// The coordinator fans out to all replicas itself rather than chaining
// primary -> replicas: one fewer network hop on the critical path, and a
// slow primary cannot delay the other replicas.
func (r *Replicator) Write(ctx context.Context, replicas []string, op protocol.Op, key string, value []byte) *protocol.Response {
	if len(replicas) == 0 {
		return protocol.ErrorResponse(protocol.StatusUnavailable, "no storage nodes in ring")
	}
	version := r.clock.Next()
	need := quorum(r.cfg.W, len(replicas))

	results := make(chan result, len(replicas))
	var failures []string
	sent := 0
	// primaryPending is true while we still owe the primary's answer; see
	// the loop below for why the primary is part of every write quorum.
	primaryPending := false
	for i, addr := range replicas {
		m, ok := r.reg.Get(addr)
		if !ok || !m.Writable() {
			// Don't wait on a node we already believe is down. It will
			// receive this write via resync when it comes back.
			r.skippedWrites.Add(1)
			failures = append(failures, addr+": unhealthy (skipped)")
			continue
		}
		req := &protocol.Request{Op: op, Key: key, Value: value, Version: version}
		if i > 0 {
			req.Flags = protocol.FlagReplica
		} else {
			primaryPending = true
		}
		sent++
		go func() {
			// Detached from the client's ctx so stragglers still complete
			// after we return; bounded by the per-replica timeout.
			resp, err := r.send(context.WithoutCancel(ctx), m, req)
			results <- result{addr: m.Addr, resp: resp, err: err}
		}()
	}

	// Wait for W acks AND for the primary's answer (when it is up). Reads go
	// to the primary, so if the quorum could be formed by two non-primary
	// replicas, a client could write, get an ack, and immediately read a
	// stale value from the primary. Including the primary makes
	// read-your-writes hold whenever the primary is healthy. If the primary
	// fails, we don't wait on it: the quorum of other replicas suffices.
	acks := 0
	for i := 0; i < sent && (acks < need || primaryPending); i++ {
		// Stop early if the remaining replicas can't make up the quorum.
		if acks+(sent-i) < need {
			break
		}
		res := <-results
		if res.addr == replicas[0] {
			primaryPending = false
		}
		if res.err != nil {
			failures = append(failures, res.err.Error())
			continue
		}
		acks++
	}
	if acks < need {
		r.quorumFailures.Add(1)
		return protocol.ErrorResponse(protocol.StatusUnavailable,
			"write quorum not met: %d/%d acks (N=%d): %s", acks, need, len(replicas), strings.Join(failures, "; "))
	}
	return &protocol.Response{Status: protocol.StatusOK, Version: version}
}

// Read returns the value for key from the replica set.
func (r *Replicator) Read(ctx context.Context, replicas []string, key string) *protocol.Response {
	if len(replicas) == 0 {
		return protocol.ErrorResponse(protocol.StatusUnavailable, "no storage nodes in ring")
	}
	if r.cfg.R <= 1 {
		return r.readOne(ctx, replicas, key)
	}
	return r.readQuorum(ctx, replicas, key)
}

// readOne tries replicas in preference order (primary first), skipping
// members not currently readable and falling back to the next replica on any
// failure. A NOT_FOUND from a readable replica is an answer, not a failure.
func (r *Replicator) readOne(ctx context.Context, replicas []string, key string) *protocol.Response {
	var errs []string
	for i, addr := range replicas {
		m, ok := r.reg.Get(addr)
		if !ok || !m.Readable() {
			errs = append(errs, addr+": not readable")
			continue
		}
		if i > 0 || len(errs) > 0 {
			r.readFallbacks.Add(1)
		}
		resp, err := r.send(ctx, m, &protocol.Request{Op: protocol.OpGet, Key: key})
		if err != nil {
			errs = append(errs, err.Error())
			if ctx.Err() != nil {
				break
			}
			continue
		}
		return resp
	}
	return protocol.ErrorResponse(protocol.StatusUnavailable, "no replica could serve read: %s", strings.Join(errs, "; "))
}

// readQuorum queries every readable replica in parallel, waits for R
// answers, returns the one with the highest version (a tombstone counts as a
// version, so a newer delete beats an older value), and repairs replicas that
// returned older versions.
func (r *Replicator) readQuorum(ctx context.Context, replicas []string, key string) *protocol.Response {
	need := quorum(r.cfg.R, len(replicas))
	results := make(chan result, len(replicas))
	sent := 0
	for _, addr := range replicas {
		m, ok := r.reg.Get(addr)
		if !ok || !m.Readable() {
			continue
		}
		sent++
		go func() {
			resp, err := r.send(ctx, m, &protocol.Request{Op: protocol.OpGet, Key: key})
			results <- result{addr: m.Addr, resp: resp, err: err}
		}()
	}
	var got []result
	var errs []string
	for i := 0; i < sent && len(got) < need; i++ {
		res := <-results
		if res.err != nil {
			errs = append(errs, res.err.Error())
			continue
		}
		got = append(got, res)
	}
	if len(got) < need {
		return protocol.ErrorResponse(protocol.StatusUnavailable,
			"read quorum not met: %d/%d replies: %s", len(got), need, strings.Join(errs, "; "))
	}
	best := got[0].resp
	for _, g := range got[1:] {
		if g.resp.Version > best.Version {
			best = g.resp
		}
	}
	for _, g := range got {
		if g.resp.Version < best.Version {
			r.repair(g.addr, key, best)
		}
	}
	return best
}

// repair asynchronously pushes the winning version to a stale replica.
func (r *Replicator) repair(addr, key string, best *protocol.Response) {
	m, ok := r.reg.Get(addr)
	if !ok {
		return
	}
	req := &protocol.Request{Op: protocol.OpPut, Key: key, Value: best.Value, Version: best.Version, Flags: protocol.FlagSync}
	if best.Status == protocol.StatusNotFound {
		req.Op, req.Value = protocol.OpDelete, nil
	}
	r.readRepairs.Add(1)
	go func() {
		if _, err := r.send(context.Background(), m, req); err != nil {
			r.log.Printf("read repair of %q on %s failed: %v", key, addr, err)
		}
	}()
}

// ErrSyncSuperseded means the member failed or restarted again mid-resync.
var ErrSyncSuperseded = errors.New("resync superseded")

// Resync copies to target every entry, from every other readable member,
// whose replica set includes target. It is used when a node rejoins (its
// in-memory data is gone or stale) and when a node is added to the ring.
//
// It pages through each source with OpScan and forwards each page's relevant
// entries to target with one OpApplyBatch. Its cost is proportional to
// the total data in the cluster; a production system would compare Merkle
// trees per key range and transfer only differences. Because every entry
// carries its version and nodes apply last-writer-wins, pushing entries
// concurrently with live writes is safe: an older synced value can never
// overwrite a newer live write. Tombstones are copied too, so deletes that
// happened while target was down are not undone.
func (r *Replicator) Resync(ctx context.Context, target string, gen int, replicasFor func(key string) []string) error {
	tm, ok := r.reg.Get(target)
	if !ok {
		return fmt.Errorf("resync: unknown node %s", target)
	}
	start := time.Now()
	var pushed atomic.Int64
	for _, src := range r.reg.Members() {
		if src.Addr == target || !src.Readable() {
			continue
		}
		if err := r.resyncFrom(ctx, src, tm, gen, replicasFor, &pushed); err != nil {
			return err
		}
	}
	r.resyncs.Add(1)
	r.log.Printf("resync of %s complete: %d entries pushed in %v", target, pushed.Load(), time.Since(start).Round(time.Millisecond))
	return nil
}

func (r *Replicator) resyncFrom(ctx context.Context, src, target *cluster.Member, gen int,
	replicasFor func(string) []string, pushed *atomic.Int64) error {
	var cursor []byte
	pageSize := binary.BigEndian.AppendUint32(nil, 256<<10)
	for {
		if !r.reg.SyncStillWanted(target.Addr, gen) {
			return ErrSyncSuperseded
		}
		resp, err := r.send(ctx, src, &protocol.Request{Op: protocol.OpScan, Key: string(cursor), Value: pageSize})
		if err != nil {
			return fmt.Errorf("resync %s: scan %s: %w", target.Addr, src.Addr, err)
		}
		page, err := protocol.DecodeScanPage(resp.Value)
		if err != nil {
			return fmt.Errorf("resync %s: scan %s: %w", target.Addr, src.Addr, err)
		}

		// Forward the entries this target replicates as one batch: a
		// single round trip per scan page instead of one per key.
		batch := &protocol.ScanPage{}
		for _, e := range page.Entries {
			if containsAddr(replicasFor(e.Key), target.Addr) {
				batch.Entries = append(batch.Entries, e)
			}
		}
		if len(batch.Entries) > 0 {
			req := &protocol.Request{Op: protocol.OpApplyBatch, Flags: protocol.FlagSync, Value: protocol.EncodeScanPage(batch)}
			if _, err := r.send(ctx, target, req); err != nil {
				return fmt.Errorf("resync %s: apply batch: %w", target.Addr, err)
			}
			pushed.Add(int64(len(batch.Entries)))
			r.resyncedKeys.Add(int64(len(batch.Entries)))
		}
		if len(page.NextCursor) == 0 {
			return nil
		}
		cursor = page.NextCursor
	}
}

func containsAddr(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
