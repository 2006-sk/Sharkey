// Package replication implements quorum writes, reads with replica fallback
// and read repair, and anti-entropy resynchronisation of recovered nodes.
//
// It is the part of the coordinator that turns "one logical key" into "N
// physical copies". The coordinator's router (consistent-hash ring) decides
// WHICH nodes hold a key (the replica set, primary first); this package
// decides HOW to talk to them: how many must answer, in what order, what to
// do when some are down, and how to bring a node that missed writes back up
// to date.
//
// Vocabulary used throughout this file:
//
//   - N: replication factor, the number of distinct physical nodes that hold
//     each key (default 3: the primary plus the next 2 nodes clockwise).
//   - W: write quorum, the number of replicas that must acknowledge a write
//     before the client is told "OK" (default 2).
//   - R: read quorum, the number of replicas that must answer a read
//     (default 1, meaning "ask the primary, fall back down the list").
//   - Primary: replicas[0], the first node clockwise from hash(key). With
//     R=1 it serves every read while it is healthy.
//
// Consistency model (see README for the full discussion):
//
//   - Every write gets a version from the coordinator's monotonic clock, and
//     nodes apply last-writer-wins by version. Replicas therefore converge to
//     the same value for a key regardless of message ordering or retries.
//     (LWW = "last-writer-wins": a node keeps whichever copy has the highest
//     version and silently ignores older ones. Because applying the same
//     versioned write twice is a no-op, retries and resync replays are
//     idempotent.)
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
//
// Why R + W > N guarantees overlap (pigeonhole argument): a successful write
// touched W of the N nodes and a read asks R of them. If R + W > N, the two
// sets cannot be disjoint, so at least one node in the read set holds the
// acknowledged write, and picking the highest version among the answers
// finds it. With the defaults W=2, R=1, N=3 we have R + W = 3 = N, so there
// is no guaranteed overlap; the "primary is always in the write quorum" rule
// (see Write) recovers read-your-writes for the common case where the primary
// is healthy. Setting R=2 closes the gap even after a primary failure.
//
// Deletes are writes too: a DELETE stores a versioned tombstone ("this key
// was deleted at version v") rather than removing the entry. Without it, a
// replica that missed the delete, or a delayed older PUT, could bring the key
// back to life, because "no entry" carries no version to compare against.
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
//
// All four values come from coordinator flags (e.g. --write-quorum,
// --read-quorum). N itself is used by the coordinator when it asks the ring
// for a replica set; this package works with whatever replica list it is
// handed, which may be shorter than N in a cluster with fewer than N nodes.
//
// Fields:
//
//   - N is the replication factor: how many distinct nodes hold each key.
//   - W is the write quorum: acks required before a write reports success.
//     It is clamped to the replica count by quorum() below.
//   - R is the read quorum. R <= 1 selects the cheap primary-first path
//     (readOne); R >= 2 selects the parallel highest-version path
//     (readQuorum) with read repair.
//   - RequestTimeout bounds every single request to a single replica. It is
//     what guarantees nothing ever waits forever on a dead or hung node, and
//     it is also the upper bound on how long a detached straggler write can
//     live after Write has returned.
//
// (Field comments are kept here, above the struct, because comment lines
// between the fields would break gofmt's column alignment.)
type Config struct {
	N              int           // replication factor
	W              int           // write quorum
	R              int           // read quorum (1 = primary-first read)
	RequestTimeout time.Duration // per-replica request timeout
}

// Replicator executes replicated operations against cluster members.
//
// One Replicator is shared by all coordinator request goroutines, so every
// field is either immutable after New or safe for concurrent use (the
// registry has its own locks, the clock is a CAS loop, counters are atomics).
//
// Fields:
//
//   - cfg holds N/W/R and the per-replica timeout; read-only after New.
//   - reg is the coordinator's membership and health view. It tells us which
//     members are Readable (HEALTHY) or Writable (not UNHEALTHY), hands out
//     their connection pools, and receives failure/success reports so that
//     real traffic doubles as failure detection ("passive" detection).
//   - clock issues the strictly increasing versions stamped on every write.
//     There is exactly one per coordinator; versions from two different
//     clocks would only be ordered as well as the machines' clocks agree,
//     which is why the system runs a single coordinator.
//   - log reports resync completions and background read-repair failures.
//
// Counters (exported through Stats() and the coordinator's /stats endpoint;
// atomics, so request goroutines bump them without taking any lock):
//
//   - readFallbacks: reads (R=1 path) not served by the first replica in
//     preference order, because it was not readable or failed.
//   - readRepairs: repair writes started by readQuorum for replicas that
//     answered with an older version than the winner.
//   - quorumFailures: writes that returned an error because fewer than W
//     replicas acknowledged.
//   - skippedWrites: individual replica writes never attempted because the
//     member was already UNHEALTHY; resync repairs those later.
//   - resyncs: resyncs that ran to completion.
//   - resyncedKeys: entries pushed to targets across all resyncs.
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
//
// The replicator owns a fresh version clock. Because the coordinator creates
// exactly one Replicator, this is the single source of write versions for
// the whole cluster.
func New(cfg Config, reg *cluster.Registry, logger *log.Logger) *Replicator {
	return &Replicator{cfg: cfg, reg: reg, clock: clock.New(), log: logger}
}

// Stats are replication counters.
//
// The JSON names are what shows up under the replication section of the
// coordinator's /stats document; each field mirrors one atomic counter on
// Replicator:
//
//   - ReadFallbacks: reads served by a replica other than the first in
//     preference order (usually "the primary was down or failed").
//   - ReadRepairs: stale replicas repaired during quorum reads.
//   - WriteQuorumFailures: writes that failed to collect W acks.
//   - SkippedWrites: replica writes not sent because the node was UNHEALTHY.
//   - Resyncs: completed anti-entropy resyncs.
//   - ResyncedKeys: total entries pushed by resyncs.
type Stats struct {
	ReadFallbacks       int64 `json:"read_fallbacks"`
	ReadRepairs         int64 `json:"read_repairs"`
	WriteQuorumFailures int64 `json:"write_quorum_failures"`
	SkippedWrites       int64 `json:"replica_writes_skipped_unhealthy"`
	Resyncs             int64 `json:"resyncs_completed"`
	ResyncedKeys        int64 `json:"resync_entries_pushed"`
}

// Stats returns a snapshot of the counters.
//
// Each Load is atomic on its own, but the snapshot as a whole is not taken
// at a single instant; for monitoring counters that is fine and it means
// /stats never blocks request traffic.
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
//
// Every replica RPC in this package goes through send, which gives three
// guarantees in one place:
//
//  1. Bounded waiting: the request can never take longer than
//     RequestTimeout, even if the caller's ctx has no deadline.
//  2. Passive failure detection: transport-level errors (refused
//     connection, reset, timeout) count toward the node's consecutive
//     failure threshold, exactly like a failed health-check ping. This is
//     why a crash under load is noticed in tens of milliseconds instead of
//     waiting for the next health-check round. The cost is possible false
//     positives: a node that is merely overloaded and times out a few
//     requests in a row can be marked UNHEALTHY (README, "Tradeoffs").
//  3. Uniform error shape: callers get either a usable response (OK or
//     NOT_FOUND) or an error, never an application error status to decode.
func (r *Replicator) send(ctx context.Context, m *cluster.Member, req *protocol.Request) (*protocol.Response, error) {
	// Derive a child context that expires after RequestTimeout. If the
	// parent already has an earlier deadline (or is cancelled) that wins.
	ctx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
	// Always release the timer, whether the request succeeded or not;
	// forgetting this leaks a timer per request until it fires.
	defer cancel()
	// Pool.Do sends the frame over a multiplexed connection to the node and
	// waits for the matching response (or ctx expiry).
	resp, err := m.Pool.Do(ctx, req)
	if err != nil {
		// Only transport errors say anything about the node's health. An
		// application error (e.g. bad request) proves the node is alive and
		// answering, so it must not push it toward UNHEALTHY.
		if transport.IsTransportError(err) {
			r.reg.ReportFailure(m.Addr, err)
		}
		return nil, err
	}
	// Any round trip that completed resets the consecutive-failure counter,
	// so sporadic isolated errors never add up to "3 in a row".
	r.reg.ReportRequestSuccess(m.Addr)
	// NOT_FOUND is a legitimate answer (the key is absent or tombstoned on
	// that replica), not a failure; anything else non-OK is turned into an
	// error prefixed with the node address for diagnosable messages.
	if resp.Status != protocol.StatusOK && resp.Status != protocol.StatusNotFound {
		return nil, fmt.Errorf("%s: %w", m.Addr, resp.Err())
	}
	return resp, nil
}

// quorum clamps a configured quorum size to what the replica set can offer.
//
// A 2-node cluster has only 2 replicas per key, so W=3 must become 2 or every
// write would fail; and a quorum of 0 would make "success" meaningless, so
// the floor is 1. Examples: quorum(2, 3) = 2, quorum(3, 2) = 2,
// quorum(0, 3) = 1.
func quorum(want, replicas int) int {
	return max(1, min(want, replicas))
}

// result is what each fan-out goroutine sends back over the results channel:
// which replica answered (addr, so Write can recognise the primary's answer
// and readQuorum knows whom to repair), and either the node's response
// (resp) or the transport/application error (err). Exactly one of resp and
// err is meaningful.
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
//
// Fan-out timeline for N=3, W=2, all replicas healthy:
//
//	t0  Write stamps version v, starts 3 goroutines, then blocks on results
//	      |--- goroutine -> primary   (replicas[0]) --------ack---->|
//	      |--- goroutine -> replica 1 (replicas[1]) --ack-->|       |
//	      |--- goroutine -> replica 2 (replicas[2]) -------------------ack--> (late)
//	t1  replica 1 acks                       acks=1, primary still pending
//	t2  primary acks                         acks=2 >= W and primary answered
//	    Write returns OK to the client       replica 2's goroutine keeps going,
//	                                         bounded by RequestTimeout, and drops
//	                                         its result into the buffered channel
//
// Note that at t1 we would already have W=2 acks if replica 2 had been fast
// too, but Write still waits for the primary. That extra wait is the price of
// read-your-writes with R=1 (explained at the wait loop below).
//
// If a replica is UNHEALTHY it is skipped up front (never contacted), and it
// catches up later through Resync when it rejoins. A write can therefore
// succeed with 2 of 3 replicas while the third is down: availability for
// writes survives N-W failures.
func (r *Replicator) Write(ctx context.Context, replicas []string, op protocol.Op, key string, value []byte) *protocol.Response {
	// An empty ring means there is nowhere to store anything.
	if len(replicas) == 0 {
		return protocol.ErrorResponse(protocol.StatusUnavailable, "no storage nodes in ring")
	}
	// One version for this logical write, shared by every replica. That is
	// what lets all copies agree under last-writer-wins: they all compare
	// the same number, whatever order the messages arrive in.
	version := r.clock.Next()
	// need = W clamped to the number of replicas actually in the set.
	need := quorum(r.cfg.W, len(replicas))

	// The channel is buffered with one slot per replica. This is essential:
	// Write may return as soon as it has its quorum, and nobody will ever
	// read the remaining results. With an unbuffered channel those
	// straggler goroutines would block forever on the send and leak. With
	// the buffer, every goroutine's send succeeds immediately and it exits;
	// the channel is then garbage-collected.
	results := make(chan result, len(replicas))
	// failures collects human-readable reasons for the error message if the
	// quorum is not met (skipped replicas and failed requests alike).
	var failures []string
	// sent counts goroutines actually started, i.e. how many results will
	// eventually arrive on the channel. Skipped replicas are not counted.
	sent := 0
	// primaryPending is true while we still owe the primary's answer; see
	// the loop below for why the primary is part of every write quorum.
	// It starts false and is only set if the primary is actually contacted,
	// so a down primary never makes us wait for it.
	primaryPending := false
	for i, addr := range replicas {
		// Look up the member; a node the registry does not know, or one
		// currently UNHEALTHY, is not Writable. (SYNCING nodes ARE writable:
		// they must receive live writes while resync copies older data.)
		m, ok := r.reg.Get(addr)
		if !ok || !m.Writable() {
			// Don't wait on a node we already believe is down. It will
			// receive this write via resync when it comes back.
			// Sending anyway would cost up to RequestTimeout of waiting on
			// a node that is very likely dead.
			r.skippedWrites.Add(1)
			failures = append(failures, addr+": unhealthy (skipped)")
			continue
		}
		// Each goroutine gets its own Request value, since Flags differs
		// between the primary and the replicas and requests must not be
		// shared across concurrent sends.
		req := &protocol.Request{Op: op, Key: key, Value: value, Version: version}
		if i > 0 {
			// Non-primary copies are tagged so nodes can count them as
			// replication traffic in their stats; the write semantics
			// (LWW apply) are identical.
			req.Flags = protocol.FlagReplica
		} else {
			// We are contacting the primary, so its answer is now owed.
			primaryPending = true
		}
		sent++
		// Go 1.22+ loop semantics: m and req are fresh variables on every
		// iteration, so each goroutine captures its own replica.
		go func() {
			// Detached from the client's ctx so stragglers still complete
			// after we return; bounded by the per-replica timeout.
			//
			// Why detach? If the client disconnects or its ctx is cancelled
			// right after we reply OK, cancelling the in-flight write to the
			// third replica would needlessly leave it stale (to be fixed
			// only by a later resync). context.WithoutCancel keeps the
			// parent's values but drops its cancellation and deadline, and
			// send() then re-imposes RequestTimeout, so the goroutine is
			// still guaranteed to finish.
			resp, err := r.send(context.WithoutCancel(ctx), m, req)
			// Never blocks, thanks to the buffered channel (see above).
			results <- result{addr: m.Addr, resp: resp, err: err}
		}()
	}

	// Wait for W acks AND for the primary's answer (when it is up). Reads go
	// to the primary, so if the quorum could be formed by two non-primary
	// replicas, a client could write, get an ack, and immediately read a
	// stale value from the primary. Including the primary makes
	// read-your-writes hold whenever the primary is healthy. If the primary
	// fails, we don't wait on it: the quorum of other replicas suffices.
	//
	// History: the first version of this code waited only for "any W acks",
	// and TestConcurrentClients caught read-your-writes failures under load:
	//
	//	client: PUT k=v2 ... replicas 1 and 2 ack first -> "OK"
	//	client: GET k    ... goes to the primary, whose PUT is still in
	//	                     flight -> returns the old v1
	//
	// Cost of the fix: a hung (not crashed) primary stalls writes for up to
	// RequestTimeout, because we keep waiting for its answer.
	//
	// Loop condition: at most `sent` results can ever arrive, and we keep
	// reading while we either lack acks or still owe the primary's answer.
	acks := 0
	for i := 0; i < sent && (acks < need || primaryPending); i++ {
		// Stop early if the remaining replicas can't make up the quorum.
		// (sent - i) results are still outstanding; even if all of them
		// were acks we could not reach `need`, so fail now instead of
		// waiting up to RequestTimeout for answers that cannot help.
		if acks+(sent-i) < need {
			break
		}
		// Block until the next replica answers (in completion order, not
		// replica order). Each send is bounded by RequestTimeout, so this
		// receive is bounded too.
		res := <-results
		// Whatever the primary said, success or failure, we no longer owe
		// its answer. If it failed, the quorum can still be formed by the
		// other replicas.
		if res.addr == replicas[0] {
			primaryPending = false
		}
		if res.err != nil {
			failures = append(failures, res.err.Error())
			continue
		}
		// A successful response is an ack. Note that a node also acks a
		// write it ignored as stale (it already holds a newer version),
		// which is correct: that replica already reflects a later state.
		acks++
	}
	if acks < need {
		// Not rolled back: replicas that did ack keep the value, so this
		// "failed" write may still become visible later (README, "Failed
		// writes"). The client only learns that durability to W copies was
		// not confirmed.
		r.quorumFailures.Add(1)
		return protocol.ErrorResponse(protocol.StatusUnavailable,
			"write quorum not met: %d/%d acks (N=%d): %s", acks, need, len(replicas), strings.Join(failures, "; "))
	}
	// Return the version so callers (and tests) can see how this write was
	// ordered relative to others.
	return &protocol.Response{Status: protocol.StatusOK, Version: version}
}

// Read returns the value for key from the replica set.
//
// It dispatches on the configured read quorum:
//
//   - R <= 1: readOne, sequential primary-first reads with fallback. One RPC
//     in the common case; cheapest, but may return stale data after a
//     primary failure (R + W = N with the defaults, so no overlap guarantee).
//   - R >= 2: readQuorum, parallel reads, highest version wins, stale
//     replicas repaired. About R times the read cost, but with W=2 it gives
//     R + W > N, so every acknowledged write is observed.
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
//
// This is "replica fallback": reads stay available as long as any one of the
// N replicas is healthy (they survive N-1 failures). The catch is freshness:
// with W=2, the fallback replica may be exactly the one that missed the
// latest write (e.g. it was the straggler, or was skipped while down), so a
// read after a primary failure can return an older value.
//
// "Readable" means HEALTHY only. SYNCING nodes are skipped because they are
// still being filled by resync and may be missing data; returning their
// NOT_FOUND would be wrong, not just stale.
func (r *Replicator) readOne(ctx context.Context, replicas []string, key string) *protocol.Response {
	// errs accumulates why each replica could not answer, for the final
	// error message.
	var errs []string
	for i, addr := range replicas {
		m, ok := r.reg.Get(addr)
		if !ok || !m.Readable() {
			// Skip without contacting it: UNHEALTHY nodes would likely time
			// out, SYNCING nodes may give a wrong answer.
			errs = append(errs, addr+": not readable")
			continue
		}
		// We are about to ask someone other than the first choice, either
		// because this is not the primary (i > 0) or because an earlier
		// replica was skipped or failed. Count it as a fallback.
		if i > 0 || len(errs) > 0 {
			r.readFallbacks.Add(1)
		}
		// Sequential, not parallel: in the common case the primary answers
		// and we have spent exactly one RPC.
		resp, err := r.send(ctx, m, &protocol.Request{Op: protocol.OpGet, Key: key})
		if err != nil {
			errs = append(errs, err.Error())
			// If the client's own context is done (cancelled or past its
			// deadline), trying further replicas is pointless: every send
			// would fail immediately with the same context error.
			if ctx.Err() != nil {
				break
			}
			// Otherwise the failure was this replica's; try the next one.
			continue
		}
		// First successful answer wins, including NOT_FOUND.
		return resp
	}
	return protocol.ErrorResponse(protocol.StatusUnavailable, "no replica could serve read: %s", strings.Join(errs, "; "))
}

// readQuorum queries every readable replica in parallel, waits for R
// answers, returns the one with the highest version (a tombstone counts as a
// version, so a newer delete beats an older value), and repairs replicas that
// returned older versions.
//
// Flow for N=3, R=2:
//
//	fan out GET to all readable replicas (in parallel)
//	      |
//	take the first 2 successful replies  (e.g. A: v7 "new", B: v5 "old")
//	      |
//	best = highest version -> A's v7
//	      |
//	B answered v5 < v7 -> repair(B): async PUT of v7 with FlagSync
//	      |
//	return v7 to the client
//
// Read repair is opportunistic anti-entropy: data that is actually read heals
// itself, while cold data is fixed only by a full Resync. Only the replicas
// whose answers we collected are compared; a third replica that answered
// late is neither compared nor repaired.
func (r *Replicator) readQuorum(ctx context.Context, replicas []string, key string) *protocol.Response {
	// need = R clamped to the replica count.
	need := quorum(r.cfg.R, len(replicas))
	// Buffered for the same reason as in Write: once we have R replies we
	// stop receiving, and late goroutines must still be able to send and
	// exit instead of leaking.
	results := make(chan result, len(replicas))
	sent := 0
	for _, addr := range replicas {
		m, ok := r.reg.Get(addr)
		if !ok || !m.Readable() {
			// Only HEALTHY replicas are asked. If fewer than R are healthy,
			// the quorum below fails rather than silently weakening.
			continue
		}
		sent++
		go func() {
			// Unlike Write, this uses the caller's ctx directly: if the
			// client gives up there is no reason to finish a read, and
			// send() still bounds each request by RequestTimeout.
			resp, err := r.send(ctx, m, &protocol.Request{Op: protocol.OpGet, Key: key})
			results <- result{addr: m.Addr, resp: resp, err: err}
		}()
	}
	// got holds successful replies (OK or NOT_FOUND), errs the failures.
	var got []result
	var errs []string
	// Receive until we have R successes or every sent request has answered.
	// Responses arrive in completion order, so we return as soon as the
	// fastest R replicas agree to answer, not the slowest.
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
	// Pick the reply with the highest version: that is last-writer-wins
	// applied at read time. A tombstone comes back as NOT_FOUND carrying
	// its delete version, so a newer delete correctly beats an older value.
	// A key that never existed on a replica comes back as NOT_FOUND with
	// version 0, which loses to any real write.
	best := got[0].resp
	for _, g := range got[1:] {
		if g.resp.Version > best.Version {
			best = g.resp
		}
	}
	// Every collected replica that is behind the winner gets the winning
	// version pushed to it in the background (read repair). This does not
	// delay the reply to the client.
	for _, g := range got {
		if g.resp.Version < best.Version {
			r.repair(g.addr, key, best)
		}
	}
	return best
}

// repair asynchronously pushes the winning version to a stale replica.
//
// The repair is an ordinary versioned write, so it is safe even if a newer
// write lands on that replica in the meantime: last-writer-wins means the
// node keeps the newer one and ignores the repair. That is why read repair
// needs no locking or coordination with live traffic.
func (r *Replicator) repair(addr, key string, best *protocol.Response) {
	m, ok := r.reg.Get(addr)
	if !ok {
		return
	}
	// Re-send the winner with its ORIGINAL version (not a new one from the
	// clock). A new version would make this copy look newer than the real
	// write and could let the repair overwrite a later concurrent write.
	// FlagSync makes nodes count it as sync traffic in their stats.
	req := &protocol.Request{Op: protocol.OpPut, Key: key, Value: best.Value, Version: best.Version, Flags: protocol.FlagSync}
	if best.Status == protocol.StatusNotFound {
		// The winner was a tombstone: propagate the delete, at the delete's
		// version, rather than writing an empty value.
		req.Op, req.Value = protocol.OpDelete, nil
	}
	r.readRepairs.Add(1)
	go func() {
		// context.Background: the repair must not be cancelled when the
		// client's read returns; send() bounds it with RequestTimeout.
		// Failures are only logged; the replica stays stale until the next
		// quorum read or resync touches this key.
		if _, err := r.send(context.Background(), m, req); err != nil {
			r.log.Printf("read repair of %q on %s failed: %v", key, addr, err)
		}
	}()
}

// ErrSyncSuperseded means the member failed or restarted again mid-resync.
//
// Each resync request carries a sync generation number from the registry.
// If the target fails again, or restarts (new boot ID) while we are copying,
// the registry moves on to a newer generation or out of SYNCING. The old
// resync must then stop rather than finish and mark the node HEALTHY on the
// basis of a copy made into a process that no longer exists.
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
//
// Resync flow (target T is SYNCING: it receives live writes, serves no reads):
//
//	for each other HEALTHY member S (sorted by address):
//	    cursor = ""
//	    loop:
//	        still wanted? (same gen, T still SYNCING) -- no --> ErrSyncSuperseded
//	        S <- SCAN(cursor, 256 KB)       one page of S's entries, incl. tombstones
//	        keep entries whose replica set contains T
//	        T <- APPLY_BATCH(kept entries)   one RPC per page, LWW per entry
//	        cursor = page.NextCursor; empty cursor -> next source
//	all sources done -> return nil; the caller then asks the registry to
//	                    promote T to HEALTHY (MarkSynced, checked against gen)
//
// Why every source and not just T's ring neighbours? Any node might hold keys
// T replicates (e.g. after membership changes), and scanning all of them is
// simple and obviously complete. Keys are pushed once per source that holds
// them, so a key held by 2 other replicas is sent twice; LWW makes the
// duplicate harmless.
//
// This is anti-entropy, not consensus: it only makes T's copy converge with
// what its peers hold. Writes that no healthy peer holds cannot be recovered.
func (r *Replicator) Resync(ctx context.Context, target string, gen int, replicasFor func(key string) []string) error {
	tm, ok := r.reg.Get(target)
	if !ok {
		return fmt.Errorf("resync: unknown node %s", target)
	}
	start := time.Now()
	// pushed counts entries sent to the target in this resync only (the
	// resyncedKeys field is the lifetime total).
	var pushed atomic.Int64
	for _, src := range r.reg.Members() {
		// Never copy from the target to itself, and never from a node that
		// is not HEALTHY: a SYNCING peer may itself be incomplete, and an
		// UNHEALTHY one would just time out.
		if src.Addr == target || !src.Readable() {
			continue
		}
		// Sources are processed one at a time; any error (including being
		// superseded) aborts the whole resync so the node is not promoted
		// with a partial copy. The caller decides whether to retry.
		if err := r.resyncFrom(ctx, src, tm, gen, replicasFor, &pushed); err != nil {
			return err
		}
	}
	r.resyncs.Add(1)
	r.log.Printf("resync of %s complete: %d entries pushed in %v", target, pushed.Load(), time.Since(start).Round(time.Millisecond))
	return nil
}

// resyncFrom streams one source's data to target, page by page.
//
// Paging keeps memory and frame sizes bounded: each SCAN returns at most
// about 256 KB of entries plus an opaque cursor to resume from, so resync of
// any amount of data never needs one giant message. Batching the relevant
// entries of a page into one APPLY_BATCH means one round trip per page
// instead of one per key.
func (r *Replicator) resyncFrom(ctx context.Context, src, target *cluster.Member, gen int,
	replicasFor func(string) []string, pushed *atomic.Int64) error {
	// A nil cursor means "start from the beginning" of the source's data.
	var cursor []byte
	// The requested page size travels as a 4-byte big-endian integer in the
	// SCAN request's Value (256<<10 = 256 KB). The node may lower it to fit
	// its frame-size limit.
	pageSize := binary.BigEndian.AppendUint32(nil, 256<<10)
	for {
		// Check before every page, so a superseded resync stops within one
		// page of work instead of copying everything first. This is the
		// sync-generation check: gen must still be the target's latest
		// generation and the target must still be SYNCING.
		if !r.reg.SyncStillWanted(target.Addr, gen) {
			return ErrSyncSuperseded
		}
		// Fetch the next page from the source, resuming after the cursor.
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
		// replicasFor asks the CURRENT ring for the key's replica set, so a
		// newly added node receives exactly the keys it now owns a copy of.
		// Entries keep their original versions and tombstone flags.
		batch := &protocol.ScanPage{}
		for _, e := range page.Entries {
			if containsAddr(replicasFor(e.Key), target.Addr) {
				batch.Entries = append(batch.Entries, e)
			}
		}
		// Skip the RPC entirely when nothing on this page belongs to target.
		if len(batch.Entries) > 0 {
			// The target applies each entry with last-writer-wins, exactly
			// as if it were a separate versioned PUT/DELETE, so a live write
			// that already reached the target with a newer version is kept.
			req := &protocol.Request{Op: protocol.OpApplyBatch, Flags: protocol.FlagSync, Value: protocol.EncodeScanPage(batch)}
			if _, err := r.send(ctx, target, req); err != nil {
				return fmt.Errorf("resync %s: apply batch: %w", target.Addr, err)
			}
			pushed.Add(int64(len(batch.Entries)))
			r.resyncedKeys.Add(int64(len(batch.Entries)))
		}
		// An empty NextCursor means the source has no more pages.
		if len(page.NextCursor) == 0 {
			return nil
		}
		cursor = page.NextCursor
	}
}

// containsAddr reports whether v appears in s. Replica sets have at most N
// entries (3 by default), so a linear scan beats building a map.
func containsAddr(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
