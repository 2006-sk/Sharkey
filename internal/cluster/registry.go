// Package cluster tracks storage-node membership and health.
//
// Failure detection here is a local, per-coordinator opinion based on
// timeouts. It is NOT consensus: a node marked UNHEALTHY may be alive but
// partitioned from the coordinator, and a slow node may be falsely suspected.
//
// Big picture
// -----------
// The coordinator needs to know, for every storage node, "may I send reads
// to it?" and "may I send writes to it?". This package answers those two
// questions. It owns:
//
//   - the Registry: the member list plus a small state machine per member;
//   - the HealthChecker (health.go): an ACTIVE failure detector that pings
//     every node on a timer;
//   - the hooks for PASSIVE failure detection: the replication layer calls
//     ReportFailure / ReportRequestSuccess with the outcome of real data
//     requests, so a crash under load is noticed from failed traffic long
//     before the next ping (README "Failure handling").
//
// Failure detection vs consensus
// ------------------------------
// A failure detector only produces a *suspicion*. Over a network you cannot
// distinguish "dead" from "slow" or "partitioned from me": all three look like
// a timeout. Systems that need every participant to agree on membership use a
// consensus protocol (Raft, Paxos) or gossip with suspicion refutation. This
// project has one coordinator, so its local opinion is simply taken as truth
// for routing. The price is false positives: a briefly overloaded node can be
// marked UNHEALTHY and lose traffic until the next health check revives it.
//
// Node state machine (per member)
// -------------------------------
//
//	              failThreshold consecutive failures
//	         (failed pings OR transport errors on real requests)
//	  +---------+ ----------------------------------> +-----------+
//	  | HEALTHY |                                     | UNHEALTHY |
//	  +---------+                                     +-----------+
//	     ^    |                                          |     ^
//	     |    | health check shows a NEW boot ID         |     |
//	     |    | (restarted fast, lost its memory)        |     |
//	     |    v                                          |     |
//	     |  +---------+ <-- a health check succeeds -----+     |
//	     +--| SYNCING |                                        |
//	        +---------+ --- failThreshold consecutive ---------+
//	     resync done           failures (resync abandoned)
//	     (MarkSynced with
//	      current generation)
//
//	HEALTHY   : reads + writes       (Readable, Writable)
//	SYNCING   : writes only          (Writable, not Readable)
//	UNHEALTHY : no traffic at all    (neither)
//
// Why a SYNCING state at all? A node that was down (or restarted and lost its
// in-memory data) is missing every write it did not see. If we let it serve
// reads immediately it would answer "not found" or an old value. So it first
// receives new writes (so it stops falling further behind) while an
// anti-entropy resync copies the data it missed from its peers; only then is
// it promoted back to HEALTHY and allowed to serve reads.
package cluster

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"distkv/internal/transport"
)

// State is the coordinator's view of a node.
//
// It is an int enum (iota) rather than a string so comparisons are cheap and
// typos are compile errors; String/MarshalText give it a readable form for
// logs and the JSON admin endpoints.
type State int

const (
	// Healthy nodes receive reads and writes.
	//
	// Healthy is the zero value (iota starts at 0), so a freshly declared
	// State is Healthy.
	Healthy State = iota
	// Unhealthy nodes failed FailureThreshold consecutive checks/requests.
	// They receive no traffic until a health check succeeds again.
	//
	// Writes skip them (the replicator counts these as skipped writes) and
	// reads fall back to the next replica in the preference list.
	Unhealthy
	// Syncing nodes are back (or new) but may be missing data: they receive
	// writes, but not reads, until anti-entropy resynchronisation finishes.
	//
	// Nodes added at runtime also start here: they own key ranges they have
	// never seen, so they must be filled by resync before serving reads.
	Syncing
)

// String returns the upper-case state name used in logs and JSON
// ("HEALTHY", "UNHEALTHY", "SYNCING"). Implementing fmt.Stringer means %s and
// %v in Printf print the name instead of the raw integer.
func (s State) String() string {
	switch s {
	case Healthy:
		return "HEALTHY"
	case Unhealthy:
		return "UNHEALTHY"
	case Syncing:
		return "SYNCING"
	default:
		// Defensive: only reachable if someone converts an arbitrary int to
		// State. Returning a marker is friendlier than panicking in a log line.
		return "UNKNOWN"
	}
}

// MarshalText makes State render as its name in JSON.
//
// encoding/json uses encoding.TextMarshaler when present, so a MemberStatus
// serialises as "state":"SYNCING" rather than "state":2.
func (s State) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// UnmarshalText parses a state name.
//
// It is the inverse of MarshalText, so clients (and tests) can decode the
// coordinator's /nodes JSON back into State values. Unknown names are an
// error rather than silently becoming Healthy (the zero value).
func (s *State) UnmarshalText(b []byte) error {
	// Try every known state and match on its canonical name.
	for _, st := range []State{Healthy, Unhealthy, Syncing} {
		if st.String() == string(b) {
			*s = st
			return nil
		}
	}
	return fmt.Errorf("unknown node state %q", b)
}

// Member is one storage node as seen by the coordinator.
//
// Addr and Pool are set once at creation and never change, so they may be
// read without the lock. Everything below mu is mutable health bookkeeping and
// must only be touched while holding mu.
type Member struct {
	// Addr is the node's "host:port"; it is also the member's identity in the
	// registry map and on the consistent-hash ring. Pool is the set of
	// multiplexed TCP connections the coordinator uses to talk to this node,
	// for data requests and health pings alike.
	Addr string
	Pool *transport.Pool

	// mu guards every field below. It is per member, so updating one node's
	// health never contends with lookups or updates of another node.
	//
	//   state       current position in the HEALTHY/UNHEALTHY/SYNCING state
	//               machine (see the package comment).
	//   failures    CONSECUTIVE failures. Any success resets it, so only an
	//               unbroken streak of failThreshold failures marks the node
	//               down. This debounces a single dropped packet or slow reply.
	//   bootID      the random ID the node process generated at start-up, as
	//               last reported by a successful ping. Empty until the first
	//               successful check; a different value later = a restart.
	//   lastErr     text of the most recent failure, shown in /nodes and in the
	//               log line when the node is marked UNHEALTHY. Cleared by a
	//               successful health check.
	//   lastChange  when state last changed (for operators and /nodes).
	//   syncGen     the resync "generation", bumped on every resync request. A
	//               resync goroutine carries the generation it was started for
	//               and its result is honoured only if that generation is still
	//               current, so a slow, superseded resync (the node failed or
	//               restarted again while it ran) can never promote the node to
	//               HEALTHY on the strength of data copied before the newer
	//               failure. See requestSync for a timeline.
	//   transitions number of state changes, a cheap signal of flapping.
	mu          sync.Mutex
	state       State
	failures    int // consecutive failed checks/requests
	bootID      string
	lastErr     string
	lastChange  time.Time
	syncGen     int // incremented every time a resync is requested
	transitions int
}

// State returns the member's current state.
//
// It takes the member lock so the read is race-free with concurrent health
// updates. The answer can be stale the instant the lock is released; callers
// treat it as a routing hint, which is all a failure detector can offer.
func (m *Member) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Readable reports whether reads may be served by this member.
//
// Only HEALTHY nodes serve reads: a SYNCING node may be missing data and would
// return stale or "not found" answers, and an UNHEALTHY node is presumed dead.
func (m *Member) Readable() bool { return m.State() == Healthy }

// Writable reports whether writes should be sent to this member.
//
// HEALTHY and SYNCING nodes both take writes. Sending writes to a SYNCING node
// is essential: otherwise it would fall further behind every second it spent
// resyncing and resync could never "catch up" with live traffic.
func (m *Member) Writable() bool { return m.State() != Unhealthy }

// MemberStatus is a JSON-friendly snapshot of a member.
//
// It is a plain value copy taken under the member lock (see Snapshot), so it
// can be encoded and inspected without any further locking. The field names
// mirror Member's private fields.
type MemberStatus struct {
	// Fields mirror Member's private ones: Addr is "host:port"; State renders
	// by name thanks to State.MarshalText; ConsecutiveFailures is the current
	// failure streak; BootID is the last boot ID seen by a health check;
	// LastError is the most recent failure text (both omitted when empty);
	// LastStateChange and StateTransitions describe state-change history.
	Addr                string    `json:"addr"`
	State               State     `json:"state"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	BootID              string    `json:"boot_id,omitempty"`
	LastError           string    `json:"last_error,omitempty"`
	LastStateChange     time.Time `json:"last_state_change"`
	StateTransitions    int       `json:"state_transitions"`
}

// SyncFunc is invoked (in its own goroutine) when a member needs
// resynchronisation. gen identifies the request; pass it to MarkSynced.
//
// In the real system this is Coordinator.resyncLoop, which retries the
// resync (Replicator.Resync) with backoff, checking SyncStillWanted before
// every attempt, and calls MarkSynced(addr, gen) on success.
type SyncFunc func(addr string, gen int)

// Registry holds all members. The member map is guarded by an RWMutex (it
// changes only when nodes are added); each member's state has its own mutex,
// so health updates for one node never block lookups of another.
//
// Lock discipline: the code never holds r.mu and a member's m.mu at the same
// time. Lookups copy what they need out of the map under r.mu, release it,
// and only then lock individual members. With no nested locking there is no
// lock-ordering deadlock to worry about.
type Registry struct {
	//   mu            guards the members map only (not the members' contents).
	//   members       node address -> Member. Entries are added, never removed
	//                 (runtime node removal is a known limitation, see README).
	//   failThreshold consecutive failures that turn a node UNHEALTHY
	//                 (--failure-threshold, 3 by default per the README).
	//   onSync        starts a resync for a member; nil means "no resync
	//                 machinery", and a sync request then promotes immediately.
	//   log           receives one line per state transition.
	//   newPool       builds the connection pool for a new member; injected so
	//                 the coordinator (and tests) control dial timeouts and
	//                 connection counts.
	mu            sync.RWMutex
	members       map[string]*Member
	failThreshold int
	onSync        SyncFunc
	log           *log.Logger
	newPool       func(addr string) *transport.Pool
}

// NewRegistry creates an empty registry. A member becomes Unhealthy after
// failThreshold consecutive failures. onSync may be nil.
func NewRegistry(failThreshold int, newPool func(addr string) *transport.Pool, onSync SyncFunc, logger *log.Logger) *Registry {
	// A threshold of 0 would mark nodes down before any failure; fall back to
	// the default of 3 consecutive failures.
	if failThreshold <= 0 {
		failThreshold = 3
	}
	return &Registry{
		members:       make(map[string]*Member),
		failThreshold: failThreshold,
		onSync:        onSync,
		log:           logger,
		newPool:       newPool,
	}
}

// Add registers a member in the given initial state. Adding an existing
// member is a no-op and returns false.
//
// The coordinator adds its startup nodes as Healthy and nodes added at
// runtime (POST /nodes) as Syncing, which immediately kicks off a resync so
// the new node is filled with the keys it now replicates.
func (r *Registry) Add(addr string, initial State) bool {
	// Exclusive lock: we are mutating the map.
	r.mu.Lock()
	if _, ok := r.members[addr]; ok {
		// Already known: keep the existing member (and its health history).
		r.mu.Unlock()
		return false
	}
	// A brand-new member starts with zero failures and no boot ID; the first
	// successful health check records its boot ID.
	m := &Member{Addr: addr, Pool: r.newPool(addr), state: initial, lastChange: time.Now()}
	r.members[addr] = m
	// Release the map lock BEFORE requesting a sync. requestSync may call
	// MarkSynced synchronously (when onSync is nil), which calls r.Get and
	// takes r.mu.RLock; Go's RWMutex is not reentrant, so holding r.mu here
	// would deadlock.
	r.mu.Unlock()
	if initial == Syncing {
		r.requestSync(m)
	}
	return true
}

// Get returns a member by address.
//
// A read lock suffices: many goroutines (every request's replica lookup,
// every health report) can look members up in parallel.
func (r *Registry) Get(addr string) (*Member, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.members[addr]
	return m, ok
}

// Members returns all members sorted by address.
//
// It returns a fresh slice, so callers can iterate without holding any
// registry lock. Sorting makes the output deterministic (Go map iteration
// order is randomised), which keeps /nodes output and logs stable.
func (r *Registry) Members() []*Member {
	r.mu.RLock()
	out := make([]*Member, 0, len(r.members))
	for _, m := range r.members {
		out = append(out, m)
	}
	// Drop the lock before sorting: the sort only touches our private slice
	// and immutable Addr fields.
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

// Snapshot returns the status of every member.
//
// Each member is copied under its own lock, so each MemberStatus is
// internally consistent. The snapshot as a whole is not atomic across
// members (member A may be copied before B changes), which is fine for a
// monitoring view.
func (r *Registry) Snapshot() []MemberStatus {
	ms := r.Members()
	out := make([]MemberStatus, len(ms))
	for i, m := range ms {
		m.mu.Lock()
		out[i] = MemberStatus{
			Addr: m.Addr, State: m.state, ConsecutiveFailures: m.failures, BootID: m.bootID,
			LastError: m.lastErr, LastStateChange: m.lastChange, StateTransitions: m.transitions,
		}
		m.mu.Unlock()
	}
	return out
}

// Counts returns the number of members in each state.
//
// All three states are pre-seeded with 0 so the result (used by coordinator
// /stats) always lists every state, even ones with no members.
func (r *Registry) Counts() map[State]int {
	out := map[State]int{Healthy: 0, Unhealthy: 0, Syncing: 0}
	for _, m := range r.Members() {
		out[m.State()]++
	}
	return out
}

// setState transitions m; caller holds m.mu.
//
// This is the single place where state changes, so every transition is
// logged with its reason and timestamped, and the transition counter stays
// accurate. Setting the state it already has is a silent no-op.
func (r *Registry) setState(m *Member, s State, why string) {
	if m.state == s {
		return
	}
	r.log.Printf("node %s: %s -> %s (%s)", m.Addr, m.state, s, why)
	m.state = s
	m.lastChange = time.Now()
	m.transitions++
}

// ReportFailure records a failed health check or a transport-level request
// failure. After failThreshold consecutive failures the member is marked
// Unhealthy and its connections are closed (failing requests stuck on them).
//
// Two detectors feed this function:
//
//   - ACTIVE: HealthChecker.CheckAll calls it for every failed ping. Pings
//     run every health interval (2 s by default), so with a threshold of 3 a
//     crash on an idle cluster takes a few intervals to notice.
//   - PASSIVE: Replicator.send calls it when a real data request fails with a
//     transport error (connection refused/reset, timeout). Under load many
//     requests fail within milliseconds of a crash, so the threshold is hit
//     almost immediately (the README reports ~15 ms). Application-level error
//     responses are NOT reported: a node that answers "bad request" is alive.
//
// The cost of passive detection is false positives: a node that is merely
// overloaded and times out a few requests in a row is marked down too.
func (r *Registry) ReportFailure(addr string, err error) {
	m, ok := r.Get(addr)
	if !ok {
		// Unknown address (e.g. raced with a caller holding a stale address):
		// nothing to record.
		return
	}
	m.mu.Lock()
	// Extend the consecutive-failure streak.
	m.failures++
	if err != nil {
		m.lastErr = err.Error()
	}
	// Remember whether THIS call performed the transition, so the expensive
	// side effect below happens once and outside the lock.
	becameUnhealthy := false
	// Trip only on the streak reaching the threshold, and only if not already
	// Unhealthy (further failures on a down node change nothing). Note this
	// also applies to SYNCING members: a node that fails again mid-resync
	// goes back to UNHEALTHY, which makes its running resync obsolete
	// (SyncStillWanted becomes false because state != Syncing).
	if m.failures >= r.failThreshold && m.state != Unhealthy {
		r.setState(m, Unhealthy, "consecutive failures: "+m.lastErr)
		becameUnhealthy = true
	}
	m.mu.Unlock()
	if becameUnhealthy {
		// Close the node's connections. Requests already in flight on a hung
		// connection would otherwise wait out their full timeout; closing the
		// socket fails them right away so their callers can fall back to
		// another replica. Done after Unlock so we never hold the member lock
		// while doing network/socket work.
		m.Pool.CloseConns()
	}
}

// ReportRequestSuccess records a successful data request. It resets the
// failure count but never promotes an Unhealthy member: only a health check,
// which also verifies the boot ID, can do that.
//
// Why not let a successful request revive an Unhealthy node? Requests are
// not routed to Unhealthy nodes, so a success here is typically a straggler
// that was already in flight. More importantly, a node coming back may have
// lost data (restart) or missed writes (outage); it must go through SYNCING
// and a resync, and only ReportHealthy starts that path.
func (r *Registry) ReportRequestSuccess(addr string) {
	m, ok := r.Get(addr)
	if !ok {
		return
	}
	m.mu.Lock()
	// Break the failure streak for live (HEALTHY or SYNCING) members only.
	// For an Unhealthy member the counter is left alone; it is reset by the
	// next successful health check instead.
	if m.state != Unhealthy {
		m.failures = 0
	}
	m.mu.Unlock()
}

// ReportHealthy records a successful health check carrying the node's boot
// ID. A member coming back from Unhealthy, or whose boot ID changed (it
// restarted and lost its in-memory data, possibly too fast for any check to
// fail), moves to Syncing and a resync is requested.
//
// Boot IDs explained: each node process picks a random boot ID when it
// starts and returns it in every ping. Storage is in memory, so a restarted
// process is empty. If it restarts between two pings (e.g. under a process
// supervisor, in well under the 2 s health interval), no ping ever fails and
// failure counting alone would leave it HEALTHY, serving "not found" for all
// its keys. Comparing boot IDs catches that: same address, new incarnation.
//
// Transitions performed here:
//
//	any state, boot ID changed     -> SYNCING   (lost its data)
//	UNHEALTHY, same/first boot ID  -> SYNCING   (back, but missed writes)
//	HEALTHY or SYNCING, same ID    -> unchanged (just resets the streak)
func (r *Registry) ReportHealthy(addr, bootID string) {
	m, ok := r.Get(addr)
	if !ok {
		return
	}
	m.mu.Lock()
	// A successful check ends any failure streak and clears the last error.
	m.failures = 0
	m.lastErr = ""
	needSync := false
	switch {
	// Restart detection. m.bootID == "" means we have never seen a boot ID
	// for this member (first successful check), so there is nothing to
	// compare against; we just record it below. This case is checked first,
	// so an Unhealthy node that also restarted is logged with the more
	// precise reason.
	case m.bootID != "" && bootID != m.bootID:
		r.setState(m, Syncing, "boot ID changed: node restarted")
		needSync = true
	// Recovery from an outage (crash, hang, partition). Even with the same
	// boot ID the node missed every write sent while it was UNHEALTHY
	// (writes skip Unhealthy nodes), so it must resync before serving reads.
	case m.state == Unhealthy:
		r.setState(m, Syncing, "health check succeeded")
		needSync = true
	}
	// Remember the (possibly new) incarnation for the next comparison.
	m.bootID = bootID
	m.mu.Unlock()
	if needSync {
		// Drop old connections: after a restart they point at a dead process
		// incarnation, and after an outage they may be half-broken. The pool
		// redials lazily on the next request.
		m.Pool.CloseConns()
		// Start (or restart, with a new generation) the resync. Called after
		// Unlock because requestSync takes m.mu itself.
		r.requestSync(m)
	}
}

// requestSync starts a new resync generation for m.
//
// Each call bumps syncGen and hands the new generation to onSync. Any resync
// still running for an older generation becomes obsolete: SyncStillWanted
// returns false for it (so it stops at its next check) and MarkSynced ignores
// it (so it can never promote the node).
//
// Why generations are needed (a concrete race):
//
//	t0  node fails, recovers -> SYNCING, resync gen 1 starts copying data
//	t1  node restarts (loses memory) -> SYNCING again, resync gen 2 starts
//	t2  gen 1 finishes: its copies went to the OLD process, now lost
//	    -> MarkSynced(gen 1) must be rejected, or the empty node would be
//	       promoted to HEALTHY and serve reads with missing data
//	t3  gen 2 finishes -> MarkSynced(gen 2) promotes the node correctly
//
// Note that the state alone cannot tell gen 1 and gen 2 apart: at t2 the node
// is SYNCING either way. Only the generation number distinguishes them.
func (r *Registry) requestSync(m *Member) {
	m.mu.Lock()
	m.syncGen++
	gen := m.syncGen
	m.mu.Unlock()
	if r.onSync != nil {
		// Run the resync in its own goroutine: it pages through every peer's
		// data and can take a while, and the caller is often the health
		// checker, which must not be blocked from checking other nodes.
		go r.onSync(m.Addr, gen)
	} else {
		// No resync machinery configured (tests, or a deployment that does
		// not want it): consider the member synced right away.
		r.MarkSynced(m.Addr, gen)
	}
}

// SyncStillWanted reports whether resync generation gen is still current and
// the member is still Syncing (a newer failure or restart supersedes it).
//
// The resync loop polls this before each attempt and between scan pages, so
// an obsolete resync stops doing work early instead of copying data to a node
// that has failed again or that a newer resync is already filling.
func (r *Registry) SyncStillWanted(addr string, gen int) bool {
	m, ok := r.Get(addr)
	if !ok {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Both conditions matter: the generation must be the latest AND the node
	// must still be SYNCING (if it went UNHEALTHY without a new generation
	// being issued yet, copying to it is pointless).
	return m.syncGen == gen && m.state == Syncing
}

// MarkSynced promotes a Syncing member to Healthy if gen is still the latest
// resync request. It reports whether the member was promoted.
//
// This is the only way to reach HEALTHY from SYNCING. The check and the
// transition happen under one lock acquisition, so no failure or new sync
// request can slip in between "is gen current?" and "set HEALTHY".
func (r *Registry) MarkSynced(addr string, gen int) bool {
	m, ok := r.Get(addr)
	if !ok {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Reject superseded generations and members that are no longer SYNCING
	// (e.g. they failed again during the resync).
	if m.syncGen != gen || m.state != Syncing {
		return false
	}
	r.setState(m, Healthy, "resync complete")
	return true
}

// Close closes every member's connection pool.
//
// Called on coordinator shutdown (and by tests) to release sockets.
func (r *Registry) Close() {
	for _, m := range r.Members() {
		m.Pool.Close()
	}
}
