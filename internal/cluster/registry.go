// Package cluster tracks storage-node membership and health.
//
// Failure detection here is a local, per-coordinator opinion based on
// timeouts. It is NOT consensus: a node marked UNHEALTHY may be alive but
// partitioned from the coordinator, and a slow node may be falsely suspected.
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
type State int

const (
	// Healthy nodes receive reads and writes.
	Healthy State = iota
	// Unhealthy nodes failed FailureThreshold consecutive checks/requests.
	// They receive no traffic until a health check succeeds again.
	Unhealthy
	// Syncing nodes are back (or new) but may be missing data: they receive
	// writes, but not reads, until anti-entropy resynchronisation finishes.
	Syncing
)

func (s State) String() string {
	switch s {
	case Healthy:
		return "HEALTHY"
	case Unhealthy:
		return "UNHEALTHY"
	case Syncing:
		return "SYNCING"
	default:
		return "UNKNOWN"
	}
}

// MarshalText makes State render as its name in JSON.
func (s State) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// UnmarshalText parses a state name.
func (s *State) UnmarshalText(b []byte) error {
	for _, st := range []State{Healthy, Unhealthy, Syncing} {
		if st.String() == string(b) {
			*s = st
			return nil
		}
	}
	return fmt.Errorf("unknown node state %q", b)
}

// Member is one storage node as seen by the coordinator.
type Member struct {
	Addr string
	Pool *transport.Pool

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
func (m *Member) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Readable reports whether reads may be served by this member.
func (m *Member) Readable() bool { return m.State() == Healthy }

// Writable reports whether writes should be sent to this member.
func (m *Member) Writable() bool { return m.State() != Unhealthy }

// MemberStatus is a JSON-friendly snapshot of a member.
type MemberStatus struct {
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
type SyncFunc func(addr string, gen int)

// Registry holds all members. The member map is guarded by an RWMutex (it
// changes only when nodes are added); each member's state has its own mutex,
// so health updates for one node never block lookups of another.
type Registry struct {
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
func (r *Registry) Add(addr string, initial State) bool {
	r.mu.Lock()
	if _, ok := r.members[addr]; ok {
		r.mu.Unlock()
		return false
	}
	m := &Member{Addr: addr, Pool: r.newPool(addr), state: initial, lastChange: time.Now()}
	r.members[addr] = m
	r.mu.Unlock()
	if initial == Syncing {
		r.requestSync(m)
	}
	return true
}

// Get returns a member by address.
func (r *Registry) Get(addr string) (*Member, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.members[addr]
	return m, ok
}

// Members returns all members sorted by address.
func (r *Registry) Members() []*Member {
	r.mu.RLock()
	out := make([]*Member, 0, len(r.members))
	for _, m := range r.members {
		out = append(out, m)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

// Snapshot returns the status of every member.
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
func (r *Registry) Counts() map[State]int {
	out := map[State]int{Healthy: 0, Unhealthy: 0, Syncing: 0}
	for _, m := range r.Members() {
		out[m.State()]++
	}
	return out
}

// setState transitions m; caller holds m.mu.
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
func (r *Registry) ReportFailure(addr string, err error) {
	m, ok := r.Get(addr)
	if !ok {
		return
	}
	m.mu.Lock()
	m.failures++
	if err != nil {
		m.lastErr = err.Error()
	}
	becameUnhealthy := false
	if m.failures >= r.failThreshold && m.state != Unhealthy {
		r.setState(m, Unhealthy, "consecutive failures: "+m.lastErr)
		becameUnhealthy = true
	}
	m.mu.Unlock()
	if becameUnhealthy {
		m.Pool.CloseConns()
	}
}

// ReportRequestSuccess records a successful data request. It resets the
// failure count but never promotes an Unhealthy member: only a health check,
// which also verifies the boot ID, can do that.
func (r *Registry) ReportRequestSuccess(addr string) {
	m, ok := r.Get(addr)
	if !ok {
		return
	}
	m.mu.Lock()
	if m.state != Unhealthy {
		m.failures = 0
	}
	m.mu.Unlock()
}

// ReportHealthy records a successful health check carrying the node's boot
// ID. A member coming back from Unhealthy, or whose boot ID changed (it
// restarted and lost its in-memory data, possibly too fast for any check to
// fail), moves to Syncing and a resync is requested.
func (r *Registry) ReportHealthy(addr, bootID string) {
	m, ok := r.Get(addr)
	if !ok {
		return
	}
	m.mu.Lock()
	m.failures = 0
	m.lastErr = ""
	needSync := false
	switch {
	case m.bootID != "" && bootID != m.bootID:
		r.setState(m, Syncing, "boot ID changed: node restarted")
		needSync = true
	case m.state == Unhealthy:
		r.setState(m, Syncing, "health check succeeded")
		needSync = true
	}
	m.bootID = bootID
	m.mu.Unlock()
	if needSync {
		m.Pool.CloseConns()
		r.requestSync(m)
	}
}

func (r *Registry) requestSync(m *Member) {
	m.mu.Lock()
	m.syncGen++
	gen := m.syncGen
	m.mu.Unlock()
	if r.onSync != nil {
		go r.onSync(m.Addr, gen)
	} else {
		r.MarkSynced(m.Addr, gen)
	}
}

// SyncStillWanted reports whether resync generation gen is still current and
// the member is still Syncing (a newer failure or restart supersedes it).
func (r *Registry) SyncStillWanted(addr string, gen int) bool {
	m, ok := r.Get(addr)
	if !ok {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.syncGen == gen && m.state == Syncing
}

// MarkSynced promotes a Syncing member to Healthy if gen is still the latest
// resync request. It reports whether the member was promoted.
func (r *Registry) MarkSynced(addr string, gen int) bool {
	m, ok := r.Get(addr)
	if !ok {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.syncGen != gen || m.state != Syncing {
		return false
	}
	r.setState(m, Healthy, "resync complete")
	return true
}

// Close closes every member's connection pool.
func (r *Registry) Close() {
	for _, m := range r.Members() {
		m.Pool.Close()
	}
}
