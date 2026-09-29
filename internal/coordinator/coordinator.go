// Package coordinator implements the request router: it hashes each key onto
// the consistent-hash ring, picks the replica set and executes the operation
// through the replication layer.
//
// Big picture
// -----------
// The coordinator is the single front door of the cluster. Clients never talk to
// storage nodes directly; they send GET/PUT/DELETE frames here and this package
// decides WHERE each key lives and HOW to talk to those places. It glues together
// four sub-systems, each living in its own package:
//
//	consistenthash.Ring      "which nodes own key k?"  (placement)
//	cluster.Registry         "which of those nodes are usable right now?"  (health view)
//	cluster.HealthChecker    periodic pings that keep the registry honest  (detection)
//	replication.Replicator   "run this op on those nodes, quorum rules" (consistency)
//
// A PUT, end to end (see README "Request lifecycle"):
//
//	client --PUT k v--> transport.Server --> Coordinator.Handle
//	                                             |
//	                          ring.Replicas(k,N) = [primary, r1, r2]
//	                                             |
//	                          Replicator.Write: stamp version, fan out in parallel,
//	                          wait for W acks (primary included when it is up)
//	                                             |
//	client <----------------- OK / error <-------+
//
// Why a coordinator at all (vs smart clients or gossip)? One process owns the
// ring, the health view and the version clock, so clients stay trivial and all
// versions come from one monotonic clock. The cost, documented in README
// "Tradeoffs", is an extra network hop and a single point of failure.
package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"distkv/internal/cluster"
	"distkv/internal/consistenthash"
	"distkv/internal/metrics"
	"distkv/internal/node"
	"distkv/internal/protocol"
	"distkv/internal/replication"
	"distkv/internal/transport"
)

// Config configures a coordinator.
//
// Every knob is a command-line flag of cmd/coordinator. The replication triple
// (ReplicationN, WriteQuorum, ReadQuorum) is the classic Dynamo-style N/W/R:
//
//   - N = how many distinct physical nodes hold each key.
//   - W = how many of them must acknowledge a write before the client gets OK.
//   - R = how many must answer a read before the coordinator replies.
//
// If R + W > N, every read quorum overlaps every write quorum in at least one
// node, so a quorum read always sees the latest acknowledged write. The default
// W=2, R=1 with N=3 does NOT satisfy that (2+1 = 3), which is why the replicator
// additionally forces the primary into every write quorum: reads go to the
// primary first, so read-your-writes still holds while the primary is healthy.
//
// Fields:
//   - Nodes is the initial list of storage-node addresses ("host:port"). Each is
//     registered as HEALTHY and hashed onto the ring in New.
//   - VirtualNodes is how many points each physical node gets on the ring. More
//     points = smoother key distribution (README: 128 vnodes gives ~1.04x the ideal
//     max load vs 1.44x with one point per node).
//   - ReplicationN is N: how many distinct physical nodes store each key.
//   - WriteQuorum is W: acknowledgements required before a write succeeds.
//   - ReadQuorum is R: 1 means "read the primary, fall back down the preference
//     list"; >= 2 means "ask all readable replicas, wait for R, take the highest
//     version and read-repair stale ones".
//   - HealthInterval is how often the HealthChecker pings every node.
//   - HealthTimeout bounds each ping (and each per-node stats fetch), so a hung node
//     delays a health round by at most this long.
//   - FailureThreshold is how many CONSECUTIVE failures (failed pings or transport
//     errors on real requests) mark a node UNHEALTHY. Requiring several strikes
//     trades detection speed for fewer false positives on a blip.
//   - DialTimeout bounds establishing a TCP connection to a storage node.
//   - RequestTimeout bounds every single RPC to one replica. Without it, one hung
//     node (e.g. SIGSTOP) could hold a client request forever.
//   - ConnsPerNode is how many multiplexed TCP connections the coordinator keeps to
//     each storage node. Each connection carries many requests concurrently.
//   - Server configures the client-facing TCP server (limits, deadlines, ...).
//   - Logger receives state transitions, resync progress and errors.
type Config struct {
	Nodes            []string
	VirtualNodes     int
	ReplicationN     int
	WriteQuorum      int
	ReadQuorum       int
	HealthInterval   time.Duration
	HealthTimeout    time.Duration
	FailureThreshold int
	DialTimeout      time.Duration
	RequestTimeout   time.Duration // per replica RPC
	ConnsPerNode     int           // multiplexed connections per storage node
	Server           transport.ServerConfig
	Logger           *log.Logger
}

// DefaultConfig returns the defaults: 128 vnodes, N=3, W=2, R=1, health
// checks every 2s, 3 strikes to mark a node unhealthy.
//
// These are the numbers README "Failure handling" quotes: 2 s pings with a
// 500 ms timeout, 3 consecutive failures, 500 ms dial timeout, 1 s per-replica
// request timeout.
func DefaultConfig() Config {
	return Config{
		VirtualNodes:     consistenthash.DefaultVirtualNodes,
		ReplicationN:     3,
		WriteQuorum:      2,
		ReadQuorum:       1,
		HealthInterval:   2 * time.Second,
		HealthTimeout:    500 * time.Millisecond,
		FailureThreshold: 3,
		DialTimeout:      500 * time.Millisecond,
		RequestTimeout:   time.Second,
		ConnsPerNode:     1,
		Server:           transport.DefaultServerConfig(),
		Logger:           log.Default(),
	}
}

// Validate checks the replication parameters.
//
// Quorums must lie in [1, N]: W=0 would acknowledge writes nobody stored, and
// W>N could never be satisfied, so every write would fail. Note that W and R
// are checked against the configured N; if the cluster has fewer physical
// nodes than N, the replicator later clamps the quorum to the actual replica
// count.
func (c *Config) Validate() error {
	// A coordinator with no storage nodes has nowhere to put data.
	if len(c.Nodes) == 0 {
		return errors.New("at least one storage node is required")
	}
	// N=0 would mean "store each key on zero nodes".
	if c.ReplicationN < 1 {
		return fmt.Errorf("replication factor must be >= 1, got %d", c.ReplicationN)
	}
	// W must be achievable (<= N) and meaningful (>= 1).
	if c.WriteQuorum < 1 || c.WriteQuorum > c.ReplicationN {
		return fmt.Errorf("write quorum must be in [1, %d], got %d", c.ReplicationN, c.WriteQuorum)
	}
	// Same for R.
	if c.ReadQuorum < 1 || c.ReadQuorum > c.ReplicationN {
		return fmt.Errorf("read quorum must be in [1, %d], got %d", c.ReplicationN, c.ReadQuorum)
	}
	return nil
}

// Coordinator routes client requests to storage nodes.
//
// Concurrency: Handle is called from many goroutines at once (one per in-flight
// client request). Everything it touches is either immutable after New (cfg,
// the latency map itself), internally synchronised (ring: RWMutex, registry:
// RWMutex + per-member mutex, replicator: atomics), or atomic (the counters).
// So the coordinator itself needs no lock of its own.
//
// Fields:
//   - cfg is the validated configuration; read-only after New.
//   - ring maps keys to the ordered list of nodes that own them.
//   - reg is the membership + health view: per-node state
//     (HEALTHY/UNHEALTHY/SYNCING), failure counts, boot IDs, connection pools.
//   - repl executes reads and writes against replica sets with quorum rules, and
//     performs anti-entropy resync.
//   - health pings every node periodically and feeds results into reg.
//   - server accepts client connections and calls Handle for each request.
//   - log is the logger (cfg.Logger, never nil after New).
//   - start is when the coordinator was created, for uptime in Stats.
//   - ctx is the lifetime context of background work (health loop, resync loops).
//     cancel is called by Shutdown so those goroutines exit.
//   - wg tracks the health-check goroutine so Shutdown can wait for it. Resync loops
//     are deliberately not tracked (see resyncLoop).
//   - Request counters. atomic.Int64 lets many request goroutines bump them without
//     a lock, and lets Stats read them without blocking requests.
//   - latency holds one lock-free histogram per data op (GET/PUT/DELETE). The map is
//     built once in New and never modified, so concurrent reads of the map are safe;
//     the histograms themselves are updated with atomics.
type Coordinator struct {
	cfg    Config
	ring   *consistenthash.Ring
	reg    *cluster.Registry
	repl   *replication.Replicator
	health *cluster.HealthChecker
	server *transport.Server
	log    *log.Logger
	start  time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	requests, gets, puts, deletes, notFound, errors atomic.Int64
	latency                                         map[protocol.Op]*metrics.Histogram
}

// New creates a coordinator. Call Start before serving.
//
// New only wires objects together; it does no network I/O. The wiring has one
// subtle circular dependency: the registry must be able to trigger a resync
// (when a node comes back), and resync needs the ring and the replicator,
// which in turn need the registry. It is broken by handing the registry a
// callback, c.resyncLoop, a method value that closes over c. By the time the
// registry ever invokes it, c.ring and c.repl are fully set.
//
//	Registry --(node needs sync: onSync(addr, gen))--> Coordinator.resyncLoop
//	   ^                                                     |
//	   |                                                     v
//	   +------ MarkSynced / SyncStillWanted <------ Replicator.Resync
func New(cfg Config) (*Coordinator, error) {
	// Reject impossible N/W/R combinations before building anything.
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// Tolerate a zero-value Logger so callers can build Config by hand.
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	// The client-facing server logs to the same place as the coordinator.
	cfg.Server.Logger = cfg.Logger
	// A cancellable root context for background goroutines. It is derived from
	// Background (not from any request) because it must outlive every request;
	// Shutdown cancels it.
	ctx, cancel := context.WithCancel(context.Background())
	c := &Coordinator{
		cfg:    cfg,
		ring:   consistenthash.New(cfg.VirtualNodes),
		log:    cfg.Logger,
		start:  time.Now(),
		ctx:    ctx,
		cancel: cancel,
		// Only data ops get latency histograms; PING/STATS/LOCATE are
		// administrative and would pollute the percentiles.
		latency: map[protocol.Op]*metrics.Histogram{
			protocol.OpGet: {}, protocol.OpPut: {}, protocol.OpDelete: {},
		},
	}
	// newPool is a factory the registry calls once per member to build that
	// node's connection pool: ConnsPerNode multiplexed TCP connections with the
	// dial timeout and the same max frame size the server enforces (so a node
	// cannot send a frame the coordinator would reject, or vice versa).
	newPool := func(addr string) *transport.Pool {
		return transport.NewPool(addr, cfg.DialTimeout, cfg.ConnsPerNode, cfg.Server.Limits.MaxFrameSize)
	}
	// The registry: FailureThreshold strikes -> UNHEALTHY; c.resyncLoop is run
	// (in its own goroutine) whenever a node enters SYNCING.
	c.reg = cluster.NewRegistry(cfg.FailureThreshold, newPool, c.resyncLoop, cfg.Logger)
	// The replicator enforces N/W/R and the per-replica timeout. It reports
	// transport errors back into the registry (passive failure detection), which
	// is how a crash under load is noticed in milliseconds rather than waiting
	// for several health intervals.
	c.repl = replication.New(replication.Config{
		N: cfg.ReplicationN, W: cfg.WriteQuorum, R: cfg.ReadQuorum, RequestTimeout: cfg.RequestTimeout,
	}, c.reg, cfg.Logger)
	// Active failure detection: periodic pings. Not started until Start.
	c.health = cluster.NewHealthChecker(c.reg, cfg.HealthInterval, cfg.HealthTimeout)
	// The server calls c.Handle for every decoded request (method value).
	c.server = transport.NewServer(cfg.Server, c.Handle)

	for _, addr := range cfg.Nodes {
		// Nodes start optimistic; the first health round (in Start) corrects
		// that before any traffic is served.
		//
		// More precisely: that first round records each live node's boot ID (the
		// baseline later used to detect restarts) and counts one strike against
		// each node that did not answer. Because a node needs FailureThreshold
		// consecutive strikes to become UNHEALTHY, a node that is down at startup
		// may still be listed HEALTHY after that single round; transport errors on
		// real requests (and later pings) add the remaining strikes quickly, and
		// reads meanwhile fall back to the next replica on error.
		//
		// Starting HEALTHY rather than SYNCING is correct at cluster birth: there
		// is no older data anywhere that a node could be missing, so no resync is
		// needed. (AddNode, by contrast, starts nodes in SYNCING.)
		c.reg.Add(addr, cluster.Healthy)
		// Registry first, then ring: routing may only ever return nodes the
		// registry knows about.
		c.ring.Add(addr)
	}
	return c, nil
}

// Start runs an initial health check round and starts the periodic checker.
//
// The first CheckAll is synchronous on purpose: by the time Start returns, the
// registry holds each reachable node's boot ID and a first opinion of each
// node, so the startup log line below reports reality rather than the
// optimistic defaults from New. The periodic loop then runs in the background
// until Shutdown cancels c.ctx.
func (c *Coordinator) Start() {
	// One blocking round: pings every node in parallel, waits at most roughly
	// HealthTimeout (a hung node cannot delay the others).
	c.health.CheckAll(c.ctx)
	// Track the health goroutine in wg so Shutdown can wait until it has
	// actually stopped before closing connection pools under it.
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// Ticks every HealthInterval; returns when c.ctx is cancelled.
		c.health.Run(c.ctx)
	}()
	// Log the effective topology and quorum parameters once at startup.
	counts := c.reg.Counts()
	c.log.Printf("coordinator: %d nodes (%d healthy), vnodes=%d N=%d W=%d R=%d",
		len(c.cfg.Nodes), counts[cluster.Healthy], c.cfg.VirtualNodes, c.cfg.ReplicationN, c.cfg.WriteQuorum, c.cfg.ReadQuorum)
}

// Serve serves client connections on ln until Shutdown.
//
// Taking a listener (rather than an address) lets tests listen on ":0" and learn
// the chosen port before serving.
func (c *Coordinator) Serve(ln net.Listener) error { return c.server.Serve(ln) }

// ListenAndServe listens on addr and serves until Shutdown.
func (c *Coordinator) ListenAndServe(addr string) error { return c.server.ListenAndServe(addr) }

// Shutdown stops serving, stops background work and closes node pools.
//
// Order matters:
//  1. Stop the client-facing server first (it drains in-flight requests until
//     ctx expires), so no new requests start using node connections.
//  2. Cancel c.ctx: the health loop and any resync loops see it and exit.
//  3. Wait for the health goroutine, so it cannot ping through a pool that is
//     about to be closed.
//  4. Close every node connection pool.
//
// Background write stragglers (replicas still in flight after the quorum was
// met) are not waited for; closing the pools fails them, which is harmless
// because those writes were already acknowledged by a quorum.
func (c *Coordinator) Shutdown(ctx context.Context) error {
	err := c.server.Shutdown(ctx)
	c.cancel()
	c.wg.Wait()
	c.reg.Close()
	return err
}

// AddNode adds a storage node at runtime. Only ~1/(n+1) of keys change
// ownership; the new node starts in SYNCING (receives writes, not reads) and
// is populated by resync before serving reads.
//
// It is reached through the admin HTTP endpoint (POST /nodes?addr=host:port in
// cmd/coordinator). The lifecycle of a newly added node:
//
//	AddNode -> registry: SYNCING (registry.Add calls requestSync -> resyncLoop)
//	        -> ring: node now owns ~1/(n+1) of the key space
//	        -> live writes for those keys already go to it (SYNCING is writable)
//	        -> reads for those keys skip it and fall back to older replicas,
//	           which still hold the data
//	        -> resync copies every entry it now replicates from the other nodes
//	        -> MarkSynced: HEALTHY, starts serving reads
//
// Why SYNCING and not HEALTHY? If the new node became a key's primary and
// immediately served reads, every key it took over would read as NOT_FOUND until
// someone rewrote it. Live writes during the resync are safe because both live
// writes and resync copies carry versions and the node applies last-writer-wins:
// an older copied value can never overwrite a newer live write.
func (c *Coordinator) AddNode(addr string) error {
	// Reject garbage early; the pool would otherwise fail on every dial.
	if _, err := net.ResolveTCPAddr("tcp", addr); err != nil {
		return fmt.Errorf("bad node address %q: %w", addr, err)
	}
	// Register before adding to the ring so routing never sees an unknown node.
	//
	// If the order were reversed, a request racing with AddNode could get this
	// address from ring.Replicas, find no registry member, and treat a
	// perfectly good replica as missing. Registering in SYNCING also kicks off
	// the resync goroutine (Registry.Add -> requestSync -> resyncLoop).
	// Add returns false for a duplicate address, which would otherwise double
	// its points on the ring.
	if !c.reg.Add(addr, cluster.Syncing) {
		return fmt.Errorf("node %s already present", addr)
	}
	// Now hash the node's virtual points onto the ring. From this instant,
	// ring.Replicas returns it for the keys it owns, so resync (which filters
	// entries by ring.Replicas) and live writes agree on what it should hold.
	c.ring.Add(addr)
	c.log.Printf("node %s added to ring (%d nodes)", addr, c.ring.Len())
	return nil
}

// resyncLoop runs resynchronisation for addr until it succeeds or is
// superseded (the node failed/restarted again, starting a newer generation).
// It is not tracked by c.wg: it observes c.ctx and exits promptly on shutdown.
//
// It is the registry's onSync callback, started in its own goroutine every time
// a member enters SYNCING (runtime add, recovery from UNHEALTHY, or a boot-ID
// change after a fast restart). gen is the member's sync generation at the
// moment the request was made.
//
// Sync generations, and why they exist: resyncs can overlap. Suppose node X
// comes back (gen 1 starts), then crashes and comes back again mid-resync
// (gen 2 starts). The gen-1 loop is now copying data into a process that was
// wiped; if it finished and promoted X to HEALTHY, X would serve reads with
// half its data. So every step checks "is my gen still the latest, and is the
// node still SYNCING?" and gives up otherwise; only the newest generation may
// call MarkSynced.
//
//	for attempt = 1, 2, 3, ...
//	    stop if shutting down or gen superseded
//	    Resync(addr, gen)  --ok-->  MarkSynced(addr, gen)  -> HEALTHY, done
//	         | ErrSyncSuperseded --> stop (a newer loop owns the node)
//	         | other error       --> log, back off, retry
//
// Why retry forever instead of giving up? A SYNCING node never serves reads, so
// abandoning the resync would leave it permanently read-dead. Failures here are
// usually transient (a source node timing out mid-scan).
func (c *Coordinator) resyncLoop(addr string, gen int) {
	// "Which nodes should hold key k?" answered with the CURRENT ring. Resync
	// uses it to forward only the entries whose replica set includes addr.
	replicasFor := func(key string) []string { return c.ring.Replicas(key, c.cfg.ReplicationN) }
	for attempt := 1; ; attempt++ {
		// Cheap early exit: coordinator shutting down, or this generation has
		// been superseded / the node left SYNCING (e.g. went UNHEALTHY again).
		if c.ctx.Err() != nil || !c.reg.SyncStillWanted(addr, gen) {
			return
		}
		// Page through every other readable node (SCAN) and push the relevant
		// entries to addr (APPLY_BATCH). Resync re-checks the generation before
		// every page, so a superseded run stops mid-way.
		err := c.repl.Resync(c.ctx, addr, gen, replicasFor)
		if err == nil {
			// Promote SYNCING -> HEALTHY, but only if gen is still current.
			// MarkSynced re-checks under the member's lock, closing the window
			// between Resync finishing and this call.
			c.reg.MarkSynced(addr, gen)
			return
		}
		// A newer generation owns the node now; its own loop will finish the job.
		if errors.Is(err, replication.ErrSyncSuperseded) {
			return
		}
		c.log.Printf("resync of %s failed (attempt %d): %v", addr, attempt, err)
		// Linear backoff: 500 ms, 1 s, 1.5 s, ... capped at 5 s, so a
		// persistently failing resync does not hammer the cluster with full
		// scans. Selecting on ctx.Done makes shutdown interrupt the wait.
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(min(time.Duration(attempt)*500*time.Millisecond, 5*time.Second)):
		}
	}
}

// Handle dispatches one client request.
//
// It runs concurrently for many requests (the transport server gives each
// request its own goroutine), so it only touches thread-safe state. The flow:
// count -> route by op -> record latency -> classify outcome -> return.
//
// About ctx: the server passes its own lifetime context, which is cancelled
// only when Shutdown gives up waiting. Per-request deadlines are therefore not
// taken from ctx here; instead every individual replica RPC gets
// cfg.RequestTimeout inside the replicator, so nothing waits forever on a dead
// node.
func (c *Coordinator) Handle(ctx context.Context, req *protocol.Request) *protocol.Response {
	// Total requests of every kind, including admin ops and bad requests.
	c.requests.Add(1)
	// Measured from here so latency covers routing + replication, i.e. what a
	// client sees minus the network hop to the coordinator.
	start := time.Now()
	var resp *protocol.Response
	switch req.Op {
	case protocol.OpGet:
		c.gets.Add(1)
		// R=1: primary first, falling back down the preference list.
		// R>=2: quorum read with read repair. Both live in the replicator.
		resp = c.repl.Read(ctx, c.replicas(req.Key), req.Key)
	case protocol.OpPut:
		c.puts.Add(1)
		// Versioned, parallel fan-out to all writable replicas; returns once W
		// have acked (with the primary among them when it is up).
		resp = c.repl.Write(ctx, c.replicas(req.Key), protocol.OpPut, req.Key, req.Value)
	case protocol.OpDelete:
		c.deletes.Add(1)
		// A delete is a versioned write of a tombstone (nil value), not a
		// removal. That way an older PUT arriving late, a retry, or a resync copy
		// cannot resurrect the key: the tombstone's newer version wins under
		// last-writer-wins.
		resp = c.repl.Write(ctx, c.replicas(req.Key), protocol.OpDelete, req.Key, nil)
	case protocol.OpPing:
		// Cluster-level health (ok/degraded/down) as JSON. The bool is ignored
		// here; a ping always "succeeds" and the status field carries the news.
		h, _ := c.Health()
		resp = jsonResponse(h)
	case protocol.OpStats:
		// Full stats including a concurrent fetch of every node's own stats.
		resp = jsonResponse(c.Stats(ctx, true))
	case protocol.OpLocate:
		// Debugging aid: where does this key live and in what state are those
		// nodes?
		resp = jsonResponse(c.Locate(req.Key))
	default:
		// Node-internal ops (SCAN, APPLY_BATCH, ...) are not meant for clients.
		resp = protocol.ErrorResponse(protocol.StatusBadRequest, "coordinator does not support %s", req.Op)
	}
	// Record latency only for ops that have a histogram (GET/PUT/DELETE).
	if h, ok := c.latency[req.Op]; ok {
		h.Record(time.Since(start))
	}
	// Classify the outcome. NOT_FOUND is a legitimate answer, counted separately
	// so it does not inflate the error rate; OK counts nothing extra; anything
	// else (quorum failures, unavailable, bad request, ...) is an error.
	switch resp.Status {
	case protocol.StatusNotFound:
		c.notFound.Add(1)
	case protocol.StatusOK:
	default:
		c.errors.Add(1)
	}
	return resp
}

// replicas returns key's preference list: [primary, replica1, replica2, ...],
// N distinct physical nodes (fewer if the cluster is smaller than N). The
// order is significant: index 0 is the primary, which reads try first and
// which must be in every write quorum while it is up.
func (c *Coordinator) replicas(key string) []string {
	return c.ring.Replicas(key, c.cfg.ReplicationN)
}

// jsonResponse wraps any JSON-serialisable document (health, stats, location)
// in an OK response. A marshal failure becomes an error response instead of a
// panic.
func jsonResponse(v any) *protocol.Response {
	b, err := json.Marshal(v)
	if err != nil {
		return protocol.ErrorResponse(protocol.StatusError, "marshal: %v", err)
	}
	return &protocol.Response{Status: protocol.StatusOK, Value: b}
}

// Location describes where a key lives.
//
// Returned by the LOCATE op; useful for demos and debugging ("which node do I
// kill to test primary failover for this key?").
//
// Fields:
//   - Key is the key that was asked about.
//   - Hash is the key's position on the ring (FNV-1a + fmix64).
//   - Replicas is the preference list, primary first.
type Location struct {
	Key      string            `json:"key"`
	Hash     uint64            `json:"hash"`
	Replicas []ReplicaLocation `json:"replicas"`
}

// ReplicaLocation is one member of a key's replica set.
//
// Fields:
//   - Addr is the storage node's address.
//   - Role is "primary" for index 0 of the preference list, else "replica".
//   - State is the coordinator's current opinion of that node.
type ReplicaLocation struct {
	Addr  string        `json:"addr"`
	Role  string        `json:"role"`
	State cluster.State `json:"state"`
}

// Locate returns the replica set for key in preference order.
func (c *Coordinator) Locate(key string) Location {
	loc := Location{Key: key, Hash: consistenthash.Hash(key)}
	for i, addr := range c.replicas(key) {
		rl := ReplicaLocation{Addr: addr, Role: "replica"}
		// The first node clockwise from the key's hash is the primary.
		if i == 0 {
			rl.Role = "primary"
		}
		// Attach the node's health state; every ring node should be registered
		// (AddNode and New register before adding to the ring).
		if m, ok := c.reg.Get(addr); ok {
			rl.State = m.State()
		}
		loc.Replicas = append(loc.Replicas, rl)
	}
	return loc
}

// HealthDoc is the coordinator's health document.
//
// Fields:
//   - Status summarises the cluster: "ok", "degraded" or "down".
//   - HealthyNodes serve reads and writes.
//   - UnhealthyNodes get no traffic.
//   - SyncingNodes get writes but not reads while resync runs.
type HealthDoc struct {
	Status         string `json:"status"` // ok | degraded | down
	HealthyNodes   int    `json:"healthy_nodes"`
	UnhealthyNodes int    `json:"unhealthy_nodes"`
	SyncingNodes   int    `json:"syncing_nodes"`
}

// Health reports "ok" when all nodes are healthy, "degraded" when some are
// not, and "down" when none are.
//
// The second return value is true when at least one node is HEALTHY; the admin
// HTTP endpoint in cmd/coordinator uses it to choose the HTTP status code.
// "degraded" is still considered up: with N=3 the cluster keeps serving
// through a single node failure.
func (c *Coordinator) Health() (HealthDoc, bool) {
	counts := c.reg.Counts()
	h := HealthDoc{Status: "ok", HealthyNodes: counts[cluster.Healthy], UnhealthyNodes: counts[cluster.Unhealthy], SyncingNodes: counts[cluster.Syncing]}
	// Order matters: "down" (no healthy node) takes precedence over "degraded".
	switch {
	case h.HealthyNodes == 0:
		h.Status = "down"
	case h.UnhealthyNodes+h.SyncingNodes > 0:
		h.Status = "degraded"
	}
	return h, h.HealthyNodes > 0
}

// Stats is the coordinator's statistics document.
//
// It is what `bin/client stats` and GET /stats show: coordinator-level
// counters plus, optionally, every node's own stats, and cluster-wide cache
// totals summed from those.
//
// Fields:
//   - UptimeSeconds is time since New.
//   - Config echoes the effective parameters, so a stats dump is self-describing.
//   - Requests counts every request Handle saw, of any op.
//   - Gets, Puts and Deletes count data ops by type.
//   - NotFound counts responses with StatusNotFound (not errors).
//   - Errors counts responses that were neither OK nor NOT_FOUND.
//   - Latency maps op name -> percentile summary from its histogram.
//   - Replication holds the replicator's counters: fallbacks, read repairs, quorum
//     failures, skipped writes, resyncs.
//   - Health is the same document PING returns.
//   - Connections describes the client-facing server's connections.
//   - Writes reports frame write coalescing (frames per write syscall).
//   - Cache sums the LRU counters of every node whose stats were fetched.
//   - Nodes has one report per registered member, sorted by address.
type Stats struct {
	UptimeSeconds float64                    `json:"uptime_seconds"`
	Config        ConfigSummary              `json:"config"`
	Requests      int64                      `json:"requests"`
	Gets          int64                      `json:"gets"`
	Puts          int64                      `json:"puts"`
	Deletes       int64                      `json:"deletes"`
	NotFound      int64                      `json:"not_found"`
	Errors        int64                      `json:"request_errors"`
	Latency       map[string]metrics.Summary `json:"latency"`
	Replication   replication.Stats          `json:"replication"`
	Health        HealthDoc                  `json:"health"`
	Connections   transport.ServerStats      `json:"connections"`
	Writes        transport.WriteStats       `json:"write_coalescing"`
	Cache         CacheTotals                `json:"cache_totals"`
	Nodes         []NodeReport               `json:"nodes"`
}

// ConfigSummary echoes the effective configuration.
type ConfigSummary struct {
	VirtualNodes     int     `json:"virtual_nodes"`
	ReplicationN     int     `json:"replication_factor"`
	WriteQuorum      int     `json:"write_quorum"`
	ReadQuorum       int     `json:"read_quorum"`
	HealthIntervalMS float64 `json:"health_interval_ms"`
	RequestTimeoutMS float64 `json:"request_timeout_ms"`
	FailureThreshold int     `json:"failure_threshold"`
}

// CacheTotals sums cache counters across reachable nodes.
//
// Each node has its own LRU; there is no global cache. Summing gives the
// cluster-wide picture. HitRate is recomputed from the summed counts, not
// averaged per node, so a busy node weighs more than an idle one, which is the
// correct weighting.
type CacheTotals struct {
	Hits      int64   `json:"hits"`
	Misses    int64   `json:"misses"`
	Evictions int64   `json:"evictions"`
	HitRate   float64 `json:"hit_rate"`
}

// NodeReport is a member's status plus (if reachable) its own stats.
//
// Fields:
//   - MemberStatus (embedded): address, state, consecutive failures, boot ID,
//     last error, last state change and transition count (the coordinator's
//     view).
//   - RingShare is the fraction of the hash space for which this node is the primary
//     (the sum of its virtual nodes' arcs).
//   - Stats is the node's own stats document, if it was fetched successfully.
//   - StatsErr explains why Stats is missing when a fetch was attempted and failed.
type NodeReport struct {
	cluster.MemberStatus
	RingShare float64     `json:"ring_share"`
	Stats     *node.Stats `json:"stats,omitempty"`
	StatsErr  string      `json:"stats_error,omitempty"`
}

// Stats returns coordinator statistics; with includeNodes it also queries
// every node's stats concurrently (bounded by the health timeout).
//
// Aggregation pattern (fan-out / fan-in):
//
//	snapshot members --+--> goroutine: fetch stats node A --> write s.Nodes[0]
//	                   +--> goroutine: fetch stats node B --> write s.Nodes[1]
//	                   +--> goroutine: fetch stats node C --> write s.Nodes[2]
//	wg.Wait() ---------+    (each bounded by HealthTimeout)
//	sort by address, sum cache counters, compute hit rate
//
// Querying nodes in parallel means the call costs roughly one HealthTimeout in
// the worst case instead of one per node. Each goroutine owns exactly one slot
// of s.Nodes, so no lock is needed; wg.Wait() is the happens-before edge that
// makes their writes visible to the aggregation below.
func (c *Coordinator) Stats(ctx context.Context, includeNodes bool) Stats {
	health, _ := c.Health()
	// Coordinator-local numbers: all atomic loads or cheap snapshots, no
	// blocking of in-flight requests. The counters are read one by one, so they
	// are not a single consistent instant; that is fine for monitoring.
	s := Stats{
		UptimeSeconds: time.Since(c.start).Seconds(),
		Config: ConfigSummary{
			VirtualNodes: c.cfg.VirtualNodes, ReplicationN: c.cfg.ReplicationN,
			WriteQuorum: c.cfg.WriteQuorum, ReadQuorum: c.cfg.ReadQuorum,
			HealthIntervalMS: float64(c.cfg.HealthInterval.Milliseconds()),
			RequestTimeoutMS: float64(c.cfg.RequestTimeout.Milliseconds()),
			FailureThreshold: c.cfg.FailureThreshold,
		},
		Requests:    c.requests.Load(),
		Gets:        c.gets.Load(),
		Puts:        c.puts.Load(),
		Deletes:     c.deletes.Load(),
		NotFound:    c.notFound.Load(),
		Errors:      c.errors.Load(),
		Latency:     map[string]metrics.Summary{},
		Replication: c.repl.Stats(),
		Health:      health,
		Connections: c.server.Stats(),
		Writes:      transport.ReadWriteStats(),
	}
	// Turn each op's histogram into a percentile summary keyed by op name.
	for op, h := range c.latency {
		s.Latency[op.String()] = h.Snapshot()
	}
	// Ring share per node and a status snapshot of every member.
	shares := c.ring.Shares()
	members := c.reg.Snapshot()
	// Allocate the full slice up front: the goroutines below hold pointers into
	// it, so it must never be re-allocated (append could move it) while they run.
	s.Nodes = make([]NodeReport, len(members))
	var wg sync.WaitGroup
	for i, ms := range members {
		s.Nodes[i] = NodeReport{MemberStatus: ms, RingShare: shares[ms.Addr]}
		// Skip the network call when not asked (the HTTP /stats handler passes
		// false) or when the node is known to be down: asking an UNHEALTHY node
		// would just burn a full timeout. SYNCING nodes are alive, so they are
		// asked.
		if !includeNodes || ms.State == cluster.Unhealthy {
			continue
		}
		wg.Add(1)
		// Pass &s.Nodes[i] as an argument so each goroutine gets its own slot.
		go func(r *NodeReport) {
			defer wg.Done()
			ns, err := c.fetchNodeStats(ctx, r.Addr)
			if err != nil {
				// Partial results beat no results: record why this node is missing
				// and keep going.
				r.StatsErr = err.Error()
				return
			}
			r.Stats = ns
		}(&s.Nodes[i])
	}
	// Fan-in: every fetch has finished (or timed out) past this point.
	wg.Wait()
	// Deterministic output order. This must happen after wg.Wait(): sorting
	// while goroutines still write through their pointers would move elements
	// underneath them and mix up reports.
	sort.Slice(s.Nodes, func(i, j int) bool { return s.Nodes[i].Addr < s.Nodes[j].Addr })
	// Sum cache counters over the nodes that answered.
	for _, n := range s.Nodes {
		if n.Stats != nil {
			s.Cache.Hits += n.Stats.Cache.Hits
			s.Cache.Misses += n.Stats.Cache.Misses
			s.Cache.Evictions += n.Stats.Cache.Evictions
		}
	}
	// Cluster hit rate = total hits / total lookups (guard against 0/0).
	if t := s.Cache.Hits + s.Cache.Misses; t > 0 {
		s.Cache.HitRate = float64(s.Cache.Hits) / float64(t)
	}
	return s
}

// fetchNodeStats asks one storage node for its stats document.
//
// It calls the member's pool directly rather than going through the
// replicator, so a failed stats fetch is NOT reported to the failure detector:
// monitoring traffic should not be able to mark a node down. It uses
// HealthTimeout (short) rather than RequestTimeout, because stats are a
// best-effort side channel and one slow node should not stall the whole dump.
func (c *Coordinator) fetchNodeStats(ctx context.Context, addr string) (*node.Stats, error) {
	m, ok := c.reg.Get(addr)
	if !ok {
		return nil, fmt.Errorf("unknown node %s", addr)
	}
	// Bound the RPC; defer cancel releases the timer as soon as we return.
	ctx, cancel := context.WithTimeout(ctx, c.cfg.HealthTimeout)
	defer cancel()
	resp, err := m.Pool.Do(ctx, &protocol.Request{Op: protocol.OpStats})
	// Transport-level failure: dial refused, connection reset, timeout...
	if err != nil {
		return nil, err
	}
	// Application-level failure: the node answered with an error status.
	if err := resp.Err(); err != nil {
		return nil, err
	}
	// The value is the node's JSON stats document (node.Stats).
	var ns node.Stats
	if err := json.Unmarshal(resp.Value, &ns); err != nil {
		return nil, fmt.Errorf("decode stats from %s: %w", addr, err)
	}
	return &ns, nil
}
