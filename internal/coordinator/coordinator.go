// Package coordinator implements the request router: it hashes each key onto
// the consistent-hash ring, picks the replica set and executes the operation
// through the replication layer.
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
func (c *Config) Validate() error {
	if len(c.Nodes) == 0 {
		return errors.New("at least one storage node is required")
	}
	if c.ReplicationN < 1 {
		return fmt.Errorf("replication factor must be >= 1, got %d", c.ReplicationN)
	}
	if c.WriteQuorum < 1 || c.WriteQuorum > c.ReplicationN {
		return fmt.Errorf("write quorum must be in [1, %d], got %d", c.ReplicationN, c.WriteQuorum)
	}
	if c.ReadQuorum < 1 || c.ReadQuorum > c.ReplicationN {
		return fmt.Errorf("read quorum must be in [1, %d], got %d", c.ReplicationN, c.ReadQuorum)
	}
	return nil
}

// Coordinator routes client requests to storage nodes.
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
func New(cfg Config) (*Coordinator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	cfg.Server.Logger = cfg.Logger
	ctx, cancel := context.WithCancel(context.Background())
	c := &Coordinator{
		cfg:    cfg,
		ring:   consistenthash.New(cfg.VirtualNodes),
		log:    cfg.Logger,
		start:  time.Now(),
		ctx:    ctx,
		cancel: cancel,
		latency: map[protocol.Op]*metrics.Histogram{
			protocol.OpGet: {}, protocol.OpPut: {}, protocol.OpDelete: {},
		},
	}
	newPool := func(addr string) *transport.Pool {
		return transport.NewPool(addr, cfg.DialTimeout, cfg.ConnsPerNode, cfg.Server.Limits.MaxFrameSize)
	}
	c.reg = cluster.NewRegistry(cfg.FailureThreshold, newPool, c.resyncLoop, cfg.Logger)
	c.repl = replication.New(replication.Config{
		N: cfg.ReplicationN, W: cfg.WriteQuorum, R: cfg.ReadQuorum, RequestTimeout: cfg.RequestTimeout,
	}, c.reg, cfg.Logger)
	c.health = cluster.NewHealthChecker(c.reg, cfg.HealthInterval, cfg.HealthTimeout)
	c.server = transport.NewServer(cfg.Server, c.Handle)

	for _, addr := range cfg.Nodes {
		// Nodes start optimistic; the first health round (in Start) corrects
		// that before any traffic is served.
		c.reg.Add(addr, cluster.Healthy)
		c.ring.Add(addr)
	}
	return c, nil
}

// Start runs an initial health check round and starts the periodic checker.
func (c *Coordinator) Start() {
	c.health.CheckAll(c.ctx)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.health.Run(c.ctx)
	}()
	counts := c.reg.Counts()
	c.log.Printf("coordinator: %d nodes (%d healthy), vnodes=%d N=%d W=%d R=%d",
		len(c.cfg.Nodes), counts[cluster.Healthy], c.cfg.VirtualNodes, c.cfg.ReplicationN, c.cfg.WriteQuorum, c.cfg.ReadQuorum)
}

// Serve serves client connections on ln until Shutdown.
func (c *Coordinator) Serve(ln net.Listener) error { return c.server.Serve(ln) }

// ListenAndServe listens on addr and serves until Shutdown.
func (c *Coordinator) ListenAndServe(addr string) error { return c.server.ListenAndServe(addr) }

// Shutdown stops serving, stops background work and closes node pools.
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
func (c *Coordinator) AddNode(addr string) error {
	if _, err := net.ResolveTCPAddr("tcp", addr); err != nil {
		return fmt.Errorf("bad node address %q: %w", addr, err)
	}
	// Register before adding to the ring so routing never sees an unknown node.
	if !c.reg.Add(addr, cluster.Syncing) {
		return fmt.Errorf("node %s already present", addr)
	}
	c.ring.Add(addr)
	c.log.Printf("node %s added to ring (%d nodes)", addr, c.ring.Len())
	return nil
}

// resyncLoop runs resynchronisation for addr until it succeeds or is
// superseded (the node failed/restarted again, starting a newer generation).
// It is not tracked by c.wg: it observes c.ctx and exits promptly on shutdown.
func (c *Coordinator) resyncLoop(addr string, gen int) {
	replicasFor := func(key string) []string { return c.ring.Replicas(key, c.cfg.ReplicationN) }
	for attempt := 1; ; attempt++ {
		if c.ctx.Err() != nil || !c.reg.SyncStillWanted(addr, gen) {
			return
		}
		err := c.repl.Resync(c.ctx, addr, gen, replicasFor)
		if err == nil {
			c.reg.MarkSynced(addr, gen)
			return
		}
		if errors.Is(err, replication.ErrSyncSuperseded) {
			return
		}
		c.log.Printf("resync of %s failed (attempt %d): %v", addr, attempt, err)
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(min(time.Duration(attempt)*500*time.Millisecond, 5*time.Second)):
		}
	}
}

// Handle dispatches one client request.
func (c *Coordinator) Handle(ctx context.Context, req *protocol.Request) *protocol.Response {
	c.requests.Add(1)
	start := time.Now()
	var resp *protocol.Response
	switch req.Op {
	case protocol.OpGet:
		c.gets.Add(1)
		resp = c.repl.Read(ctx, c.replicas(req.Key), req.Key)
	case protocol.OpPut:
		c.puts.Add(1)
		resp = c.repl.Write(ctx, c.replicas(req.Key), protocol.OpPut, req.Key, req.Value)
	case protocol.OpDelete:
		c.deletes.Add(1)
		resp = c.repl.Write(ctx, c.replicas(req.Key), protocol.OpDelete, req.Key, nil)
	case protocol.OpPing:
		h, _ := c.Health()
		resp = jsonResponse(h)
	case protocol.OpStats:
		resp = jsonResponse(c.Stats(ctx, true))
	case protocol.OpLocate:
		resp = jsonResponse(c.Locate(req.Key))
	default:
		resp = protocol.ErrorResponse(protocol.StatusBadRequest, "coordinator does not support %s", req.Op)
	}
	if h, ok := c.latency[req.Op]; ok {
		h.Record(time.Since(start))
	}
	switch resp.Status {
	case protocol.StatusNotFound:
		c.notFound.Add(1)
	case protocol.StatusOK:
	default:
		c.errors.Add(1)
	}
	return resp
}

func (c *Coordinator) replicas(key string) []string {
	return c.ring.Replicas(key, c.cfg.ReplicationN)
}

func jsonResponse(v any) *protocol.Response {
	b, err := json.Marshal(v)
	if err != nil {
		return protocol.ErrorResponse(protocol.StatusError, "marshal: %v", err)
	}
	return &protocol.Response{Status: protocol.StatusOK, Value: b}
}

// Location describes where a key lives.
type Location struct {
	Key      string            `json:"key"`
	Hash     uint64            `json:"hash"`
	Replicas []ReplicaLocation `json:"replicas"`
}

// ReplicaLocation is one member of a key's replica set.
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
		if i == 0 {
			rl.Role = "primary"
		}
		if m, ok := c.reg.Get(addr); ok {
			rl.State = m.State()
		}
		loc.Replicas = append(loc.Replicas, rl)
	}
	return loc
}

// HealthDoc is the coordinator's health document.
type HealthDoc struct {
	Status         string `json:"status"` // ok | degraded | down
	HealthyNodes   int    `json:"healthy_nodes"`
	UnhealthyNodes int    `json:"unhealthy_nodes"`
	SyncingNodes   int    `json:"syncing_nodes"`
}

// Health reports "ok" when all nodes are healthy, "degraded" when some are
// not, and "down" when none are.
func (c *Coordinator) Health() (HealthDoc, bool) {
	counts := c.reg.Counts()
	h := HealthDoc{Status: "ok", HealthyNodes: counts[cluster.Healthy], UnhealthyNodes: counts[cluster.Unhealthy], SyncingNodes: counts[cluster.Syncing]}
	switch {
	case h.HealthyNodes == 0:
		h.Status = "down"
	case h.UnhealthyNodes+h.SyncingNodes > 0:
		h.Status = "degraded"
	}
	return h, h.HealthyNodes > 0
}

// Stats is the coordinator's statistics document.
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
type CacheTotals struct {
	Hits      int64   `json:"hits"`
	Misses    int64   `json:"misses"`
	Evictions int64   `json:"evictions"`
	HitRate   float64 `json:"hit_rate"`
}

// NodeReport is a member's status plus (if reachable) its own stats.
type NodeReport struct {
	cluster.MemberStatus
	RingShare float64     `json:"ring_share"`
	Stats     *node.Stats `json:"stats,omitempty"`
	StatsErr  string      `json:"stats_error,omitempty"`
}

// Stats returns coordinator statistics; with includeNodes it also queries
// every node's stats concurrently (bounded by the health timeout).
func (c *Coordinator) Stats(ctx context.Context, includeNodes bool) Stats {
	health, _ := c.Health()
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
	for op, h := range c.latency {
		s.Latency[op.String()] = h.Snapshot()
	}
	shares := c.ring.Shares()
	members := c.reg.Snapshot()
	s.Nodes = make([]NodeReport, len(members))
	var wg sync.WaitGroup
	for i, ms := range members {
		s.Nodes[i] = NodeReport{MemberStatus: ms, RingShare: shares[ms.Addr]}
		if !includeNodes || ms.State == cluster.Unhealthy {
			continue
		}
		wg.Add(1)
		go func(r *NodeReport) {
			defer wg.Done()
			ns, err := c.fetchNodeStats(ctx, r.Addr)
			if err != nil {
				r.StatsErr = err.Error()
				return
			}
			r.Stats = ns
		}(&s.Nodes[i])
	}
	wg.Wait()
	sort.Slice(s.Nodes, func(i, j int) bool { return s.Nodes[i].Addr < s.Nodes[j].Addr })
	for _, n := range s.Nodes {
		if n.Stats != nil {
			s.Cache.Hits += n.Stats.Cache.Hits
			s.Cache.Misses += n.Stats.Cache.Misses
			s.Cache.Evictions += n.Stats.Cache.Evictions
		}
	}
	if t := s.Cache.Hits + s.Cache.Misses; t > 0 {
		s.Cache.HitRate = float64(s.Cache.Hits) / float64(t)
	}
	return s
}

func (c *Coordinator) fetchNodeStats(ctx context.Context, addr string) (*node.Stats, error) {
	m, ok := c.reg.Get(addr)
	if !ok {
		return nil, fmt.Errorf("unknown node %s", addr)
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.HealthTimeout)
	defer cancel()
	resp, err := m.Pool.Do(ctx, &protocol.Request{Op: protocol.OpStats})
	if err != nil {
		return nil, err
	}
	if err := resp.Err(); err != nil {
		return nil, err
	}
	var ns node.Stats
	if err := json.Unmarshal(resp.Value, &ns); err != nil {
		return nil, fmt.Errorf("decode stats from %s: %w", addr, err)
	}
	return &ns, nil
}
