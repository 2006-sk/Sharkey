// Package node implements a storage node: a TCP server in front of an LRU
// cache and a storage engine.
package node

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"distkv/internal/cache"
	"distkv/internal/clock"
	"distkv/internal/protocol"
	"distkv/internal/storage"
	"distkv/internal/transport"
)

// Config configures a node.
type Config struct {
	// CacheCapacity is the LRU size in entries; 0 disables the cache.
	CacheCapacity int
	Server        transport.ServerConfig
	Logger        *log.Logger
}

// DefaultConfig returns defaults: a 10,000-entry cache.
func DefaultConfig() Config {
	return Config{
		CacheCapacity: cache.DefaultCapacity,
		Server:        transport.DefaultServerConfig(),
		Logger:        log.Default(),
	}
}

// DefaultScanPageBytes is the scan page size used when a request doesn't
// specify one.
const DefaultScanPageBytes = 512 << 10

// numKeyLocks is the number of striped key locks; see Node.keyLock.
const numKeyLocks = 256

// Node is a storage node.
type Node struct {
	cfg    Config
	store  storage.Engine
	cache  *cache.Cache // nil when disabled
	clock  *clock.Clock
	bootID string
	start  time.Time
	server *transport.Server
	log    *log.Logger

	// keyLocks serialise the "storage + cache" update for a key so the cache
	// can never hold a value older than storage. Without them this
	// interleaving leaves a stale entry cached forever:
	//
	//	GET miss: read v1 from storage
	//	PUT:      write v2 to storage, update cache (not cached yet: no-op)
	//	GET miss: populate cache with v1   <- stale
	//
	// Cache hits don't take the lock; they read the cache's own mutex only.
	// Striping (hash(key) % 256) bounds memory while keeping contention low.
	keyLocks [numKeyLocks]sync.Mutex

	requests, gets, puts, deletes, scans, errors atomic.Int64
	replicationOps, syncOps, staleWrites         atomic.Int64
}

// New creates a node with an in-memory storage engine.
func New(cfg Config) *Node {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	cfg.Server.Logger = cfg.Logger
	// Node handlers never block on I/O, so run them inline when a
	// connection is idle (saves a goroutine handoff per request).
	cfg.Server.InlineWhenIdle = true
	n := &Node{
		cfg:    cfg,
		store:  storage.NewMemory(),
		clock:  clock.New(),
		bootID: newBootID(),
		start:  time.Now(),
		log:    cfg.Logger,
	}
	if cfg.CacheCapacity > 0 {
		n.cache = cache.New(cfg.CacheCapacity)
	}
	n.server = transport.NewServer(cfg.Server, n.Handle)
	return n
}

// newBootID returns a random ID identifying this process incarnation. The
// coordinator compares it across health checks: a changed boot ID means the
// node restarted and (being in-memory) lost its data, even if the restart
// was too quick for any health check to fail.
func newBootID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))
	}
	return hex.EncodeToString(b[:])
}

// Serve serves requests on ln until Shutdown.
func (n *Node) Serve(ln net.Listener) error { return n.server.Serve(ln) }

// ListenAndServe listens on addr and serves until Shutdown.
func (n *Node) ListenAndServe(addr string) error { return n.server.ListenAndServe(addr) }

// Shutdown gracefully stops the TCP server.
func (n *Node) Shutdown(ctx context.Context) error { return n.server.Shutdown(ctx) }

// BootID returns this incarnation's boot ID.
func (n *Node) BootID() string { return n.bootID }

func (n *Node) keyLock(key string) *sync.Mutex {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return &n.keyLocks[h%numKeyLocks]
}

// Handle dispatches one request. It is the transport.Handler for the node.
func (n *Node) Handle(_ context.Context, req *protocol.Request) *protocol.Response {
	n.requests.Add(1)
	var resp *protocol.Response
	switch req.Op {
	case protocol.OpGet:
		n.gets.Add(1)
		resp = n.get(req.Key)
	case protocol.OpPut:
		n.puts.Add(1)
		resp = n.write(req, storage.Entry{Value: req.Value})
	case protocol.OpDelete:
		n.deletes.Add(1)
		resp = n.write(req, storage.Entry{Tombstone: true})
	case protocol.OpPing:
		resp = n.jsonResponse(n.Health())
	case protocol.OpStats:
		resp = n.jsonResponse(n.Stats())
	case protocol.OpScan:
		n.scans.Add(1)
		resp = n.scan(req)
	case protocol.OpApplyBatch:
		resp = n.applyBatch(req)
	default:
		resp = protocol.ErrorResponse(protocol.StatusBadRequest, "node does not support %s", req.Op)
	}
	if resp.Status == protocol.StatusError || resp.Status == protocol.StatusBadRequest {
		n.errors.Add(1)
	}
	return resp
}

// get implements the read path: cache -> storage -> populate cache.
func (n *Node) get(key string) *protocol.Response {
	if n.cache != nil {
		if v, ver, ok := n.cache.Get(key); ok {
			return &protocol.Response{Status: protocol.StatusOK, Version: ver, Value: v}
		}
		mu := n.keyLock(key)
		mu.Lock()
		defer mu.Unlock()
	}
	e, ok := n.store.Get(key)
	switch {
	case !ok:
		return &protocol.Response{Status: protocol.StatusNotFound}
	case e.Tombstone:
		// The version lets the coordinator order this delete against
		// values held by other replicas.
		return &protocol.Response{Status: protocol.StatusNotFound, Version: e.Version}
	}
	if n.cache != nil {
		n.cache.Put(key, e.Value, e.Version)
	}
	return &protocol.Response{Status: protocol.StatusOK, Version: e.Version, Value: e.Value}
}

// write applies a PUT or DELETE (tombstone) with last-writer-wins semantics.
func (n *Node) write(req *protocol.Request, e storage.Entry) *protocol.Response {
	switch {
	case req.Flags&protocol.FlagSync != 0:
		n.syncOps.Add(1)
	case req.Flags&protocol.FlagReplica != 0:
		n.replicationOps.Add(1)
	}
	e.Version = req.Version
	if e.Version == 0 {
		// Unversioned write (a client talking to the node directly).
		e.Version = n.clock.Next()
	}
	cur := n.apply(req.Key, e)
	// A stale write is still acknowledged: this replica already reflects a
	// later state, which is what the writer needs to know.
	return &protocol.Response{Status: protocol.StatusOK, Version: cur.Version}
}

// apply stores e for key (last-writer-wins) and keeps the cache coherent.
func (n *Node) apply(key string, e storage.Entry) storage.Entry {
	n.clock.Observe(e.Version)
	mu := n.keyLock(key)
	mu.Lock()
	applied, cur := n.store.Apply(key, e)
	if n.cache != nil && applied {
		if e.Tombstone {
			n.cache.Delete(key)
		} else {
			n.cache.Update(key, e.Value, e.Version)
		}
	}
	mu.Unlock()
	if !applied {
		n.staleWrites.Add(1)
	}
	return cur
}

// applyBatch applies a batch of versioned entries sent by resync. Each entry
// is applied independently with last-writer-wins, exactly as if it had
// arrived as its own PUT/DELETE, so batching changes cost, not semantics.
func (n *Node) applyBatch(req *protocol.Request) *protocol.Response {
	page, err := protocol.DecodeScanPage(req.Value)
	if err != nil {
		return protocol.ErrorResponse(protocol.StatusBadRequest, "apply batch: %v", err)
	}
	for _, e := range page.Entries {
		if e.Key == "" || e.Version == 0 {
			return protocol.ErrorResponse(protocol.StatusBadRequest, "apply batch: entries need a key and a version")
		}
	}
	for _, e := range page.Entries {
		n.apply(e.Key, storage.Entry{Value: e.Value, Version: e.Version, Tombstone: e.Tombstone})
	}
	n.syncOps.Add(int64(len(page.Entries)))
	return &protocol.Response{Status: protocol.StatusOK}
}

func (n *Node) scan(req *protocol.Request) *protocol.Response {
	maxBytes := DefaultScanPageBytes
	if len(req.Value) == 4 {
		maxBytes = int(binary.BigEndian.Uint32(req.Value))
	}
	// Leave headroom for the page encoding within the frame limit.
	maxBytes = min(maxBytes, n.cfg.Server.Limits.MaxFrameSize/2)
	kvs, next, err := n.store.Scan([]byte(req.Key), maxBytes)
	if err != nil {
		return protocol.ErrorResponse(protocol.StatusBadRequest, "scan: %v", err)
	}
	page := &protocol.ScanPage{NextCursor: next, Entries: make([]protocol.ScanEntry, len(kvs))}
	for i, kv := range kvs {
		page.Entries[i] = protocol.ScanEntry{Key: kv.Key, Value: kv.Value, Version: kv.Version, Tombstone: kv.Tombstone}
	}
	return &protocol.Response{Status: protocol.StatusOK, Value: protocol.EncodeScanPage(page)}
}

func (n *Node) jsonResponse(v any) *protocol.Response {
	b, err := json.Marshal(v)
	if err != nil {
		return protocol.ErrorResponse(protocol.StatusError, "marshal: %v", err)
	}
	return &protocol.Response{Status: protocol.StatusOK, Value: b}
}

// Health is the node's health document (returned by PING and /health).
type Health struct {
	Status string `json:"status"`
	BootID string `json:"boot_id"`
	Keys   int    `json:"keys"`
}

// Health returns the health document.
func (n *Node) Health() Health {
	return Health{Status: "ok", BootID: n.bootID, Keys: n.store.Stats().Keys}
}

// Stats is the node's statistics document.
type Stats struct {
	BootID         string                `json:"boot_id"`
	UptimeSeconds  float64               `json:"uptime_seconds"`
	Requests       int64                 `json:"requests"`
	Gets           int64                 `json:"gets"`
	Puts           int64                 `json:"puts"`
	Deletes        int64                 `json:"deletes"`
	Scans          int64                 `json:"scans"`
	Errors         int64                 `json:"errors"`
	ReplicationOps int64                 `json:"replication_ops"`
	SyncOps        int64                 `json:"sync_ops"`
	StaleWrites    int64                 `json:"stale_writes_ignored"`
	CacheEnabled   bool                  `json:"cache_enabled"`
	Cache          cache.Stats           `json:"cache"`
	Storage        storage.Stats         `json:"storage"`
	Connections    transport.ServerStats `json:"connections"`
	Writes         transport.WriteStats  `json:"write_coalescing"`
}

// Stats returns a snapshot of the node's counters.
func (n *Node) Stats() Stats {
	s := Stats{
		BootID:         n.bootID,
		UptimeSeconds:  time.Since(n.start).Seconds(),
		Requests:       n.requests.Load(),
		Gets:           n.gets.Load(),
		Puts:           n.puts.Load(),
		Deletes:        n.deletes.Load(),
		Scans:          n.scans.Load(),
		Errors:         n.errors.Load(),
		ReplicationOps: n.replicationOps.Load(),
		SyncOps:        n.syncOps.Load(),
		StaleWrites:    n.staleWrites.Load(),
		CacheEnabled:   n.cache != nil,
		Storage:        n.store.Stats(),
		Connections:    n.server.Stats(),
		Writes:         transport.ReadWriteStats(),
	}
	if n.cache != nil {
		s.Cache = n.cache.Stats()
	}
	return s
}
