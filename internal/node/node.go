// Package node implements a storage node: a TCP server in front of an LRU
// cache and a storage engine.
//
// # Where a node sits in the system
//
// Clients never talk to nodes directly in normal operation. The coordinator hashes each
// key onto the consistent-hash ring, picks N=3 distinct nodes (the primary plus two
// replicas), and sends them requests over the binary TCP protocol. A node is therefore a
// deliberately "dumb" component: it does not know about the ring, other nodes, quorums or
// health states. It only knows how to:
//
//   - serve reads (GET) from its cache or its storage,
//   - apply versioned writes (PUT, DELETE-as-tombstone) with last-writer-wins,
//   - page through its whole data set (SCAN) so the coordinator can copy it to a
//     recovering peer, and apply such copies in bulk (APPLY_BATCH),
//   - report its health (PING, including a boot ID) and statistics (STATS).
//
// All distributed logic (replication, fallback, repair, resync) lives in the
// coordinator. Keeping nodes simple is what makes them easy to reason about: every
// operation on a node is local, and the only rule that matters for correctness across
// replicas is "the highest version wins".
//
// # The layers inside one node
//
//	      request (from transport.Server)
//	                 |
//	              Handle      dispatch by opcode, count, return a Response
//	                 |
//	 +---------------+----------------+
//	 |                                |
//	get (read path)            write / applyBatch (write path)
//	 |  1. cache hit? return          |  1. take the key's stripe lock
//	 |  2. take stripe lock           |  2. storage.Apply (LWW by version)
//	 |  3. storage.Get                |  3. update-in-place or invalidate cache
//	 |  4. populate cache             |
//	 +---------------+----------------+
//	                 |
//	 cache.Cache (LRU, one mutex)   storage.Memory (64 shards, RWMutex each)
//
// The cache is a read-through cache: reads fill it, writes never allocate in it (see
// Node.apply for why). The 256 striped key locks keep the cache coherent with storage
// (see the keyLocks field).
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
//
// It is a plain value type so callers (cmd/node, tests) can start from DefaultConfig and
// override only the fields they care about.
//
// Fields (documented here, above the block, so the field list stays one aligned group):
//
//   - Server configures the TCP server: frame-size limits, idle and write timeouts,
//     connection caps and per-connection in-flight limits. New overrides two of its
//     fields (Logger and InlineWhenIdle).
//   - Logger receives server-level log lines. New substitutes log.Default() for nil.
type Config struct {
	// CacheCapacity is the LRU size in entries; 0 disables the cache.
	//
	// With the cache disabled every GET goes straight to storage and no stripe lock is
	// needed on the read path (there is no second copy of the data that could go stale).
	CacheCapacity int
	Server        transport.ServerConfig
	Logger        *log.Logger
}

// DefaultConfig returns defaults: a 10,000-entry cache.
//
// cache.DefaultCapacity is 10,000 entries per node (the README's `--cache-capacity`
// default), and the server config comes from transport.DefaultServerConfig.
func DefaultConfig() Config {
	return Config{
		CacheCapacity: cache.DefaultCapacity,
		Server:        transport.DefaultServerConfig(),
		Logger:        log.Default(),
	}
}

// DefaultScanPageBytes is the scan page size used when a request doesn't
// specify one.
//
// 512 << 10 is 512 KiB of key+value data. The coordinator's resync normally asks for its
// own, smaller page size (the README cites 256 KB pages) by putting a 4-byte size in the
// request; this constant is only the fallback. Either way the effective size is also
// capped at half the maximum frame size (see Node.scan).
const DefaultScanPageBytes = 512 << 10

// numKeyLocks is the number of striped key locks; see Node.keyLock.
//
// A per-key mutex map would be exact but would grow with the key space and need its own
// locking and garbage collection. A fixed array of 256 mutexes costs a few kilobytes and
// never grows. Two different keys that hash to the same stripe merely serialise with each
// other (a false conflict), which is harmless for correctness and rare enough to be cheap.
const numKeyLocks = 256

// Node is a storage node.
//
// A Node is safe for concurrent use: the transport server calls Handle from many
// goroutines at once (one reader goroutine per connection, plus extra goroutines when a
// connection has several requests in flight).
type Node struct {
	// The first group of fields, in order:
	//
	//   cfg     the configuration the node was built with (after New filled in
	//           defaults); scan reads cfg.Server.Limits.MaxFrameSize from it.
	//   store   the authoritative data: a sharded in-memory map of versioned entries,
	//           including tombstones. It is NOT durable; a process restart loses
	//           everything, which is exactly why bootID exists.
	//   cache   the LRU read cache in front of store. It only ever holds copies of
	//           live (non-tombstone) values, each tagged with its version.
	//   clock   issues versions for unversioned writes (a client talking to the node
	//           directly, as tests do). Writes coming through the coordinator already
	//           carry a version from the coordinator's clock; apply feeds those into
	//           clock.Observe so any version this node later issues is larger than
	//           every version it has seen.
	//   bootID  a random identifier for this process incarnation, reported by PING.
	//           The coordinator's health checker compares it across checks to detect
	//           restarts.
	//   start   the process start time, used for UptimeSeconds in Stats.
	//   server  the TCP transport that decodes frames and calls Handle.
	//   log     the logger shared with the server.
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
	// Why "forever"? Writes use update-in-place (cache.Update) and the cache has no TTL,
	// so nothing would ever evict or correct that v1 until the next write to the key or
	// until LRU eviction pushes it out. Every GET in the meantime is a cache hit that
	// returns the old value, even though storage holds v2.
	//
	// With the lock, the GET miss path (storage.Get + cache.Put) and the write path
	// (storage.Apply + cache.Update/Delete) for the same key run one at a time. So a
	// populate either happens entirely before the write (and the write then updates the
	// freshly cached entry) or entirely after it (and reads v2 from storage).
	//
	// Cache hits don't take the lock; they read the cache's own mutex only.
	// Striping (hash(key) % 256) bounds memory while keeping contention low.
	// TestCacheNeverStaleUnderConcurrency hammers exactly this interleaving.
	keyLocks [numKeyLocks]sync.Mutex

	// Operation counters, reported by Stats. They are atomics rather than fields under a
	// mutex so that counting never adds lock contention to the hot path and a STATS
	// request can read them without stopping traffic.
	//
	//   requests        every request Handle sees, of any opcode
	//   gets, puts, deletes, scans
	//                   per-opcode counts
	//   errors          responses with StatusError or StatusBadRequest
	//   replicationOps  writes marked FlagReplica (sent to a non-primary replica)
	//   syncOps         entries written by anti-entropy resync (FlagSync writes and
	//                   every entry of an APPLY_BATCH)
	//   staleWrites     writes rejected by last-writer-wins because this node already
	//                   held an equal or newer version (reported as
	//                   "stale_writes_ignored")
	requests, gets, puts, deletes, scans, errors atomic.Int64
	replicationOps, syncOps, staleWrites         atomic.Int64
}

// New creates a node with an in-memory storage engine.
//
// It does not start listening; call Serve or ListenAndServe afterwards.
func New(cfg Config) *Node {
	// Tolerate a zero-value Config: never leave a nil logger to crash on later.
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	// The server logs through the same logger as the node.
	cfg.Server.Logger = cfg.Logger
	// Node handlers never block on I/O, so run them inline when a
	// connection is idle (saves a goroutine handoff per request).
	//
	// "Idle" means nothing else is in flight or already buffered on that connection. A
	// node handler only touches memory (map, cache, counters), so running it on the
	// reader goroutine can't stall the connection for long. The coordinator must NOT do
	// this: its handlers wait on network calls to nodes and would block the next
	// request multiplexed on the same connection.
	cfg.Server.InlineWhenIdle = true
	// Memory is the only Engine today; the interface is the seam for a durable one.
	// A fresh random boot ID per process is how a restart becomes visible.
	n := &Node{
		cfg:    cfg,
		store:  storage.NewMemory(),
		clock:  clock.New(),
		bootID: newBootID(),
		start:  time.Now(),
		log:    cfg.Logger,
	}
	// A nil cache means "disabled"; every cache access below checks n.cache != nil.
	if cfg.CacheCapacity > 0 {
		n.cache = cache.New(cfg.CacheCapacity)
	}
	// The server calls n.Handle for every decoded request (method value as a
	// transport.Handler).
	n.server = transport.NewServer(cfg.Server, n.Handle)
	return n
}

// newBootID returns a random ID identifying this process incarnation. The
// coordinator compares it across health checks: a changed boot ID means the
// node restarted and (being in-memory) lost its data, even if the restart
// was too quick for any health check to fail.
//
// Without a boot ID, a node killed and restarted between two 2-second health checks
// would look continuously HEALTHY while actually being empty, and reads routed to it
// would return NOT_FOUND for keys that exist. With it, the coordinator moves the node to
// SYNCING and resyncs it (TestFastRestartDetectedByBootID covers this).
//
// Eight random bytes (16 hex characters) make an accidental collision between two
// incarnations practically impossible.
func newBootID() string {
	var b [8]byte
	// crypto/rand essentially never fails; if it does, fall back to the current time in
	// nanoseconds, which still differs between two process starts.
	if _, err := rand.Read(b[:]); err != nil {
		binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))
	}
	return hex.EncodeToString(b[:])
}

// Serve serves requests on ln until Shutdown.
//
// Taking a listener (rather than an address) lets tests listen on 127.0.0.1:0 and learn
// the chosen port before serving.
func (n *Node) Serve(ln net.Listener) error { return n.server.Serve(ln) }

// ListenAndServe listens on addr and serves until Shutdown.
func (n *Node) ListenAndServe(addr string) error { return n.server.ListenAndServe(addr) }

// Shutdown gracefully stops the TCP server.
//
// Because storage is in memory, the data dies with the process; there is nothing to
// flush.
func (n *Node) Shutdown(ctx context.Context) error { return n.server.Shutdown(ctx) }

// BootID returns this incarnation's boot ID.
func (n *Node) BootID() string { return n.bootID }

// keyLock returns the stripe mutex that guards key's "storage + cache" update.
//
// It hashes the key with 32-bit FNV-1a (offset basis 2166136261, prime 16777619) and
// takes the result modulo 256. The same key always maps to the same mutex, which is the
// only property correctness needs: every read-miss and every write for one key contends
// on one lock. The hash is inlined (no hash.Hash allocation) because it runs on every
// write and every cache miss.
func (n *Node) keyLock(key string) *sync.Mutex {
	// FNV-1a offset basis.
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		// FNV-1a: XOR in the byte first, then multiply by the FNV prime.
		h ^= uint32(key[i])
		h *= 16777619
	}
	// Return a pointer into the array: copying a sync.Mutex would break it.
	return &n.keyLocks[h%numKeyLocks]
}

// Handle dispatches one request. It is the transport.Handler for the node.
//
// It is called concurrently from the server's goroutines, so everything it touches is
// either atomic (counters) or protected by its own lock (cache, storage shards, key
// stripes). The context is ignored because no node operation blocks: each one finishes
// in memory in bounded time, so there is nothing to cancel.
func (n *Node) Handle(_ context.Context, req *protocol.Request) *protocol.Response {
	n.requests.Add(1)
	var resp *protocol.Response
	switch req.Op {
	case protocol.OpGet:
		n.gets.Add(1)
		resp = n.get(req.Key)
	case protocol.OpPut:
		n.puts.Add(1)
		// A PUT stores the value; its version comes from the request (see write).
		resp = n.write(req, storage.Entry{Value: req.Value})
	case protocol.OpDelete:
		n.deletes.Add(1)
		// A DELETE is a versioned write of a tombstone, not a map delete. The tombstone
		// remembers "deleted at version V", so an older PUT that arrives late (a delayed
		// replica write, a retry, a resync copy) loses to it under last-writer-wins
		// instead of resurrecting the key.
		resp = n.write(req, storage.Entry{Tombstone: true})
	case protocol.OpPing:
		// Health check: status, boot ID and live key count, as JSON.
		resp = n.jsonResponse(n.Health())
	case protocol.OpStats:
		// Counter snapshot, as JSON; the coordinator sums these across nodes.
		resp = n.jsonResponse(n.Stats())
	case protocol.OpScan:
		n.scans.Add(1)
		// One page of the node's data, used by the coordinator's resync.
		resp = n.scan(req)
	case protocol.OpApplyBatch:
		// A page of entries copied from a peer by resync.
		resp = n.applyBatch(req)
	default:
		// Coordinator-only or unknown opcodes are rejected rather than ignored, so a
		// misrouted request fails loudly.
		resp = protocol.ErrorResponse(protocol.StatusBadRequest, "node does not support %s", req.Op)
	}
	// NOT_FOUND is a normal answer, not an error; only server and request errors count.
	if resp.Status == protocol.StatusError || resp.Status == protocol.StatusBadRequest {
		n.errors.Add(1)
	}
	return resp
}

// get implements the read path: cache -> storage -> populate cache.
//
// This is a read-through cache: the caller only ever asks get, and get decides whether
// the answer comes from the cache or from storage, filling the cache on the way.
//
//	hit : cache.Get                                    (cache mutex only)
//	miss: lock stripe -> store.Get -> cache.Put -> unlock
//
// The miss path holds the key's stripe lock from before the storage read until after the
// cache populate (the deferred Unlock runs when get returns). That is what prevents the
// stale-populate race described on Node.keyLocks.
func (n *Node) get(key string) *protocol.Response {
	if n.cache != nil {
		// Fast path: a hit returns immediately without any stripe lock. It cannot be
		// stale, because every write that changes this key updates or removes the cached
		// entry while holding the stripe lock.
		if v, ver, ok := n.cache.Get(key); ok {
			return &protocol.Response{Status: protocol.StatusOK, Version: ver, Value: v}
		}
		// Miss: serialise with writers of this key before touching storage, so no write
		// can slip in between our storage read and our cache populate.
		mu := n.keyLock(key)
		mu.Lock()
		defer mu.Unlock()
	}
	// With the cache disabled we reach here without a stripe lock. That is fine: the
	// storage shard's RWMutex makes the Get atomic, and there is no cache to go stale.
	e, ok := n.store.Get(key)
	switch {
	case !ok:
		// Never written on this node: NOT_FOUND with version 0.
		return &protocol.Response{Status: protocol.StatusNotFound}
	case e.Tombstone:
		// The version lets the coordinator order this delete against
		// values held by other replicas.
		//
		// For example, with a read quorum one replica may answer "NOT_FOUND at v7" and
		// another "value at v5". The tombstone's version tells the coordinator the
		// delete is newer, so it returns NOT_FOUND and can read-repair the stale replica.
		// Tombstones are never cached: the cache holds only live values, and a miss for
		// a deleted key simply re-reads the tombstone from storage.
		return &protocol.Response{Status: protocol.StatusNotFound, Version: e.Version}
	}
	// Populate the cache (this is the "read-through" fill). Put may evict the least
	// recently used entry if the cache is full.
	if n.cache != nil {
		n.cache.Put(key, e.Value, e.Version)
	}
	return &protocol.Response{Status: protocol.StatusOK, Version: e.Version, Value: e.Value}
}

// write applies a PUT or DELETE (tombstone) with last-writer-wins semantics.
//
// e carries the payload (a value, or Tombstone=true); write fills in the version and
// hands it to apply. The response always carries the version this node now holds for
// the key, which may be newer than the one just sent (if the write was stale).
func (n *Node) write(req *protocol.Request, e storage.Entry) *protocol.Response {
	// Classify the write for Stats. The coordinator sets FlagSync on writes pushed by
	// resync and FlagReplica on copies sent to non-primary replicas; a write to the
	// primary has neither flag. The flags only affect counters, never behaviour: every
	// write goes through the same last-writer-wins apply.
	switch {
	case req.Flags&protocol.FlagSync != 0:
		n.syncOps.Add(1)
	case req.Flags&protocol.FlagReplica != 0:
		n.replicationOps.Add(1)
	}
	// Normally the coordinator stamped the write with a version from its monotonic
	// clock. All replicas receive the same version for the same write, which is what
	// lets them converge on the same value regardless of arrival order.
	e.Version = req.Version
	if e.Version == 0 {
		// Unversioned write (a client talking to the node directly).
		//
		// Version 0 is never valid (it would lose to everything), so issue one from the
		// node's own clock. That clock has observed every version applied here, so the
		// new version is newer than anything this node holds and the write wins.
		e.Version = n.clock.Next()
	}
	cur := n.apply(req.Key, e)
	// A stale write is still acknowledged: this replica already reflects a
	// later state, which is what the writer needs to know.
	//
	// Returning an error instead would make the coordinator count a perfectly healthy
	// replica as failing the quorum, only because a newer write overtook this one. The
	// returned version (cur.Version) lets the caller see what actually won.
	return &protocol.Response{Status: protocol.StatusOK, Version: cur.Version}
}

// apply stores e for key (last-writer-wins) and keeps the cache coherent.
//
// It is the single write path shared by PUT, DELETE and APPLY_BATCH, so every way data
// can enter a node obeys the same two rules:
//
//  1. Last-writer-wins: storage.Apply keeps e only if e.Version is strictly greater than
//     the stored version. Equal versions are rejected too, which makes a replayed write
//     (a transport retry, a resync copy of data the node already has) a harmless no-op.
//     That idempotence is what makes retries and resync safe.
//  2. Cache coherence: if storage changed, the cache is changed under the same stripe
//     lock, so a concurrent GET miss can't cache the old value (see Node.keyLocks).
//
// It returns the entry now stored for key: e itself if it won, otherwise the newer entry
// that beat it.
func (n *Node) apply(key string, e storage.Entry) storage.Entry {
	// Advance the local clock past this version (a hybrid-clock "observe"), so an
	// unversioned write issued later on this node gets a larger version and isn't
	// silently rejected as stale.
	n.clock.Observe(e.Version)
	// Take the key's stripe lock around "storage + cache" as one unit.
	mu := n.keyLock(key)
	mu.Lock()
	// Last-writer-wins compare-and-store, atomic under the storage shard's lock.
	applied, cur := n.store.Apply(key, e)
	// Only touch the cache if storage actually changed. A stale write must not touch the
	// cache either, or it could overwrite a newer cached value.
	if n.cache != nil && applied {
		if e.Tombstone {
			// DELETE invalidates: drop the entry so the next GET misses and reads the
			// tombstone from storage (tombstones are never cached).
			n.cache.Delete(key)
		} else {
			// PUT updates in place, and only if the key is already cached ("no
			// write-allocate"). Two of a key's three replicas take every write but serve
			// reads only on fallback. If writes allocated cache entries, those replicas
			// would fill their caches with keys nobody reads from them, evicting keys
			// that are actually read. The cost is that the first read after a write to
			// an uncached key is a miss. Updating an existing entry (instead of
			// invalidating it) keeps a hot key hot through overwrites.
			n.cache.Update(key, e.Value, e.Version)
		}
	}
	// Release the stripe lock before counting: the counter is atomic and needs no lock.
	mu.Unlock()
	if !applied {
		// This node already held an equal or newer version; the write was ignored.
		n.staleWrites.Add(1)
	}
	return cur
}

// applyBatch applies a batch of versioned entries sent by resync. Each entry
// is applied independently with last-writer-wins, exactly as if it had
// arrived as its own PUT/DELETE, so batching changes cost, not semantics.
//
// During anti-entropy resync the coordinator SCANs each healthy peer page by page,
// keeps the entries whose replica set includes the recovering node, and sends them here
// as one APPLY_BATCH per page instead of one request per key. The payload reuses the
// SCAN page encoding.
//
// Resync runs concurrently with live writes. That is safe because of last-writer-wins:
// if a live write with version 20 has already landed and resync then copies an old
// value with version 12, apply rejects the copy as stale. Tombstones are copied too, so
// the node also learns about deletes it missed while it was down.
//
// The batch is not atomic as a whole (each entry is applied on its own), and it doesn't
// need to be: every entry is individually idempotent and order-independent.
func (n *Node) applyBatch(req *protocol.Request) *protocol.Response {
	// Decode the whole page first; a corrupt payload is rejected before any change.
	page, err := protocol.DecodeScanPage(req.Value)
	if err != nil {
		return protocol.ErrorResponse(protocol.StatusBadRequest, "apply batch: %v", err)
	}
	// Validate every entry before applying any, so a bad batch has no partial effect.
	// Resync entries must carry the version they had on the source node: version 0
	// would lose to any existing data, and an empty key is meaningless.
	for _, e := range page.Entries {
		if e.Key == "" || e.Version == 0 {
			return protocol.ErrorResponse(protocol.StatusBadRequest, "apply batch: entries need a key and a version")
		}
	}
	// Apply each entry through the same LWW + cache-coherent path as a single write.
	for _, e := range page.Entries {
		n.apply(e.Key, storage.Entry{Value: e.Value, Version: e.Version, Tombstone: e.Tombstone})
	}
	// Every entry counts as a sync op, including ones rejected as stale (those are also
	// counted in staleWrites by apply).
	n.syncOps.Add(int64(len(page.Entries)))
	return &protocol.Response{Status: protocol.StatusOK}
}

// scan returns one page of this node's entries (tombstones included), for resync.
//
// Request encoding:
//
//	req.Key   = cursor from the previous page's NextCursor (empty for the first page)
//	req.Value = optional 4-byte big-endian page size in bytes
//
// The response Value is an encoded protocol.ScanPage. An empty NextCursor means the
// scan is complete. Pagination keeps each response bounded (a whole node's data in one
// frame could exceed the frame limit and would hold memory for the full copy), and
// the cursor makes the scan resumable without the node keeping any per-scan state.
//
// A scan is not a consistent snapshot: entries written while it runs may or may not
// appear. Resync tolerates that because those concurrent writes also go to the
// recovering node directly (SYNCING nodes receive writes) and LWW merges both sources.
func (n *Node) scan(req *protocol.Request) *protocol.Response {
	maxBytes := DefaultScanPageBytes
	// Exactly four bytes means "use this page size"; anything else means "default".
	if len(req.Value) == 4 {
		maxBytes = int(binary.BigEndian.Uint32(req.Value))
	}
	// Leave headroom for the page encoding within the frame limit.
	//
	// maxBytes counts roughly key + value bytes (storage adds a small per-entry
	// overhead), but the encoded page also has per-entry headers and the response frame
	// has its own header. Capping at half the maximum frame size keeps the encoded
	// response safely under the limit whatever size the caller asked for.
	maxBytes = min(maxBytes, n.cfg.Server.Limits.MaxFrameSize/2)
	// The cursor travels in the Key field as opaque bytes; storage decodes it (it
	// encodes a shard index plus the last key returned).
	kvs, next, err := n.store.Scan([]byte(req.Key), maxBytes)
	if err != nil {
		// Only a malformed cursor can fail, and that is the caller's fault.
		return protocol.ErrorResponse(protocol.StatusBadRequest, "scan: %v", err)
	}
	// Convert storage entries to wire entries, preserving version and tombstone flag so
	// the receiver can apply them with last-writer-wins.
	page := &protocol.ScanPage{NextCursor: next, Entries: make([]protocol.ScanEntry, len(kvs))}
	for i, kv := range kvs {
		page.Entries[i] = protocol.ScanEntry{Key: kv.Key, Value: kv.Value, Version: kv.Version, Tombstone: kv.Tombstone}
	}
	return &protocol.Response{Status: protocol.StatusOK, Value: protocol.EncodeScanPage(page)}
}

// jsonResponse wraps v, encoded as JSON, in an OK response. PING and STATS use it; the
// data is small and read by humans and by the coordinator, so JSON's readability is worth
// more than the compactness of a binary encoding.
func (n *Node) jsonResponse(v any) *protocol.Response {
	b, err := json.Marshal(v)
	if err != nil {
		return protocol.ErrorResponse(protocol.StatusError, "marshal: %v", err)
	}
	return &protocol.Response{Status: protocol.StatusOK, Value: b}
}

// Health is the node's health document (returned by PING and /health).
//
// Fields:
//
//   - Status is always "ok": answering at all is the health signal. A dead or hung node
//     shows up as a failed or timed-out ping on the coordinator side, not as a status.
//   - BootID identifies this process incarnation; a change means "restarted, data
//     lost".
//   - Keys is the number of live (non-tombstone) keys stored.
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
//
// It is returned by the STATS opcode as JSON; the coordinator's /stats endpoint collects
// one from every node and sums them (for example into a cluster-wide cache hit rate).
//
// Fields:
//
//   - BootID lets a reader tell whether counters reset because the node restarted.
//   - UptimeSeconds is the time since New was called.
//   - Requests counts every request of any opcode; Gets, Puts, Deletes and Scans count
//     requests per opcode.
//   - Errors counts StatusError and StatusBadRequest responses.
//   - ReplicationOps counts writes flagged as replica copies (non-primary writes).
//   - SyncOps counts entries written by resync (FlagSync writes and APPLY_BATCH
//     entries).
//   - StaleWrites counts writes ignored by last-writer-wins. A non-zero value is
//     normal: it shows resync copies or late replica writes being correctly rejected.
//   - CacheEnabled is false when CacheCapacity was 0 (Cache is then all zeros).
//   - Cache holds hits, misses, evictions, size and hit rate from the LRU.
//   - Storage holds live-key and tombstone counts.
//   - Connections holds the TCP server's connection counters.
//   - Writes holds process-wide write-coalescing counters from the transport (how many
//     response frames were sent per write syscall).
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
//
// Each counter is loaded atomically on its own, so the snapshot is not a single
// consistent instant (Requests may be read slightly before Gets, say). For monitoring
// that is fine, and it means Stats never blocks request processing.
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
	// Leave Cache zeroed when the cache is disabled (n.cache is nil).
	if n.cache != nil {
		s.Cache = n.cache.Stats()
	}
	return s
}
