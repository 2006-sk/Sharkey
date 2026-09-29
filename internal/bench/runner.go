package bench

// runner.go is the load generator: it drives a live cluster through the
// coordinator's TCP port and measures what happens.
//
// # Shape of one run (Run)
//
//  1. Open one TCP connection per simulated client.
//  2. Preload (optional): PUT every key once, so later GETs find real data
//     instead of cheap "not found" answers.
//  3. Warm-up (optional): run the real workload for a while WITHOUT recording
//     anything, so the measured phase starts in steady state.
//  4. Snapshot cluster-wide counters (cache hits/misses, write-coalescing).
//  5. Measured phase: every worker loops "pick op, pick key, send, wait for the
//     answer, record latency". In parallel, background goroutines sample the
//     coordinator's health, probe one watched key, and run fault/recovery
//     shell commands at fixed offsets.
//  6. Snapshot the counters again; the difference is what the measured phase
//     caused. Aggregate per-worker samples into exact percentiles.
//
// # Closed-loop load and its main caveat: coordinated omission
//
// Each simulated client has exactly ONE request outstanding: it sends, waits
// for the answer, and only then sends the next. That is a CLOSED-LOOP load
// generator (the classic model: N users each thinking/acting in turn). Its
// strength is realism for interactive clients and a self-limiting load: the
// offered load automatically adapts to what the system can absorb, so you can
// measure maximum throughput without guessing a rate.
//
// Its weakness is COORDINATED OMISSION (Gil Tene's term). If the server stalls
// for 1 s, each client records ONE slow sample of ~1 s and then carries on.
// An OPEN-LOOP generator (fixed arrival rate, e.g. 50k req/s regardless of
// responses) would have wanted to send thousands of requests during that
// second, each of which would have waited part of the stall. The closed-loop
// tool "coordinates" with the server by not sending them, so the stall is
// represented by far fewer samples than a real, rate-driven user population
// would experience, and the tail percentiles (p99, p99.9) UNDERSTATE what such
// users would see during stalls. The README's numbers must be read with that
// in mind: they are "latency as seen by N closed-loop clients". The timeline
// (throughput per 100 ms interval) and the probe (see runProbe) make stalls
// visible even when the percentiles hide them: a stall shows up as a
// throughput dip, and in Test D the SIGSTOP probe's worst latency is ~1 s.
//
// Little's law ties the three quantities together for a closed loop:
// clients ≈ throughput × mean latency. That is why, past saturation, adding
// clients raises latency almost proportionally instead of raising throughput
// (README Test C).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"distkv/internal/client"
	"distkv/internal/coordinator"
	"distkv/internal/metrics"
	"distkv/internal/protocol"
	"distkv/internal/transport"
)

// Config configures a load run.
//
// It is filled from command-line flags by cmd/benchmark and is also embedded
// verbatim in the JSON Result, so every result file records exactly which
// parameters produced it (including the seed actually used). Durations are
// serialised as integer nanoseconds, hence the "_ns" JSON tags; the summarize
// script converts them back.
//
// Fields, grouped by purpose:
//
//   - Label: a free-form name for the run ("scaling-4n-rf3", ...), copied
//     into the result so plots and tables can identify it.
//   - Address: the coordinator's host:port. All traffic goes through it,
//     exactly like a real application's would.
//   - Clients: the number of simulated clients = worker goroutines = TCP
//     connections. Concurrency, not request rate, is the input knob of a
//     closed-loop generator.
//   - Requests / Duration: the two ways to bound the measured phase. A fixed
//     operation count gives a reproducible amount of work (used by tests); a
//     fixed wall-clock time (used by the benchmark suites) samples the same
//     span of time however fast the system is.
//   - Warmup: excluded from every statistic; see Run for why it exists.
//   - ReadRatio: probability that an operation is a GET (the rest are PUTs).
//     0.8 = 80% reads, a typical read-heavy web workload.
//   - Keyspace: the number of distinct keys, KeyName(0..Keyspace-1).
//   - ValueSize: bytes in every PUT value. All PUTs send the same buffer, so
//     generating values costs nothing inside the timed loop.
//   - Distribution / ZipfTheta: select the KeyGen; θ is used for "zipf"
//     (0.99 in the README suites, YCSB's default).
//   - Preload: write every key before measuring, so GETs return real values.
//     A GET of a missing key is a cheaper code path and would flatter results.
//   - RequestTimeout: client-side bound on each request. A request exceeding
//     it counts as an error and its connection is replaced. Run defaults it
//     to 5 s.
//   - Seed: makes the operation mix and key sequence reproducible; worker w
//     uses PCG(Seed, w). 0 means "pick one from the clock", and Run writes the
//     chosen value back so the result file still records it. The suites run
//     repeat r with --seed r, so repeats differ but each can be replayed.
//   - TimelineInterval: width of each timeline bucket (100 ms in the failure
//     suite, 1 s if unset). Per-interval throughput/p99 is what makes a
//     transient stall visible; whole-run averages would smooth it away.
//   - ProbeKey / ProbeInterval: if set, ProbeKey is GET-ed every ProbeInterval
//     on a dedicated connection, a fine-grained view of ONE key's
//     availability (in Test D, the key whose primary node is killed).
//   - FaultAt / FaultCmd / RecoverAt / RecoverCmd: a shell command (e.g.
//     "kill -9 <pid>") run FaultAt after the measured phase starts, and a
//     recovery command (restart, kill -CONT) run at RecoverAt. The benchmark
//     runs them itself so that fault time and traffic are measured on ONE
//     clock; a separate script that sleeps and then kills would introduce an
//     unknown skew between the two timelines.
type Config struct {
	Label          string        `json:"label"`
	Address        string        `json:"address"`
	Clients        int           `json:"clients"`
	Requests       int64         `json:"requests"`    // total measured ops (0 = run for Duration)
	Duration       time.Duration `json:"duration_ns"` // used when Requests == 0
	Warmup         time.Duration `json:"warmup_ns"`   // unmeasured ops before the measured phase
	ReadRatio      float64       `json:"read_ratio"`  // fraction of GETs
	Keyspace       int           `json:"keyspace"`
	ValueSize      int           `json:"value_size"`
	Distribution   string        `json:"distribution"` // uniform | zipf
	ZipfTheta      float64       `json:"zipf_theta"`
	Preload        bool          `json:"preload"` // PUT every key once before measuring
	RequestTimeout time.Duration `json:"request_timeout_ns"`
	Seed           uint64        `json:"seed"`

	TimelineInterval time.Duration `json:"timeline_interval_ns"`
	ProbeKey         string        `json:"probe_key,omitempty"`
	ProbeInterval    time.Duration `json:"probe_interval_ns,omitempty"`
	FaultAt          time.Duration `json:"fault_at_ns,omitempty"`
	FaultCmd         string        `json:"fault_cmd,omitempty"`
	RecoverAt        time.Duration `json:"recover_at_ns,omitempty"`
	RecoverCmd       string        `json:"recover_cmd,omitempty"`
}

// Validate checks the configuration.
//
// Failing fast on nonsense input (0 clients, read ratio 1.5, ...) is better
// than producing a result file full of zeros or NaNs that someone might later
// plot without noticing. The messages name the CLI flags because the usual
// caller is cmd/benchmark.
func (c *Config) Validate() error {
	switch {
	// At least one simulated client is needed to generate any load.
	case c.Clients < 1:
		return errors.New("--clients must be >= 1")
	// Without a request count or a duration the measured phase would never end
	// (or end instantly).
	case c.Requests <= 0 && c.Duration <= 0:
		return errors.New("one of --requests or --duration must be positive")
	// ReadRatio is a probability.
	case c.ReadRatio < 0 || c.ReadRatio > 1:
		return errors.New("--read-ratio must be in [0,1]")
	// Key generators need at least one key.
	case c.Keyspace < 1:
		return errors.New("--keyspace must be >= 1")
	// The server rejects values above its limit; catching it here gives a
	// clear message instead of 100% request errors.
	case c.ValueSize < 0 || c.ValueSize > protocol.DefaultMaxValueSize:
		return fmt.Errorf("--value-size must be in [0,%d]", protocol.DefaultMaxValueSize)
	case c.Distribution != "uniform" && c.Distribution != "zipf":
		return errors.New("--distribution must be uniform or zipf")
	// A fault at t=0 would fire before any baseline exists, making the
	// before/after comparison in faultReport meaningless.
	case c.FaultCmd != "" && c.FaultAt <= 0:
		return errors.New("--fault-cmd requires --fault-at")
	}
	return nil
}

// maxBuckets bounds timeline memory (e.g. 1h at 100ms).
//
// The bucket array is allocated up front (so recording never has to grow it
// or take a lock). Operations that happen after the last bucket are folded
// into it rather than panicking, see timeline.idx.
const maxBuckets = 36000

// bucket holds everything observed during one timeline interval. All fields
// are atomics because every worker goroutine writes into the current bucket
// concurrently; a mutex per bucket would serialise the workers on the hot
// path and itself distort latency.
//
//   - ops counts operations STARTED in this interval (successful or not);
//     errs counts the failed ones.
//   - hist is this interval's latency histogram, allocated lazily on first
//     use. A metrics.Histogram is ~10 KB (1312 atomic counters); allocating
//     all 36,000 eagerly would cost hundreds of MB, while a 25 s run at
//     100 ms only touches ~250 buckets. Per-interval percentiles come from
//     this log-linear histogram (at most ~3% error), which is fine for a time
//     series; the headline percentiles in Result are exact (see summarize).
//   - healthy is the coordinator's healthy-node count last sampled during
//     this interval, or -1 if no health sample landed in it.
type bucket struct {
	ops, errs atomic.Int64
	hist      atomic.Pointer[metrics.Histogram]
	healthy   atomic.Int64
}

// timeline slices the measured phase into fixed-width intervals so that
// transient events (a node dying, a 1 s timeout stall, recovery) can be seen
// over time instead of being averaged away over the whole run.
//
//   - start is the beginning of the measured phase; every timestamp in the
//     result (buckets, probes, fault times) is relative to it.
//   - interval is the bucket width.
//   - buckets is the fixed, preallocated array of intervals.
type timeline struct {
	start    time.Time
	interval time.Duration
	buckets  []bucket
}

// newTimeline preallocates maxBuckets buckets of width interval (1 s if the
// caller passed nothing). The start time is set later, right before the
// measured phase begins, so that preload and warm-up do not shift t=0.
func newTimeline(interval time.Duration) *timeline {
	// A zero or negative interval would cause a division by zero in idx.
	if interval <= 0 {
		interval = time.Second
	}
	t := &timeline{interval: interval, buckets: make([]bucket, maxBuckets)}
	// Mark every bucket "no health sample yet": 0 would be indistinguishable
	// from "the coordinator reported zero healthy nodes".
	for i := range t.buckets {
		t.buckets[i].healthy.Store(-1)
	}
	return t
}

// idx maps a wall-clock instant to its bucket index: elapsed time since start
// divided by the interval (integer division = floor). Anything past the end
// is clamped into the last bucket so a very long run degrades gracefully.
func (t *timeline) idx(at time.Time) int {
	return min(int(at.Sub(t.start)/t.interval), maxBuckets-1)
}

// record adds one finished operation to the bucket in which it STARTED.
// Attributing by start time (not completion time) means a request that was
// sent just before a fault and hung for 1 s is charged to the interval where
// the client issued it, which is the interval a user would blame.
func (t *timeline) record(at time.Time, lat time.Duration, failed bool) {
	b := &t.buckets[t.idx(at)]
	b.ops.Add(1)
	if failed {
		b.errs.Add(1)
	}
	// Lazily create this bucket's histogram. Several workers may see nil at
	// the same moment; CompareAndSwap lets exactly one of them install its
	// histogram, and everyone then Loads the winner. No lock, no lost samples
	// (the losers' freshly allocated histograms are simply garbage).
	h := b.hist.Load()
	if h == nil {
		b.hist.CompareAndSwap(nil, &metrics.Histogram{})
		h = b.hist.Load()
	}
	// Histogram.Record is itself lock-free (a few atomic adds).
	h.Record(lat)
}

// workerStats is PRIVATE to one worker goroutine during the run, so appending
// to it needs no synchronisation. Only after all workers have finished does
// aggregate() merge the workers' stats. This "shard per goroutine, merge at the
// end" pattern keeps the measurement loop free of shared writes (other than
// the timeline's atomics).
//
//   - gets and puts hold EVERY successful operation's latency. Keeping raw
//     samples (instead of a histogram) is what allows exact percentiles. The
//     memory cost is 8 bytes per sample, e.g. ~50k ops/s × 15 s ≈ 750k
//     samples ≈ 6 MB per run, which is affordable.
//   - getErrs/putErrs count failed operations by type; nf counts GETs that
//     succeeded with "not found" (should be 0 after a preload).
//   - errSamples keeps a few distinct error messages with their counts, so
//     the result explains WHAT went wrong without one entry per failure.
type workerStats struct {
	gets, puts           []time.Duration
	getErrs, putErrs, nf int64
	errSamples           map[string]int
}

// noteErr records an error message in the worker's sample map. Messages are
// truncated to 120 bytes (some embed long addresses/values) and at most 5
// distinct messages are kept per worker; a message already present is always
// counted, so the counts of the common errors stay accurate.
func (w *workerStats) noteErr(err string) {
	if len(err) > 120 {
		err = err[:120]
	}
	if _, ok := w.errSamples[err]; ok || len(w.errSamples) < 5 {
		w.errSamples[err]++
	}
}

// healthSample is one observation of the coordinator's healthy-node count.
// The failure analysis uses the timestamps to compute "time from fault to
// detection" and "time from recovery to all-healthy".
//
//   - at is when the answer was received.
//   - healthy is the number of nodes the coordinator currently considers
//     HEALTHY; total is healthy + unhealthy + syncing (all members).
type healthSample struct {
	at      time.Time
	healthy int
	total   int
}

// Run executes a benchmark and returns measured results. Progress messages
// go to log.
//
// ctx cancels the whole run (e.g. Ctrl-C in cmd/benchmark). Errors are
// returned only for setup problems (bad config, cannot connect, preload
// failed); request failures during the run are counted in the Result instead,
// because "how many requests failed during a fault" is itself a measurement.
func Run(ctx context.Context, cfg Config, log io.Writer) (*Result, error) {
	// Reject invalid parameters before touching the network.
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// Choose the key distribution. The generator is built once and shared
	// read-only by all workers (each passes its own RNG to Next).
	var gen KeyGen = Uniform{N: cfg.Keyspace}
	if cfg.Distribution == "zipf" {
		// NewZipf's O(keyspace) precomputation happens here, before any timing.
		z, err := NewZipf(cfg.Keyspace, cfg.ZipfTheta)
		if err != nil {
			return nil, err
		}
		gen = z
	}
	// Default per-request timeout. It is generous (5 s) so that it never cuts
	// off a slow-but-successful request in normal runs; the coordinator's own
	// 1 s node timeout is what the failure tests exercise.
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	// Seed from the clock if none was given. This happens BEFORE cfg is copied
	// into the Result below, so the recorded config contains the real seed and
	// the run can be replayed.
	if cfg.Seed == 0 {
		cfg.Seed = uint64(time.Now().UnixNano())
	}

	// admin is a separate, low-volume client used for control-plane requests
	// (STATS, PING, the probe key's initial PUT). Keeping it off the workers'
	// connections means those requests never queue behind load traffic and
	// never add samples to the measured latency.
	admin := client.New(cfg.Address, client.Options{DialTimeout: 2 * time.Second, RequestTimeout: 10 * time.Second, Conns: 1})
	defer admin.Close()
	// Start filling the result: label, UTC start time, host description
	// (Go version, CPUs, load average at start) and the effective config.
	res := &Result{Label: cfg.Label, StartedAt: time.Now().UTC(), Host: hostInfo(), Config: cfg}
	// Record the cluster's shape (node count, replication factor, vnodes...)
	// as reported by the coordinator, so the result is self-describing. A
	// failure here is not fatal: it only means those fields stay empty.
	if st, err := clusterStats(ctx, admin); err == nil {
		res.ClusterNodes = len(st.Nodes)
		res.Cluster, _ = json.Marshal(st.Config)
	}

	// One dedicated connection per simulated client, like independent users.
	//
	// Why not share a pool? A shared connection would multiplex several
	// clients' requests, batching them into fewer syscalls and making the
	// system look faster than it would for N independent users, each with
	// their own socket. It would also add a pool lock to the measured path.
	// These are plain sequential transport.Conns (one request in flight per
	// connection), which is exactly the closed-loop "one outstanding request
	// per client" model. Note: with hundreds of clients plus the cluster's own
	// sockets, the process needs enough file descriptors (cmd/benchmark and
	// the scripts raise the open-file limit for that reason).
	conns := make([]*transport.Conn, cfg.Clients)
	for i := range conns {
		// maxFrame 0 = protocol default. Dial fails fast (2 s) if the
		// coordinator is not listening.
		c, err := transport.Dial(ctx, cfg.Address, 2*time.Second, 0)
		if err != nil {
			// The deferred cleanup below is not registered yet, but the
			// connections opened so far are reclaimed when the process exits
			// (Run is the whole life of cmd/benchmark).
			return nil, fmt.Errorf("client %d: %w", i, err)
		}
		conns[i] = c
	}
	// Close every connection when Run returns. Workers may have REPLACED their
	// connection after an error (runPhase writes the new one back into
	// conns[w]), so this closes the current ones, not stale ones. nil means the
	// redial was abandoned because ctx was cancelled.
	defer func() {
		for _, c := range conns {
			if c != nil {
				c.Close()
			}
		}
	}()

	// One value buffer shared by every PUT: 'a'..'z' repeated. Building it once
	// keeps allocation out of the timed loop, and printable bytes make values
	// easy to eyeball with the CLI. Workers only read it, so sharing is safe.
	value := make([]byte, cfg.ValueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	// Preload: write every key once so the measured GETs hit real data. Without
	// it, early GETs would mostly return NotFound, a cheaper path that skips
	// value transfer and would inflate throughput.
	if cfg.Preload {
		start := time.Now()
		if err := preload(ctx, cfg, conns, value); err != nil {
			return nil, err
		}
		fmt.Fprintf(log, "preloaded %d keys in %v\n", cfg.Keyspace, time.Since(start).Round(time.Millisecond))
	}
	// Make sure the probe key exists before probing starts, otherwise every
	// probe GET would be NotFound, which runProbe counts as a failure.
	if cfg.ProbeKey != "" {
		if err := admin.Put(ctx, cfg.ProbeKey, []byte("probe-value")); err != nil {
			return nil, fmt.Errorf("put probe key: %w", err)
		}
	}
	// Warm-up: run the same workload, recording nothing (tl and res are nil).
	// A freshly started system is not in steady state: the coordinator is still
	// opening node connections, LRU caches are empty, buffers and goroutine
	// stacks are still growing, the Go GC has not found its pacing, the CPU may
	// not be at full clock. Measuring from t=0 would mix that transient into
	// the numbers. requests=0 with a deadline makes it time-bounded.
	if cfg.Warmup > 0 {
		fmt.Fprintf(log, "warming up for %v\n", cfg.Warmup)
		runPhase(ctx, cfg, conns, gen, value, 0, time.Now().Add(cfg.Warmup), nil, nil)
	}

	// Counter snapshot BEFORE the measured phase. Cache and write-coalescing
	// counters are cumulative since each process started, so they include
	// preload and warm-up traffic. Only the difference after − before is
	// attributable to the measured phase (see the delta computation below).
	before, errBefore := clusterStats(ctx, admin)

	// Set up the measured phase's timeline and background activities.
	tl := newTimeline(cfg.TimelineInterval)
	// stopBg waits for the background goroutines (health sampler, probe, fault
	// runner) after the load has finished; bgCtx is how they are told to stop.
	var stopBg sync.WaitGroup
	bgCtx, cancelBg := context.WithCancel(ctx)
	// Results of the background goroutines. Each variable is written by exactly
	// one goroutine and read only after stopBg.Wait(), which provides the
	// happens-before edge, so no further locking is needed.
	var health []healthSample
	var probes []ProbeSample
	var faultAt, recoverAt time.Time

	// t=0 of the measured phase. Everything below (fault times, timeline
	// buckets, probe start times) is relative to this instant.
	tl.start = time.Now()
	// Health sampler: polls the coordinator's PING document to know how many
	// nodes it currently considers healthy.
	stopBg.Add(1)
	go func() { defer stopBg.Done(); health = sampleHealth(bgCtx, admin, tl) }()
	// Probe: repeatedly GETs one watched key on its own connection.
	if cfg.ProbeKey != "" {
		stopBg.Add(1)
		go func() { defer stopBg.Done(); probes = runProbe(bgCtx, cfg, tl.start) }()
	}
	// Fault injection: sleep until start+FaultAt, run the fault command, then
	// (optionally) sleep until start+RecoverAt and run the recovery command.
	// Doing this inside the benchmark puts the fault on the same clock as the
	// measurements, so "detected after 15 ms" is a meaningful number.
	if cfg.FaultCmd != "" {
		stopBg.Add(1)
		go func() {
			defer stopBg.Done()
			// runAt returns when the command FINISHED (e.g. kill has delivered
			// the signal), which is the moment the fault is actually in effect.
			faultAt = runAt(bgCtx, tl.start.Add(cfg.FaultAt), cfg.FaultCmd, log)
			if cfg.RecoverCmd != "" {
				recoverAt = runAt(bgCtx, tl.start.Add(cfg.RecoverAt), cfg.RecoverCmd, log)
			}
		}()
	}

	// Duration mode: stop starting new requests at start+Duration.
	// Request-count mode: deadline stays zero and is ignored by runPhase.
	var deadline time.Time
	if cfg.Requests == 0 {
		deadline = tl.start.Add(cfg.Duration)
	}
	fmt.Fprintf(log, "running: %d clients, %s\n", cfg.Clients, describeLength(cfg))
	// The measured phase itself. It blocks until all workers are done.
	stats := runPhase(ctx, cfg, conns, gen, value, cfg.Requests, deadline, tl, res)
	// Measured wall-clock time. It includes the tail where the last in-flight
	// requests complete after the deadline, so throughput = successes/elapsed
	// divides by the time the work actually took.
	elapsed := time.Since(tl.start)
	// Give the recovery command time to run if the load finished first.
	// (In practice this only warns: cancelBg below stops the fault goroutine,
	// so a recovery scheduled after the end of the load never runs.)
	if cfg.RecoverCmd != "" && cfg.Requests == 0 && cfg.RecoverAt > cfg.Duration {
		fmt.Fprintf(log, "warning: --recover-at is after the end of the run\n")
	}
	// Stop the background goroutines and wait, so their result slices are
	// complete and safe to read.
	cancelBg()
	stopBg.Wait()

	// Counter snapshot AFTER the measured phase, and the deltas.
	after, errAfter := clusterStats(ctx, admin)
	if errBefore == nil && errAfter == nil {
		// Cache delta: hits/misses/evictions caused by the measured phase only.
		// The coordinator sums these over all reachable nodes. Caveat: a node
		// that restarts during the run resets its counters to zero, so deltas
		// in fault runs are approximate; the cache suite has no faults.
		res.Cache = CacheDelta{
			Hits:      after.Cache.Hits - before.Cache.Hits,
			Misses:    after.Cache.Misses - before.Cache.Misses,
			Evictions: after.Cache.Evictions - before.Cache.Evictions,
			Available: true,
		}
		// Hit rate = hits / lookups, guarded against 0/0 (e.g. cache disabled
		// or a PUT-only workload).
		if t := res.Cache.Hits + res.Cache.Misses; t > 0 {
			res.Cache.HitRate = float64(res.Cache.Hits) / float64(t)
		}
		// Frames per write syscall on the coordinator and on the nodes, the
		// batching metric behind the README's scaling analysis.
		res.Coalescing = coalescingDelta(before, after)
	}

	// Final bookkeeping: machine load at the end (a big change between start
	// and end hints that something else was competing for the CPU), exact
	// percentiles and totals, the per-interval timeline, raw probe samples,
	// and the fault analysis.
	res.Host.LoadAvgEnd = loadAvg()
	aggregate(res, stats, elapsed)
	res.Timeline = tl.points(elapsed)
	res.Probe = probes
	if cfg.FaultCmd != "" {
		res.Fault = faultReport(cfg, tl, len(res.Timeline), health, probes, faultAt, recoverAt)
	}
	return res, nil
}

// coalescingDelta computes how many frames were written per write() syscall
// during the measured phase, separately for the coordinator and (summed) for
// all storage nodes.
//
// Why this metric: the README shows the system is syscall-bound. A server
// that answers each request with its own write() pays one syscall per frame;
// a server that coalesces pending frames into one buffer pays one syscall for
// many. frames/write = 1.0 means no batching at all; 9.2 means nine responses
// shared each syscall. Like the cache counters these are cumulative, so the
// before/after difference isolates the measured phase.
func coalescingDelta(before, after *coordinator.Stats) *CoalescingDelta {
	// Coordinator-side totals come straight from its own stats document.
	d := &CoalescingDelta{
		CoordFrames: after.Writes.Frames - before.Writes.Frames,
		CoordWrites: after.Writes.WriteSyscalls - before.Writes.WriteSyscalls,
	}
	// Node-side totals: sum over every node whose stats the coordinator
	// managed to fetch (Stats is nil for an unreachable node).
	nodeTotals := func(s *coordinator.Stats) (f, w int64) {
		for _, n := range s.Nodes {
			if n.Stats != nil {
				f += n.Stats.Writes.Frames
				w += n.Stats.Writes.WriteSyscalls
			}
		}
		return
	}
	bf, bw := nodeTotals(before)
	af, aw := nodeTotals(after)
	d.NodeFrames, d.NodeWrites = af-bf, aw-bw
	// Ratios, guarded against division by zero (no writes = no data).
	if d.CoordWrites > 0 {
		d.CoordFramesPerWrite = float64(d.CoordFrames) / float64(d.CoordWrites)
	}
	if d.NodeWrites > 0 {
		d.NodeFramesPerWrite = float64(d.NodeFrames) / float64(d.NodeWrites)
	}
	return d
}

// describeLength renders the stopping condition for the progress log:
// "2000 requests" or "for 15s".
func describeLength(cfg Config) string {
	if cfg.Requests > 0 {
		return fmt.Sprintf("%d requests", cfg.Requests)
	}
	return fmt.Sprintf("for %v", cfg.Duration)
}

// clusterStats fetches and decodes the coordinator's STATS document, which
// includes cluster config, cache totals, write-coalescing counters and each
// reachable node's own stats.
func clusterStats(ctx context.Context, c *client.Client) (*coordinator.Stats, error) {
	var st coordinator.Stats
	if err := c.StatsInto(ctx, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// preload PUTs every key once, split across the client connections.
//
// Work distribution uses a shared atomic counter ("next key to write") rather
// than giving each connection a fixed slice of keys. Each goroutine grabs the
// next index as it becomes free, so a connection that happens to be slower
// does not leave the others idle at the end (dynamic load balancing, like a
// work queue, without a channel or a lock).
//
// Any error aborts the preload: a benchmark over a partially loaded keyspace
// would produce NotFound GETs and misleading numbers, so it is better to stop.
func preload(ctx context.Context, cfg Config, conns []*transport.Conn, value []byte) error {
	// next is the next key index to write, shared by all goroutines.
	var next atomic.Int64
	// firstErr keeps only the FIRST failure; sync.Once makes the write safe
	// when several goroutines fail at the same time.
	var firstErr error
	var once sync.Once
	var wg sync.WaitGroup
	// One goroutine per connection: the preload runs at the same concurrency
	// as the benchmark itself, so it is as fast as the cluster allows (Run
	// logs how long it took).
	for _, c := range conns {
		wg.Add(1)
		go func(c *transport.Conn) {
			defer wg.Done()
			for {
				// Claim a key index: Add returns the new value, so subtract 1
				// to get the index this goroutine now owns exclusively.
				i := int(next.Add(1) - 1)
				// Stop when every key is claimed or the run was cancelled.
				if i >= cfg.Keyspace || ctx.Err() != nil {
					return
				}
				// Write the key, bounded by the per-request timeout.
				resp, err := c.DoDeadline(&protocol.Request{Op: protocol.OpPut, Key: KeyName(i), Value: value}, time.Now().Add(cfg.RequestTimeout))
				// A transport-level success can still carry an error status
				// (e.g. write quorum not met); Err() turns it into an error.
				if err == nil {
					err = resp.Err()
				}
				if err != nil {
					// Record the first failure and stop this goroutine. The
					// other goroutines are not interrupted: they keep claiming
					// keys until the keyspace is exhausted (or they fail too),
					// but once firstErr is set Run aborts the whole benchmark.
					once.Do(func() { firstErr = fmt.Errorf("preload %s: %w", KeyName(i), err) })
					return
				}
			}
		}(c)
	}
	// Wait for every goroutine; only then is firstErr safe to read.
	wg.Wait()
	return firstErr
}

// runPhase runs the workload until `requests` ops complete (if > 0) or until
// deadline. With tl == nil nothing is recorded (warm-up).
//
// This is the heart of the load generator. One goroutine per connection runs
// the closed loop:
//
//	loop: decide GET or PUT → pick a key → send → block until the answer →
//	      measure latency → record → repeat
//
// Because each goroutine waits for its answer before sending again, the number
// of requests in flight is always exactly len(conns). Latency is measured on
// the client side, around the full round trip: encode + write syscall + the
// coordinator's work (including its RPCs to replicas) + read + decode. That is
// what an application would experience, not just the server's internal time.
//
// It returns one workerStats per worker; the caller merges them.
func runPhase(ctx context.Context, cfg Config, conns []*transport.Conn, gen KeyGen, value []byte,
	requests int64, deadline time.Time, tl *timeline, res *Result) []*workerStats {
	// In request-count mode, remaining is a shared budget. Every worker
	// decrements it before each operation; whoever takes it below zero stops.
	// So exactly `requests` operations run in total, however they are spread
	// among workers (fast workers naturally do more of them).
	var remaining atomic.Int64
	remaining.Store(requests)
	stats := make([]*workerStats, len(conns))
	var wg sync.WaitGroup
	for w := range conns {
		ws := &workerStats{errSamples: map[string]int{}}
		// Pre-size the latency slices when the op count is known, so append
		// never has to reallocate and copy during the measurement (that would
		// add allocation spikes and GC work to the very latencies being
		// measured). per = this worker's expected share, +1 for rounding; the
		// +16 slack absorbs random variation in the GET/PUT mix. In duration
		// mode the count is unknown and the slices grow normally.
		if tl != nil && requests > 0 {
			per := int(requests/int64(len(conns))) + 1
			ws.gets = make([]time.Duration, 0, int(float64(per)*cfg.ReadRatio)+16)
			ws.puts = make([]time.Duration, 0, int(float64(per)*(1-cfg.ReadRatio))+16)
		}
		stats[w] = ws
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// A private RNG per worker: no lock contention on a shared source,
			// and deterministic. PCG seeded with (Seed, worker index) gives
			// each worker its own independent stream, and the same Seed
			// replays the same sequence of operations and keys per worker.
			rng := rand.New(rand.NewPCG(cfg.Seed, uint64(w)))
			// This worker's connection (it may be replaced after an error).
			conn := conns[w]
			// Run until cancelled, out of budget, or past the deadline.
			for ctx.Err() == nil {
				if requests > 0 {
					// Claim one operation from the shared budget.
					if remaining.Add(-1) < 0 {
						return
					}
				} else if time.Now().After(deadline) {
					// Duration mode: stop STARTING new ops after the deadline.
					// The op in flight (if any) has already completed.
					return
				}
				// Choose the operation: GET with probability ReadRatio.
				isGet := rng.Float64() < cfg.ReadRatio
				// Build the request: a PUT of the shared value by default...
				req := &protocol.Request{Op: protocol.OpPut, Key: KeyName(gen.Next(rng)), Value: value}
				// ...turned into a GET (no value) for reads.
				if isGet {
					req.Op, req.Value = protocol.OpGet, nil
				}
				// Time the full round trip. DoDeadline sets a socket deadline
				// instead of creating a context per request (cheaper on this
				// hot path).
				t0 := time.Now()
				resp, err := conn.DoDeadline(req, t0.Add(cfg.RequestTimeout))
				lat := time.Since(t0)
				// Success means the server answered OK, or NotFound for a GET
				// (a valid answer: "that key has no value"). Anything else
				// (timeout, broken connection, error status such as quorum
				// failure) is a failed operation.
				failed := err != nil || (resp.Status != protocol.StatusOK && resp.Status != protocol.StatusNotFound)
				// Every operation, success or failure, goes into the timeline,
				// in the bucket where it started.
				if tl != nil {
					tl.record(t0, lat, failed)
				}
				if err != nil {
					// The connection is in an unknown state: replace it.
					//
					// After a timeout the request may still be in flight; its
					// late response would be read as the answer to the NEXT
					// request. A sequential connection cannot recover from that,
					// so close it and dial a fresh one.
					conn.Close()
					ws.noteErr(err.Error())
					countErr(ws, isGet)
					conn = redial(ctx, cfg.Address)
					// Publish the new connection so Run's deferred cleanup
					// closes it (and not the already-closed old one). Each
					// worker only ever touches its own slot, so no lock needed.
					conns[w] = conn
					// nil means ctx was cancelled while redialling.
					if conn == nil {
						return
					}
					continue
				}
				// Error status, connection still healthy: count and continue.
				if failed {
					ws.noteErr(fmt.Sprintf("%s: %s", resp.Status, resp.Value))
					countErr(ws, isGet)
					continue
				}
				// Warm-up: nothing to record.
				if tl == nil {
					continue
				}
				// Keep the successful sample for exact percentiles. Note that
				// FAILED operations' latencies are deliberately not in these
				// slices: they are reported as error counts (and appear in the
				// timeline), so a burst of fast failures cannot make the
				// latency percentiles look better.
				if isGet {
					if resp.Status == protocol.StatusNotFound {
						ws.nf++
					}
					ws.gets = append(ws.gets, lat)
				} else {
					ws.puts = append(ws.puts, lat)
				}
			}
		}(w)
	}
	// Wait for all workers; their stats are complete once Wait returns.
	wg.Wait()
	return stats
}

// countErr increments the worker's error counter for the operation type.
func countErr(ws *workerStats, isGet bool) {
	if isGet {
		ws.getErrs++
	} else {
		ws.putErrs++
	}
}

// redial reconnects to addr with exponential backoff (10 ms, 20 ms, 40 ms ...
// capped at 1 s) until it succeeds or ctx is cancelled (then it returns nil).
// Backoff avoids a tight reconnect loop hammering a coordinator that is
// temporarily refusing connections, which would waste the CPU the rest of the
// benchmark needs.
func redial(ctx context.Context, addr string) *transport.Conn {
	for backoff := 10 * time.Millisecond; ctx.Err() == nil; backoff = min(2*backoff, time.Second) {
		if c, err := transport.Dial(ctx, addr, 2*time.Second, 0); err == nil {
			return c
		}
		time.Sleep(backoff)
	}
	return nil
}

// aggregate merges all workers' stats into the Result: counts, throughput and
// exact latency percentiles (overall, GET only, PUT only).
func aggregate(res *Result, stats []*workerStats, elapsed time.Duration) {
	var gets, puts []time.Duration
	res.ErrorTypes = map[string]int{}
	for _, ws := range stats {
		// Concatenate every worker's raw samples: exact percentiles need the
		// full population, not per-worker percentiles (which cannot be
		// averaged meaningfully: the mean of p99s is not the p99).
		gets = append(gets, ws.gets...)
		puts = append(puts, ws.puts...)
		res.Get.Errors += ws.getErrs
		res.Put.Errors += ws.putErrs
		res.Get.NotFound += ws.nf
		// Merge error samples, keeping at most 10 distinct messages overall
		// (existing ones always accumulate their counts).
		for e, n := range ws.errSamples {
			if _, ok := res.ErrorTypes[e]; ok || len(res.ErrorTypes) < 10 {
				res.ErrorTypes[e] += n
			}
		}
	}
	secs := elapsed.Seconds()
	res.ElapsedSec = secs
	// Count = attempts (successes + failures); OpsPerSec = successes only.
	// Throughput deliberately excludes failures: "ops/s" in the README means
	// work the system actually completed.
	res.Get.Count = int64(len(gets)) + res.Get.Errors
	res.Put.Count = int64(len(puts)) + res.Put.Errors
	res.Get.OpsPerSec = float64(len(gets)) / secs
	res.Put.OpsPerSec = float64(len(puts)) / secs
	res.Successes = int64(len(gets) + len(puts))
	res.Errors = res.Get.Errors + res.Put.Errors
	res.TotalOps = res.Successes + res.Errors
	res.OpsPerSec = float64(res.Successes) / secs
	// Overall latency = GETs and PUTs together. Build a new slice so that
	// summarize's in-place sort of `all` does not disturb gets/puts ordering
	// (they are sorted separately just below anyway).
	all := make([]time.Duration, 0, len(gets)+len(puts))
	all = append(append(all, gets...), puts...)
	res.Latency = summarize(all)
	res.Get.Latency = summarize(gets)
	res.Put.Latency = summarize(puts)
}

// points converts the used part of the timeline into serialisable
// TimelinePoints, one per interval from t=0 to the end of the run.
func (t *timeline) points(elapsed time.Duration) []TimelinePoint {
	// Number of intervals covered by the run (+1 for the final partial one),
	// capped at the array size.
	n := min(int(elapsed/t.interval)+1, maxBuckets)
	out := make([]TimelinePoint, 0, n)
	for i := 0; i < n; i++ {
		b := &t.buckets[i]
		// t_ms is the START of the interval, relative to the measured phase.
		// HealthyNodes is -1 when no health sample fell in this interval.
		p := TimelinePoint{TMS: (time.Duration(i) * t.interval).Milliseconds(), Ops: b.ops.Load(), Errors: b.errs.Load(), HealthyNodes: int(b.healthy.Load())}
		// Per-interval p99 and max from the bucket's histogram (if any
		// operation started in it).
		if h := b.hist.Load(); h != nil {
			s := h.Snapshot()
			p.P99US, p.MaxUS = s.P99US, s.MaxUS
		}
		// Rate for this interval. The last interval is usually partial, so its
		// rate reads low; plots should not over-interpret the final point.
		p.OpsPerSec = float64(p.Ops) / t.interval.Seconds()
		out = append(out, p)
	}
	return out
}

// sampleHealth polls the coordinator's health every 20ms (or the timeline
// interval, if shorter), recording the healthy-node count.
//
// This is how the benchmark observes the coordinator's FAILURE DETECTOR from
// outside: "detection time" in Test D is the delay between the fault command
// finishing and the first sample showing fewer healthy nodes. The 20 ms
// sampling period (plus one PING round trip) is therefore the resolution of
// that measurement.
func sampleHealth(ctx context.Context, c *client.Client, tl *timeline) []healthSample {
	every := min(20*time.Millisecond, tl.interval)
	var out []healthSample
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		// PING returns the coordinator's HealthDoc (healthy / unhealthy /
		// syncing counts). Timestamp the sample when the answer arrived.
		raw, err := c.Ping(ctx)
		now := time.Now()
		// A failed or undecodable ping is simply skipped: missing samples are
		// harmless, invented ones would not be.
		if err == nil {
			var h coordinator.HealthDoc
			if json.Unmarshal(raw, &h) == nil {
				out = append(out, healthSample{at: now, healthy: h.HealthyNodes, total: h.HealthyNodes + h.UnhealthyNodes + h.SyncingNodes})
				// Also stamp the timeline so graphs can plot healthy nodes over
				// time next to throughput (last sample in an interval wins).
				tl.buckets[tl.idx(now)].healthy.Store(int64(h.HealthyNodes))
			}
		}
		// Wait for the next tick, or stop when the run ends. A ticker (rather
		// than sleeping 20 ms after each ping) keeps a steady cadence even if
		// a ping is slow.
		select {
		case <-ctx.Done():
			return out
		case <-t.C:
		}
	}
}

// runProbe GETs the probe key sequentially on its own connection.
//
// The probe answers a question the aggregate numbers cannot: "what happened
// to requests for THE key whose primary node just died?" Among thousands of
// random keys, only the requests that involve the failed node (e.g. reads of
// keys it is primary for, about 1 in 4 with 4 nodes) are affected, and their
// samples are diluted in the percentiles. The probe hits that one key every
// ProbeInterval (5 ms in Test D), so the result contains a dense per-request
// trace: when it first failed or slowed, how slow the worst request was
// (~1 s with SIGSTOP: exactly the coordinator's request timeout), and when
// latency returned to normal (the fallback to another replica took effect).
//
// Like the workers it is closed-loop: the next probe starts `interval` after
// the previous one ENDS, so one probe stuck for 1 s produces one 1 s sample.
func runProbe(ctx context.Context, cfg Config, start time.Time) []ProbeSample {
	interval := cfg.ProbeInterval
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	var out []ProbeSample
	// Its own connection, separate from the workers' ones, so the probe never
	// waits behind a worker's request on the same socket.
	var conn *transport.Conn
	for ctx.Err() == nil {
		// Start time relative to the measured phase, in ms with µs precision.
		t0 := time.Now()
		s := ProbeSample{StartMS: float64(t0.Sub(start).Microseconds()) / 1e3}
		var err error
		// (Re)connect lazily: after an error the connection is dropped and a
		// new one is dialled on the next probe. Dial time counts toward this
		// probe's latency, as it would for a real client.
		if conn == nil {
			conn, err = transport.Dial(ctx, cfg.Address, time.Second, 0)
		}
		if err == nil {
			var resp *protocol.Response
			resp, err = conn.DoDeadline(&protocol.Request{Op: protocol.OpGet, Key: cfg.ProbeKey}, t0.Add(cfg.RequestTimeout))
			if err != nil {
				// Transport error: the connection may hold a late response, so
				// discard it (same reasoning as in runPhase).
				conn.Close()
				conn = nil
			} else if resp.Status != protocol.StatusOK {
				// The key was written before probing started, so NotFound (or
				// any other status) is a failure for the probe: it would mean
				// the cluster lost or could not serve the value.
				err = fmt.Errorf("%s: %s", resp.Status, resp.Value)
			}
		}
		s.LatencyUS = float64(time.Since(t0).Nanoseconds()) / 1e3
		s.OK = err == nil
		if err != nil {
			s.Err = err.Error()
		}
		out = append(out, s)
		// Pause between probes (or exit promptly when the run ends).
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
	if conn != nil {
		conn.Close()
	}
	return out
}

// runAt runs a shell command at time `at` and returns when it finished: for
// a kill command that is the moment the fault is in effect, and for a
// restart command the moment the process is back up.
//
// It returns the zero time if the run ended before `at` (the command is then
// never executed). The command runs through `sh -c`, so the scripts can pass
// arbitrary shell (kill, a bash -c that restarts a node, ...). Its output goes
// to the progress log / stderr, and a failing command is logged but does not
// abort the benchmark: the traffic measurements are still valid.
func runAt(ctx context.Context, at time.Time, cmd string, log io.Writer) time.Time {
	// Sleep until the scheduled instant, unless the run is cancelled first.
	select {
	case <-ctx.Done():
		return time.Time{}
	case <-time.After(time.Until(at)):
	}
	c := exec.Command("sh", "-c", cmd)
	c.Stdout, c.Stderr = log, os.Stderr
	err := c.Run()
	// "done" is when the command returned, the reference point for all fault
	// timings (detection, probe recovery, ...).
	done := time.Now()
	if err != nil {
		fmt.Fprintf(log, "command %q: %v\n", cmd, err)
	} else {
		fmt.Fprintf(log, "ran %q (took %v)\n", cmd, done.Sub(at).Round(time.Millisecond))
	}
	return done
}

// faultReport distils the timeline, health samples and probe samples into the
// handful of numbers the README's Test D table reports: errors before/after
// the fault, baseline vs peak p99, detection time, probe behaviour and time to
// full recovery.
//
// n is the number of timeline intervals actually used by the run.
func faultReport(cfg Config, tl *timeline, n int, health []healthSample, probes []ProbeSample, faultAt, recoverAt time.Time) *FaultReport {
	// -1 means "never happened" for detection and first probe success, which
	// is distinguishable from a genuine 0 ms.
	f := &FaultReport{FaultCmd: cfg.FaultCmd, RecoverCmd: cfg.RecoverCmd, DetectionMS: -1, ProbeFirstSuccessMS: -1}
	// The fault never ran (run ended or was cancelled first): nothing to do.
	if faultAt.IsZero() {
		return f
	}
	// ms converts an absolute instant into milliseconds since the start of
	// the measured phase (with µs precision).
	ms := func(t time.Time) float64 { return float64(t.Sub(tl.start).Microseconds()) / 1e3 }
	f.FaultAtMS = ms(faultAt)
	// fi = index of the interval containing the fault. Intervals before it are
	// the baseline; the fault's own interval and everything after count as
	// "after the fault".
	fi := tl.idx(faultAt)
	var baseline []int64
	for i := 0; i < n; i++ {
		b := &tl.buckets[i]
		h := b.hist.Load()
		if i < fi {
			// Before the fault: errors (should be 0) and one p99 per interval.
			f.ErrorsBeforeFault += b.errs.Load()
			if h != nil {
				baseline = append(baseline, h.Snapshot().P99US)
			}
			continue
		}
		// After the fault: errors, the worst interval p99 and the slowest
		// single operation. Both come from the per-interval histograms.
		f.ErrorsAfterFault += b.errs.Load()
		if h != nil {
			s := h.Snapshot()
			f.PeakP99AfterFaultUS = max(f.PeakP99AfterFaultUS, s.P99US)
			f.MaxLatencyAfterFault = max(f.MaxLatencyAfterFault, s.MaxUS)
		}
	}
	if len(baseline) > 0 {
		// Median of per-interval p99s before the fault: robust to one noisy interval.
		// (A mean would be dragged up by a single GC pause or scheduler hiccup.)
		slices.Sort(baseline)
		f.BaselineP99US = baseline[len(baseline)/2]
	}
	// Detection time: remember the healthy count in the last sample taken at
	// or before the fault, then find the first later sample reporting FEWER
	// healthy nodes. That transition is the coordinator's failure detector
	// firing (passive: 3 consecutive failed requests; or active: failed health
	// checks). Resolution is the 20 ms sampling period.
	var healthyAtFault = -1
	for _, h := range health {
		if !h.at.After(faultAt) {
			healthyAtFault = h.healthy
			continue
		}
		if healthyAtFault >= 0 && h.healthy < healthyAtFault {
			f.DetectionMS = float64(h.at.Sub(faultAt).Microseconds()) / 1e3
			break
		}
	}
	// Recovery time: from the moment the recovery command FINISHED (node
	// restarted or SIGCONT delivered) to the first sample where every member
	// is HEALTHY again. That includes detection by the next health check and
	// the resync of the data the node missed. -1 = never within the run.
	if !recoverAt.IsZero() {
		f.RecoverAtMS = ms(recoverAt)
		f.RecoveredToHealthyMS = -1
		for _, h := range health {
			if h.at.After(recoverAt) && h.healthy == h.total {
				f.RecoveredToHealthyMS = float64(h.at.Sub(recoverAt).Microseconds()) / 1e3
				break
			}
		}
	}
	// Probe analysis. A probe is affected by the fault if it was still in
	// flight when the fault took effect or started afterwards.
	//
	// Step 1: baseline = successful probes that COMPLETED before the fault.
	// Their median latency defines "normal" for this key.
	var before []float64
	for _, p := range probes {
		if p.StartMS+p.LatencyUS/1e3 < f.FaultAtMS && p.OK {
			before = append(before, p.LatencyUS)
		}
	}
	// "Slow" = more than 10× the pre-fault median. Without a baseline, +Inf
	// means no probe is ever classified as slow (rather than all of them).
	slow := math.Inf(1)
	if len(before) > 0 {
		slices.Sort(before)
		slow = 10 * before[len(before)/2]
	}
	// Step 2: walk the affected probes (those ending at or after the fault).
	for _, p := range probes {
		end := p.StartMS + p.LatencyUS/1e3
		if end < f.FaultAtMS {
			continue
		}
		// Failed probes (timeouts, error statuses) are counted, not timed.
		if !p.OK {
			f.ProbeFailures++
			continue
		}
		// Worst successful probe latency after the fault.
		f.ProbeMaxLatencyUS = max(f.ProbeMaxLatencyUS, p.LatencyUS)
		// First affected probe that succeeded: how soon after the fault the
		// key was served again (relative to the fault, in ms).
		if f.ProbeFirstSuccessMS < 0 {
			f.ProbeFirstSuccessMS = end - f.FaultAtMS
		}
		// Slow probes, and when the LAST one ended: after that point every
		// probe was fast again, i.e. the fallback to another replica was fully
		// in effect ("fallback settled" in the README; 0 = no slow probes).
		if p.LatencyUS > slow {
			f.ProbeSlowAfterFault++
			f.ProbeStallEndMS = max(f.ProbeStallEndMS, end-f.FaultAtMS)
		}
	}
	return f
}
