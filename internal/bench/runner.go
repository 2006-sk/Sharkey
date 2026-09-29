package bench

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
func (c *Config) Validate() error {
	switch {
	case c.Clients < 1:
		return errors.New("--clients must be >= 1")
	case c.Requests <= 0 && c.Duration <= 0:
		return errors.New("one of --requests or --duration must be positive")
	case c.ReadRatio < 0 || c.ReadRatio > 1:
		return errors.New("--read-ratio must be in [0,1]")
	case c.Keyspace < 1:
		return errors.New("--keyspace must be >= 1")
	case c.ValueSize < 0 || c.ValueSize > protocol.DefaultMaxValueSize:
		return fmt.Errorf("--value-size must be in [0,%d]", protocol.DefaultMaxValueSize)
	case c.Distribution != "uniform" && c.Distribution != "zipf":
		return errors.New("--distribution must be uniform or zipf")
	case c.FaultCmd != "" && c.FaultAt <= 0:
		return errors.New("--fault-cmd requires --fault-at")
	}
	return nil
}

// maxBuckets bounds timeline memory (e.g. 1h at 100ms).
const maxBuckets = 36000

type bucket struct {
	ops, errs atomic.Int64
	hist      atomic.Pointer[metrics.Histogram]
	healthy   atomic.Int64
}

type timeline struct {
	start    time.Time
	interval time.Duration
	buckets  []bucket
}

func newTimeline(interval time.Duration) *timeline {
	if interval <= 0 {
		interval = time.Second
	}
	t := &timeline{interval: interval, buckets: make([]bucket, maxBuckets)}
	for i := range t.buckets {
		t.buckets[i].healthy.Store(-1)
	}
	return t
}

func (t *timeline) idx(at time.Time) int {
	return min(int(at.Sub(t.start)/t.interval), maxBuckets-1)
}

func (t *timeline) record(at time.Time, lat time.Duration, failed bool) {
	b := &t.buckets[t.idx(at)]
	b.ops.Add(1)
	if failed {
		b.errs.Add(1)
	}
	h := b.hist.Load()
	if h == nil {
		b.hist.CompareAndSwap(nil, &metrics.Histogram{})
		h = b.hist.Load()
	}
	h.Record(lat)
}

type workerStats struct {
	gets, puts           []time.Duration
	getErrs, putErrs, nf int64
	errSamples           map[string]int
}

func (w *workerStats) noteErr(err string) {
	if len(err) > 120 {
		err = err[:120]
	}
	if _, ok := w.errSamples[err]; ok || len(w.errSamples) < 5 {
		w.errSamples[err]++
	}
}

// healthSample is one observation of the coordinator's healthy-node count.
type healthSample struct {
	at      time.Time
	healthy int
	total   int
}

// Run executes a benchmark and returns measured results. Progress messages
// go to log.
func Run(ctx context.Context, cfg Config, log io.Writer) (*Result, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var gen KeyGen = Uniform{N: cfg.Keyspace}
	if cfg.Distribution == "zipf" {
		z, err := NewZipf(cfg.Keyspace, cfg.ZipfTheta)
		if err != nil {
			return nil, err
		}
		gen = z
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.Seed == 0 {
		cfg.Seed = uint64(time.Now().UnixNano())
	}

	admin := client.New(cfg.Address, client.Options{DialTimeout: 2 * time.Second, RequestTimeout: 10 * time.Second, Conns: 1})
	defer admin.Close()
	res := &Result{Label: cfg.Label, StartedAt: time.Now().UTC(), Host: hostInfo(), Config: cfg}
	if st, err := clusterStats(ctx, admin); err == nil {
		res.ClusterNodes = len(st.Nodes)
		res.Cluster, _ = json.Marshal(st.Config)
	}

	// One dedicated connection per simulated client, like independent users.
	conns := make([]*transport.Conn, cfg.Clients)
	for i := range conns {
		c, err := transport.Dial(ctx, cfg.Address, 2*time.Second, 0)
		if err != nil {
			return nil, fmt.Errorf("client %d: %w", i, err)
		}
		conns[i] = c
	}
	defer func() {
		for _, c := range conns {
			if c != nil {
				c.Close()
			}
		}
	}()

	value := make([]byte, cfg.ValueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	if cfg.Preload {
		start := time.Now()
		if err := preload(ctx, cfg, conns, value); err != nil {
			return nil, err
		}
		fmt.Fprintf(log, "preloaded %d keys in %v\n", cfg.Keyspace, time.Since(start).Round(time.Millisecond))
	}
	if cfg.ProbeKey != "" {
		if err := admin.Put(ctx, cfg.ProbeKey, []byte("probe-value")); err != nil {
			return nil, fmt.Errorf("put probe key: %w", err)
		}
	}
	if cfg.Warmup > 0 {
		fmt.Fprintf(log, "warming up for %v\n", cfg.Warmup)
		runPhase(ctx, cfg, conns, gen, value, 0, time.Now().Add(cfg.Warmup), nil, nil)
	}

	before, errBefore := clusterStats(ctx, admin)

	tl := newTimeline(cfg.TimelineInterval)
	var stopBg sync.WaitGroup
	bgCtx, cancelBg := context.WithCancel(ctx)
	var health []healthSample
	var probes []ProbeSample
	var faultAt, recoverAt time.Time

	tl.start = time.Now()
	stopBg.Add(1)
	go func() { defer stopBg.Done(); health = sampleHealth(bgCtx, admin, tl) }()
	if cfg.ProbeKey != "" {
		stopBg.Add(1)
		go func() { defer stopBg.Done(); probes = runProbe(bgCtx, cfg, tl.start) }()
	}
	if cfg.FaultCmd != "" {
		stopBg.Add(1)
		go func() {
			defer stopBg.Done()
			faultAt = runAt(bgCtx, tl.start.Add(cfg.FaultAt), cfg.FaultCmd, log)
			if cfg.RecoverCmd != "" {
				recoverAt = runAt(bgCtx, tl.start.Add(cfg.RecoverAt), cfg.RecoverCmd, log)
			}
		}()
	}

	var deadline time.Time
	if cfg.Requests == 0 {
		deadline = tl.start.Add(cfg.Duration)
	}
	fmt.Fprintf(log, "running: %d clients, %s\n", cfg.Clients, describeLength(cfg))
	stats := runPhase(ctx, cfg, conns, gen, value, cfg.Requests, deadline, tl, res)
	elapsed := time.Since(tl.start)
	// Give the recovery command time to run if the load finished first.
	if cfg.RecoverCmd != "" && cfg.Requests == 0 && cfg.RecoverAt > cfg.Duration {
		fmt.Fprintf(log, "warning: --recover-at is after the end of the run\n")
	}
	cancelBg()
	stopBg.Wait()

	after, errAfter := clusterStats(ctx, admin)
	if errBefore == nil && errAfter == nil {
		res.Cache = CacheDelta{
			Hits:      after.Cache.Hits - before.Cache.Hits,
			Misses:    after.Cache.Misses - before.Cache.Misses,
			Evictions: after.Cache.Evictions - before.Cache.Evictions,
			Available: true,
		}
		if t := res.Cache.Hits + res.Cache.Misses; t > 0 {
			res.Cache.HitRate = float64(res.Cache.Hits) / float64(t)
		}
		res.Coalescing = coalescingDelta(before, after)
	}

	res.Host.LoadAvgEnd = loadAvg()
	aggregate(res, stats, elapsed)
	res.Timeline = tl.points(elapsed)
	res.Probe = probes
	if cfg.FaultCmd != "" {
		res.Fault = faultReport(cfg, tl, len(res.Timeline), health, probes, faultAt, recoverAt)
	}
	return res, nil
}

func coalescingDelta(before, after *coordinator.Stats) *CoalescingDelta {
	d := &CoalescingDelta{
		CoordFrames: after.Writes.Frames - before.Writes.Frames,
		CoordWrites: after.Writes.WriteSyscalls - before.Writes.WriteSyscalls,
	}
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
	if d.CoordWrites > 0 {
		d.CoordFramesPerWrite = float64(d.CoordFrames) / float64(d.CoordWrites)
	}
	if d.NodeWrites > 0 {
		d.NodeFramesPerWrite = float64(d.NodeFrames) / float64(d.NodeWrites)
	}
	return d
}

func describeLength(cfg Config) string {
	if cfg.Requests > 0 {
		return fmt.Sprintf("%d requests", cfg.Requests)
	}
	return fmt.Sprintf("for %v", cfg.Duration)
}

func clusterStats(ctx context.Context, c *client.Client) (*coordinator.Stats, error) {
	var st coordinator.Stats
	if err := c.StatsInto(ctx, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// preload PUTs every key once, split across the client connections.
func preload(ctx context.Context, cfg Config, conns []*transport.Conn, value []byte) error {
	var next atomic.Int64
	var firstErr error
	var once sync.Once
	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		go func(c *transport.Conn) {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= cfg.Keyspace || ctx.Err() != nil {
					return
				}
				resp, err := c.DoDeadline(&protocol.Request{Op: protocol.OpPut, Key: KeyName(i), Value: value}, time.Now().Add(cfg.RequestTimeout))
				if err == nil {
					err = resp.Err()
				}
				if err != nil {
					once.Do(func() { firstErr = fmt.Errorf("preload %s: %w", KeyName(i), err) })
					return
				}
			}
		}(c)
	}
	wg.Wait()
	return firstErr
}

// runPhase runs the workload until `requests` ops complete (if > 0) or until
// deadline. With tl == nil nothing is recorded (warm-up).
func runPhase(ctx context.Context, cfg Config, conns []*transport.Conn, gen KeyGen, value []byte,
	requests int64, deadline time.Time, tl *timeline, res *Result) []*workerStats {
	var remaining atomic.Int64
	remaining.Store(requests)
	stats := make([]*workerStats, len(conns))
	var wg sync.WaitGroup
	for w := range conns {
		ws := &workerStats{errSamples: map[string]int{}}
		if tl != nil && requests > 0 {
			per := int(requests/int64(len(conns))) + 1
			ws.gets = make([]time.Duration, 0, int(float64(per)*cfg.ReadRatio)+16)
			ws.puts = make([]time.Duration, 0, int(float64(per)*(1-cfg.ReadRatio))+16)
		}
		stats[w] = ws
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(cfg.Seed, uint64(w)))
			conn := conns[w]
			for ctx.Err() == nil {
				if requests > 0 {
					if remaining.Add(-1) < 0 {
						return
					}
				} else if time.Now().After(deadline) {
					return
				}
				isGet := rng.Float64() < cfg.ReadRatio
				req := &protocol.Request{Op: protocol.OpPut, Key: KeyName(gen.Next(rng)), Value: value}
				if isGet {
					req.Op, req.Value = protocol.OpGet, nil
				}
				t0 := time.Now()
				resp, err := conn.DoDeadline(req, t0.Add(cfg.RequestTimeout))
				lat := time.Since(t0)
				failed := err != nil || (resp.Status != protocol.StatusOK && resp.Status != protocol.StatusNotFound)
				if tl != nil {
					tl.record(t0, lat, failed)
				}
				if err != nil {
					// The connection is in an unknown state: replace it.
					conn.Close()
					ws.noteErr(err.Error())
					countErr(ws, isGet)
					conn = redial(ctx, cfg.Address)
					conns[w] = conn
					if conn == nil {
						return
					}
					continue
				}
				if failed {
					ws.noteErr(fmt.Sprintf("%s: %s", resp.Status, resp.Value))
					countErr(ws, isGet)
					continue
				}
				if tl == nil {
					continue
				}
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
	wg.Wait()
	return stats
}

func countErr(ws *workerStats, isGet bool) {
	if isGet {
		ws.getErrs++
	} else {
		ws.putErrs++
	}
}

func redial(ctx context.Context, addr string) *transport.Conn {
	for backoff := 10 * time.Millisecond; ctx.Err() == nil; backoff = min(2*backoff, time.Second) {
		if c, err := transport.Dial(ctx, addr, 2*time.Second, 0); err == nil {
			return c
		}
		time.Sleep(backoff)
	}
	return nil
}

func aggregate(res *Result, stats []*workerStats, elapsed time.Duration) {
	var gets, puts []time.Duration
	res.ErrorTypes = map[string]int{}
	for _, ws := range stats {
		gets = append(gets, ws.gets...)
		puts = append(puts, ws.puts...)
		res.Get.Errors += ws.getErrs
		res.Put.Errors += ws.putErrs
		res.Get.NotFound += ws.nf
		for e, n := range ws.errSamples {
			if _, ok := res.ErrorTypes[e]; ok || len(res.ErrorTypes) < 10 {
				res.ErrorTypes[e] += n
			}
		}
	}
	secs := elapsed.Seconds()
	res.ElapsedSec = secs
	res.Get.Count = int64(len(gets)) + res.Get.Errors
	res.Put.Count = int64(len(puts)) + res.Put.Errors
	res.Get.OpsPerSec = float64(len(gets)) / secs
	res.Put.OpsPerSec = float64(len(puts)) / secs
	res.Successes = int64(len(gets) + len(puts))
	res.Errors = res.Get.Errors + res.Put.Errors
	res.TotalOps = res.Successes + res.Errors
	res.OpsPerSec = float64(res.Successes) / secs
	all := make([]time.Duration, 0, len(gets)+len(puts))
	all = append(append(all, gets...), puts...)
	res.Latency = summarize(all)
	res.Get.Latency = summarize(gets)
	res.Put.Latency = summarize(puts)
}

func (t *timeline) points(elapsed time.Duration) []TimelinePoint {
	n := min(int(elapsed/t.interval)+1, maxBuckets)
	out := make([]TimelinePoint, 0, n)
	for i := 0; i < n; i++ {
		b := &t.buckets[i]
		p := TimelinePoint{TMS: (time.Duration(i) * t.interval).Milliseconds(), Ops: b.ops.Load(), Errors: b.errs.Load(), HealthyNodes: int(b.healthy.Load())}
		if h := b.hist.Load(); h != nil {
			s := h.Snapshot()
			p.P99US, p.MaxUS = s.P99US, s.MaxUS
		}
		p.OpsPerSec = float64(p.Ops) / t.interval.Seconds()
		out = append(out, p)
	}
	return out
}

// sampleHealth polls the coordinator's health every 20ms (or the timeline
// interval, if shorter), recording the healthy-node count.
func sampleHealth(ctx context.Context, c *client.Client, tl *timeline) []healthSample {
	every := min(20*time.Millisecond, tl.interval)
	var out []healthSample
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		raw, err := c.Ping(ctx)
		now := time.Now()
		if err == nil {
			var h coordinator.HealthDoc
			if json.Unmarshal(raw, &h) == nil {
				out = append(out, healthSample{at: now, healthy: h.HealthyNodes, total: h.HealthyNodes + h.UnhealthyNodes + h.SyncingNodes})
				tl.buckets[tl.idx(now)].healthy.Store(int64(h.HealthyNodes))
			}
		}
		select {
		case <-ctx.Done():
			return out
		case <-t.C:
		}
	}
}

// runProbe GETs the probe key sequentially on its own connection.
func runProbe(ctx context.Context, cfg Config, start time.Time) []ProbeSample {
	interval := cfg.ProbeInterval
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	var out []ProbeSample
	var conn *transport.Conn
	for ctx.Err() == nil {
		t0 := time.Now()
		s := ProbeSample{StartMS: float64(t0.Sub(start).Microseconds()) / 1e3}
		var err error
		if conn == nil {
			conn, err = transport.Dial(ctx, cfg.Address, time.Second, 0)
		}
		if err == nil {
			var resp *protocol.Response
			resp, err = conn.DoDeadline(&protocol.Request{Op: protocol.OpGet, Key: cfg.ProbeKey}, t0.Add(cfg.RequestTimeout))
			if err != nil {
				conn.Close()
				conn = nil
			} else if resp.Status != protocol.StatusOK {
				err = fmt.Errorf("%s: %s", resp.Status, resp.Value)
			}
		}
		s.LatencyUS = float64(time.Since(t0).Nanoseconds()) / 1e3
		s.OK = err == nil
		if err != nil {
			s.Err = err.Error()
		}
		out = append(out, s)
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
func runAt(ctx context.Context, at time.Time, cmd string, log io.Writer) time.Time {
	select {
	case <-ctx.Done():
		return time.Time{}
	case <-time.After(time.Until(at)):
	}
	c := exec.Command("sh", "-c", cmd)
	c.Stdout, c.Stderr = log, os.Stderr
	err := c.Run()
	done := time.Now()
	if err != nil {
		fmt.Fprintf(log, "command %q: %v\n", cmd, err)
	} else {
		fmt.Fprintf(log, "ran %q (took %v)\n", cmd, done.Sub(at).Round(time.Millisecond))
	}
	return done
}

func faultReport(cfg Config, tl *timeline, n int, health []healthSample, probes []ProbeSample, faultAt, recoverAt time.Time) *FaultReport {
	f := &FaultReport{FaultCmd: cfg.FaultCmd, RecoverCmd: cfg.RecoverCmd, DetectionMS: -1, ProbeFirstSuccessMS: -1}
	if faultAt.IsZero() {
		return f
	}
	ms := func(t time.Time) float64 { return float64(t.Sub(tl.start).Microseconds()) / 1e3 }
	f.FaultAtMS = ms(faultAt)
	fi := tl.idx(faultAt)
	var baseline []int64
	for i := 0; i < n; i++ {
		b := &tl.buckets[i]
		h := b.hist.Load()
		if i < fi {
			f.ErrorsBeforeFault += b.errs.Load()
			if h != nil {
				baseline = append(baseline, h.Snapshot().P99US)
			}
			continue
		}
		f.ErrorsAfterFault += b.errs.Load()
		if h != nil {
			s := h.Snapshot()
			f.PeakP99AfterFaultUS = max(f.PeakP99AfterFaultUS, s.P99US)
			f.MaxLatencyAfterFault = max(f.MaxLatencyAfterFault, s.MaxUS)
		}
	}
	if len(baseline) > 0 {
		// Median of per-interval p99s before the fault: robust to one noisy interval.
		slices.Sort(baseline)
		f.BaselineP99US = baseline[len(baseline)/2]
	}
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
	var before []float64
	for _, p := range probes {
		if p.StartMS+p.LatencyUS/1e3 < f.FaultAtMS && p.OK {
			before = append(before, p.LatencyUS)
		}
	}
	slow := math.Inf(1)
	if len(before) > 0 {
		slices.Sort(before)
		slow = 10 * before[len(before)/2]
	}
	for _, p := range probes {
		end := p.StartMS + p.LatencyUS/1e3
		if end < f.FaultAtMS {
			continue
		}
		if !p.OK {
			f.ProbeFailures++
			continue
		}
		f.ProbeMaxLatencyUS = max(f.ProbeMaxLatencyUS, p.LatencyUS)
		if f.ProbeFirstSuccessMS < 0 {
			f.ProbeFirstSuccessMS = end - f.FaultAtMS
		}
		if p.LatencyUS > slow {
			f.ProbeSlowAfterFault++
			f.ProbeStallEndMS = max(f.ProbeStallEndMS, end-f.FaultAtMS)
		}
	}
	return f
}
