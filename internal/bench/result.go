package bench

// result.go defines what one benchmark run PRODUCES: the Result structure
// that cmd/benchmark serialises to JSON, the exact percentile computation,
// host metadata, and the human-readable text report.
//
// # Why raw JSON results
//
// Every run writes one JSON file (benchmarks/results/<transport>/<suite>/).
// scripts/summarize.py later reads ALL of them and generates the README's
// tables, benchmarks/SUMMARY.md and the graphs. Nothing is typed by hand, so
// every published number can be traced back to a raw file, and the analysis
// can be changed (e.g. median instead of mean across repeats) without
// re-running hours of benchmarks. That is why the Result is deliberately
// verbose: config, host, cluster shape, totals, percentiles, cache and
// batching deltas, the timeline, raw probe samples and the fault analysis.
//
// # Exact percentiles vs histograms
//
// A histogram (like internal/metrics.Histogram, used by the servers) puts
// each sample into a bucket and reports a bucket boundary as the percentile:
// constant memory, bounded relative error (~3% there). A benchmark run is
// finite, so this package can afford to keep EVERY sample, sort them, and
// read off the exact nearest-rank percentile. No estimation error means small
// differences between configurations (e.g. cache on/off, where the README
// reports ~0% change in GET p99) are not artefacts of bucket boundaries.

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// LatencySummary holds exact percentiles computed from every recorded
// sample (not from a histogram), in microseconds.
//
// Count is the number of samples; MeanUS the arithmetic mean; P50US (the
// median), P95US, P99US and P999US (p99.9) the nearest-rank percentiles; and
// MaxUS the single slowest sample. The tail (p99, p99.9, max) matters most
// for user experience: a page that makes 100 requests hits the p99 latency
// on most page loads. The mean hides the tail and is kept only for
// reference. Values are float64 microseconds so sub-µs precision survives
// the JSON round trip.
type LatencySummary struct {
	Count  int     `json:"count"`
	MeanUS float64 `json:"mean_us"`
	P50US  float64 `json:"p50_us"`
	P95US  float64 `json:"p95_us"`
	P99US  float64 `json:"p99_us"`
	P999US float64 `json:"p999_us"`
	MaxUS  float64 `json:"max_us"`
}

// summarize sorts samples in place and computes nearest-rank percentiles.
//
// The nearest-rank definition: the p-th percentile of n sorted samples is the
// sample at 1-based rank ceil(p·n). It is always an ACTUAL observed value
// (never an interpolation between two samples), and it satisfies "at least a
// fraction p of the samples are <= it". Example from TestSummarizePercentiles:
// samples 1..1000 µs → p50 = rank 500 = 500 µs, p99 = rank 990 = 990 µs.
//
// Cost: O(n log n) for the sort. With ~750k samples per run that is well
// under a second, paid once after the measured phase, never during it.
//
// "In place" matters to callers: the slice they pass is reordered. aggregate
// passes freshly built slices, so nothing else observes the reordering.
func summarize(samples []time.Duration) LatencySummary {
	s := LatencySummary{Count: len(samples)}
	// No samples (e.g. a PUT-free workload): all-zero summary, and no index
	// out of range below.
	if len(samples) == 0 {
		return s
	}
	// Ascending order: samples[0] is the fastest, samples[n-1] the slowest.
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	// Sum for the mean. time.Duration is int64 nanoseconds: overflow would
	// need ~292 years of summed latency, so it is not a concern.
	var sum time.Duration
	for _, d := range samples {
		sum += d
	}
	// Convert a duration to fractional microseconds, the unit of every
	// latency field in the JSON.
	us := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e3 }
	pct := func(p float64) float64 {
		// Nearest-rank: the smallest sample with at least p of samples <= it.
		//
		// rank = ceil(n·p) (1-based), index = rank − 1. The ceiling is written
		// as "add 0.999999 and truncate" instead of math.Ceil because n·p is
		// computed in floating point: 1000·0.95 may come out as
		// 950.0000000001, which math.Ceil would round UP to 951, one rank too
		// high. Adding slightly less than 1 treats such values as the integer
		// they were meant to be, while any genuinely fractional n·p (fraction
		// above 0.000001) still rounds up.
		idx := int(float64(len(samples))*p+0.999999) - 1
		// Clamp into [0, n-1] for tiny n or p close to 0 or 1.
		idx = max(0, min(idx, len(samples)-1))
		return us(samples[idx])
	}
	s.MeanUS = us(sum) / float64(len(samples))
	s.P50US, s.P95US, s.P99US, s.P999US = pct(0.50), pct(0.95), pct(0.99), pct(0.999)
	// The maximum is simply the last sorted sample (the 100th percentile).
	s.MaxUS = us(samples[len(samples)-1])
	return s
}

// OpResult summarises one operation type.
//
// Count is ATTEMPTS (successes + Errors). NotFound counts GETs that
// succeeded but found no value (should be 0 when the keyspace was
// preloaded; a non-zero value would mean data loss or a missing preload).
// OpsPerSec counts successes only, and Latency covers successful operations
// only: failed operations are reported by count, not mixed into percentiles.
type OpResult struct {
	Count     int64          `json:"count"`
	Errors    int64          `json:"errors"`
	NotFound  int64          `json:"not_found,omitempty"`
	OpsPerSec float64        `json:"ops_per_sec"`
	Latency   LatencySummary `json:"latency"`
}

// CacheDelta is the change in cluster-wide cache counters during the
// measured phase (queried from the coordinator before and after).
//
// Why a delta: the nodes' counters are cumulative since process start, so
// reading them once after the run would also count the preload and warm-up
// (warm-up traffic in particular fills the cache and would inflate misses).
// Subtracting a snapshot taken just before the measured phase isolates it.
// HitRate = Hits / (Hits + Misses). Available is false when either STATS
// call failed, so a missing measurement is never confused with "0 hits".
type CacheDelta struct {
	Hits      int64   `json:"hits"`
	Misses    int64   `json:"misses"`
	Evictions int64   `json:"evictions"`
	HitRate   float64 `json:"hit_rate"`
	Available bool    `json:"available"`
}

// CoalescingDelta is the change in write-coalescing counters (frames vs
// write syscalls) during the measured phase: >1 frames per write means
// responses/requests shared syscalls.
//
// "Coord" fields are the coordinator's own writes (responses to clients and
// requests to nodes); "Node" fields are summed over all storage nodes. The
// README's scaling analysis uses NodeFramesPerWrite: ~9.2 with 1 node falling
// to ~1.0 with 8 nodes, because the same load spread over more connections
// leaves fewer frames waiting to be batched into each syscall.
type CoalescingDelta struct {
	CoordFrames         int64   `json:"coordinator_frames"`
	CoordWrites         int64   `json:"coordinator_write_syscalls"`
	CoordFramesPerWrite float64 `json:"coordinator_frames_per_write"`
	NodeFrames          int64   `json:"node_frames"`
	NodeWrites          int64   `json:"node_write_syscalls"`
	NodeFramesPerWrite  float64 `json:"node_frames_per_write"`
}

// TimelinePoint aggregates one interval of the measured phase.
//
// TMS is the interval's start in ms since the measured phase began. Ops and
// Errors count operations that STARTED in the interval. P99US and MaxUS come
// from the interval's histogram (approximate, ≤ ~3% error). HealthyNodes is
// the coordinator's healthy count sampled in the interval (-1 = no sample).
// OpsPerSec is Ops scaled to a per-second rate. The failure timeline graph
// plots these points.
type TimelinePoint struct {
	TMS          int64   `json:"t_ms"`
	Ops          int64   `json:"ops"`
	Errors       int64   `json:"errors"`
	P99US        int64   `json:"p99_us"`
	MaxUS        int64   `json:"max_us"`
	HealthyNodes int     `json:"healthy_nodes"`
	OpsPerSec    float64 `json:"ops_per_sec"`
}

// ProbeSample is one probe GET of the watched key.
//
// StartMS is when the probe was sent (ms since the measured phase began),
// LatencyUS how long it took (including a reconnect if one was needed), OK
// whether it returned the value, and Err the error text otherwise. Raw
// samples (not a summary) are stored so the analysis can later align them
// exactly with the fault time.
type ProbeSample struct {
	StartMS   float64 `json:"start_ms"`
	LatencyUS float64 `json:"latency_us"`
	OK        bool    `json:"ok"`
	Err       string  `json:"err,omitempty"`
}

// FaultReport measures the impact of an injected fault.
//
// All times are in milliseconds relative to the start of the measured phase
// (the *AtMS fields) or relative to the fault/recovery itself (the other *MS
// fields). The baseline is taken from the intervals BEFORE the fault so each
// run is compared with itself, not with a different run. See faultReport in
// runner.go for how each field is computed, and the README's Test D table
// ("detected after", "probe worst", "fallback settled",
// "recovered→all healthy") for how they are presented.
type FaultReport struct {
	FaultCmd             string  `json:"fault_cmd"`
	FaultAtMS            float64 `json:"fault_at_ms"`
	RecoverCmd           string  `json:"recover_cmd,omitempty"`
	RecoverAtMS          float64 `json:"recover_at_ms,omitempty"`
	ErrorsBeforeFault    int64   `json:"errors_before_fault"`
	ErrorsAfterFault     int64   `json:"errors_after_fault"`
	BaselineP99US        int64   `json:"baseline_p99_us"`
	PeakP99AfterFaultUS  int64   `json:"peak_interval_p99_after_fault_us"`
	MaxLatencyAfterFault int64   `json:"max_latency_after_fault_us"`
	DetectionMS          float64 `json:"detection_ms"`           // fault -> coordinator reports fewer healthy nodes (-1 = never)
	ProbeFailures        int     `json:"probe_failures"`         // failed probe GETs of the watched key
	ProbeFirstSuccessMS  float64 `json:"probe_first_success_ms"` // fault -> first successful completion of an affected probe
	ProbeStallEndMS      float64 `json:"probe_stall_end_ms"`     // fault -> end of the last slow (>10x median) probe, i.e. fallback fully in effect
	ProbeMaxLatencyUS    float64 `json:"probe_max_latency_after_fault_us"`
	ProbeSlowAfterFault  int     `json:"probe_slow_after_fault"`              // probes after fault slower than 10x baseline median
	RecoveredToHealthyMS float64 `json:"recover_to_all_healthy_ms,omitempty"` // recover cmd -> all nodes healthy (-1 = never)
}

// HostInfo records where the benchmark ran.
//
// Benchmark numbers are meaningless without their environment: the README's
// results all come from one machine where the load generator, coordinator
// and nodes share the CPUs. Recording Go version, OS, CPU count and hostname
// in every result lets the summary state that environment from the data
// itself. The load averages were added after a run was spoiled by other
// foreground work on the laptop (README "Benchmark hygiene note").
type HostInfo struct {
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"cpus"`
	Hostname  string `json:"hostname"`
	// 1-minute load average of the whole machine at the start and end of the
	// run: other activity on the machine shows up here as noise.
	LoadAvgStart float64 `json:"load_avg_1m_start"`
	LoadAvgEnd   float64 `json:"load_avg_1m_end"`
}

// loadAvg returns the 1-minute load average, or -1 if unavailable.
//
// The load average is the (exponentially smoothed) number of runnable
// threads/processes. On a 15-core machine a value well above what the
// benchmark itself explains signals background activity that may have
// perturbed the run. Linux exposes it in /proc/loadavg ("0.52 0.58 0.59
// ..."); macOS via `sysctl -n vm.loadavg`, which prints "{ 1.23 1.45 1.67 }"
// (hence the brace trimming). -1 keeps "unknown" distinct from a real 0.
func loadAvg() float64 {
	var out []byte
	var err error
	// Pick the source for this OS.
	if runtime.GOOS == "linux" {
		out, err = os.ReadFile("/proc/loadavg")
	} else {
		out, err = exec.Command("sysctl", "-n", "vm.loadavg").Output()
	}
	if err != nil {
		return -1
	}
	// Strip whitespace and macOS's braces, split on spaces, and take the
	// first field: the 1-minute average.
	fields := strings.Fields(strings.Trim(strings.TrimSpace(string(out)), "{}"))
	if len(fields) == 0 {
		return -1
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return -1
	}
	return v
}

// hostInfo captures the environment at the start of a run. LoadAvgEnd is
// filled in by Run after the measured phase. A hostname error is ignored
// (the field just stays empty): metadata must never fail a benchmark.
func hostInfo() HostInfo {
	h, _ := os.Hostname()
	return HostInfo{GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), Hostname: h, LoadAvgStart: loadAvg()}
}

// Result is the complete output of one benchmark run.
//
// Field groups (blank lines separate them in the struct):
//
//   - Identity and context: Label, StartedAt (UTC), Host, the full Config,
//     the coordinator's cluster configuration as raw JSON (Cluster; kept raw
//     so this package need not mirror its schema) and ClusterNodes.
//   - Totals: TotalOps = Successes + Errors, ElapsedSec, OpsPerSec
//     (successes per second), overall Latency, per-operation Get/Put, the
//     Cache and Coalescing deltas, and ErrorTypes (up to 10 distinct error
//     messages with counts).
//   - Time series: Timeline (per-interval points), Probe (raw probe samples)
//     and Fault (nil unless a fault command was configured).
//
// omitempty fields disappear from the JSON when unused, so a plain run's file
// is not cluttered with empty fault sections.
type Result struct {
	Label        string          `json:"label"`
	StartedAt    time.Time       `json:"started_at"`
	Host         HostInfo        `json:"host"`
	Config       Config          `json:"config"`
	Cluster      json.RawMessage `json:"cluster_config,omitempty"`
	ClusterNodes int             `json:"cluster_nodes"`

	TotalOps   int64            `json:"total_ops"`
	Successes  int64            `json:"successes"`
	Errors     int64            `json:"errors"`
	ElapsedSec float64          `json:"elapsed_sec"`
	OpsPerSec  float64          `json:"ops_per_sec"`
	Latency    LatencySummary   `json:"latency"`
	Get        OpResult         `json:"get"`
	Put        OpResult         `json:"put"`
	Cache      CacheDelta       `json:"cache"`
	Coalescing *CoalescingDelta `json:"write_coalescing,omitempty"`
	ErrorTypes map[string]int   `json:"error_samples,omitempty"`

	Timeline []TimelinePoint `json:"timeline,omitempty"`
	Probe    []ProbeSample   `json:"probe,omitempty"`
	Fault    *FaultReport    `json:"fault,omitempty"`
}

// WriteJSON saves the result, creating parent directories.
//
// Indented JSON is larger but human-diffable and readable in any editor,
// useful when checking a surprising number by hand. The trailing newline
// keeps the file POSIX-friendly (tools like `cat` and git expect it).
// Permissions 0o755 (dirs) / 0o644 (file) = owner writes, everyone reads.
func (r *Result) WriteJSON(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// Print writes a human-readable report.
//
// This is what a person sees in the terminal after `bin/benchmark`; the JSON
// file is what the tooling consumes. Latencies are printed in whole
// microseconds. The fault section appears only for fault-injection runs.
func (r *Result) Print(w io.Writer) {
	c := r.Config
	// Header: label and the parameters needed to interpret the numbers.
	fmt.Fprintf(w, "\n=== %s ===\n", r.Label)
	fmt.Fprintf(w, "target=%s clients=%d read_ratio=%.2f keyspace=%d value=%dB dist=%s cluster_nodes=%d\n",
		c.Address, c.Clients, c.ReadRatio, c.Keyspace, c.ValueSize, c.Distribution, r.ClusterNodes)
	// Totals and throughput (successful operations per second).
	fmt.Fprintf(w, "total ops      %d (ok %d, errors %d)\n", r.TotalOps, r.Successes, r.Errors)
	fmt.Fprintf(w, "elapsed        %.2fs\n", r.ElapsedSec)
	fmt.Fprintf(w, "throughput     %.0f ops/s  (GET %.0f/s, PUT %.0f/s)\n", r.OpsPerSec, r.Get.OpsPerSec, r.Put.OpsPerSec)
	// Overall exact percentiles, then the same per operation type (an
	// anonymous struct slice avoids repeating the Fprintf for GET and PUT).
	l := r.Latency
	fmt.Fprintf(w, "latency (us)   p50 %.0f  p95 %.0f  p99 %.0f  p99.9 %.0f  max %.0f  mean %.0f\n", l.P50US, l.P95US, l.P99US, l.P999US, l.MaxUS, l.MeanUS)
	for _, op := range []struct {
		name string
		r    OpResult
	}{{"GET", r.Get}, {"PUT", r.Put}} {
		l := op.r.Latency
		fmt.Fprintf(w, "  %-4s n=%-9d err=%-6d p50 %.0f  p95 %.0f  p99 %.0f  max %.0f\n", op.name, op.r.Count, op.r.Errors, l.P50US, l.P95US, l.P99US, l.MaxUS)
	}
	// Only shown when non-zero: after a preload it indicates a problem.
	if r.Get.NotFound > 0 {
		fmt.Fprintf(w, "GET not found  %d\n", r.Get.NotFound)
	}
	// Cache delta for the measured phase, if both STATS snapshots worked.
	if r.Cache.Available {
		fmt.Fprintf(w, "cache          hits %d  misses %d  hit rate %.2f%%  evictions %d\n", r.Cache.Hits, r.Cache.Misses, r.Cache.HitRate*100, r.Cache.Evictions)
	} else {
		fmt.Fprintf(w, "cache          (stats unavailable)\n")
	}
	// Sampled error messages (map order, so the order varies between runs).
	for e, n := range r.ErrorTypes {
		fmt.Fprintf(w, "error sample   %dx %s\n", n, e)
	}
	// Fault-injection summary: the same numbers as the README's Test D row.
	if f := r.Fault; f != nil {
		fmt.Fprintf(w, "--- fault injection ---\n")
		fmt.Fprintf(w, "fault at %.0fms: %s\n", f.FaultAtMS, f.FaultCmd)
		fmt.Fprintf(w, "errors before/after fault      %d / %d\n", f.ErrorsBeforeFault, f.ErrorsAfterFault)
		fmt.Fprintf(w, "baseline p99 / peak p99 after  %dus / %dus (max single op %dus)\n", f.BaselineP99US, f.PeakP99AfterFaultUS, f.MaxLatencyAfterFault)
		fmt.Fprintf(w, "failure detected after         %.0fms\n", f.DetectionMS)
		fmt.Fprintf(w, "probe key: failures %d, first success %.1fms after fault, max latency %.0fus, slow probes %d (last ended %.0fms after fault)\n",
			f.ProbeFailures, f.ProbeFirstSuccessMS, f.ProbeMaxLatencyUS, f.ProbeSlowAfterFault, f.ProbeStallEndMS)
		if f.RecoverCmd != "" {
			fmt.Fprintf(w, "recovered at %.0fms; all nodes healthy %.0fms later\n", f.RecoverAtMS, f.RecoveredToHealthyMS)
		}
	}
}
