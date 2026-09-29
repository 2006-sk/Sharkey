package bench

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
func summarize(samples []time.Duration) LatencySummary {
	s := LatencySummary{Count: len(samples)}
	if len(samples) == 0 {
		return s
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var sum time.Duration
	for _, d := range samples {
		sum += d
	}
	us := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e3 }
	pct := func(p float64) float64 {
		// Nearest-rank: the smallest sample with at least p of samples <= it.
		idx := int(float64(len(samples))*p+0.999999) - 1
		idx = max(0, min(idx, len(samples)-1))
		return us(samples[idx])
	}
	s.MeanUS = us(sum) / float64(len(samples))
	s.P50US, s.P95US, s.P99US, s.P999US = pct(0.50), pct(0.95), pct(0.99), pct(0.999)
	s.MaxUS = us(samples[len(samples)-1])
	return s
}

// OpResult summarises one operation type.
type OpResult struct {
	Count     int64          `json:"count"`
	Errors    int64          `json:"errors"`
	NotFound  int64          `json:"not_found,omitempty"`
	OpsPerSec float64        `json:"ops_per_sec"`
	Latency   LatencySummary `json:"latency"`
}

// CacheDelta is the change in cluster-wide cache counters during the
// measured phase (queried from the coordinator before and after).
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
type CoalescingDelta struct {
	CoordFrames         int64   `json:"coordinator_frames"`
	CoordWrites         int64   `json:"coordinator_write_syscalls"`
	CoordFramesPerWrite float64 `json:"coordinator_frames_per_write"`
	NodeFrames          int64   `json:"node_frames"`
	NodeWrites          int64   `json:"node_write_syscalls"`
	NodeFramesPerWrite  float64 `json:"node_frames_per_write"`
}

// TimelinePoint aggregates one interval of the measured phase.
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
type ProbeSample struct {
	StartMS   float64 `json:"start_ms"`
	LatencyUS float64 `json:"latency_us"`
	OK        bool    `json:"ok"`
	Err       string  `json:"err,omitempty"`
}

// FaultReport measures the impact of an injected fault.
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
func loadAvg() float64 {
	var out []byte
	var err error
	if runtime.GOOS == "linux" {
		out, err = os.ReadFile("/proc/loadavg")
	} else {
		out, err = exec.Command("sysctl", "-n", "vm.loadavg").Output()
	}
	if err != nil {
		return -1
	}
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

func hostInfo() HostInfo {
	h, _ := os.Hostname()
	return HostInfo{GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), Hostname: h, LoadAvgStart: loadAvg()}
}

// Result is the complete output of one benchmark run.
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
func (r *Result) Print(w io.Writer) {
	c := r.Config
	fmt.Fprintf(w, "\n=== %s ===\n", r.Label)
	fmt.Fprintf(w, "target=%s clients=%d read_ratio=%.2f keyspace=%d value=%dB dist=%s cluster_nodes=%d\n",
		c.Address, c.Clients, c.ReadRatio, c.Keyspace, c.ValueSize, c.Distribution, r.ClusterNodes)
	fmt.Fprintf(w, "total ops      %d (ok %d, errors %d)\n", r.TotalOps, r.Successes, r.Errors)
	fmt.Fprintf(w, "elapsed        %.2fs\n", r.ElapsedSec)
	fmt.Fprintf(w, "throughput     %.0f ops/s  (GET %.0f/s, PUT %.0f/s)\n", r.OpsPerSec, r.Get.OpsPerSec, r.Put.OpsPerSec)
	l := r.Latency
	fmt.Fprintf(w, "latency (us)   p50 %.0f  p95 %.0f  p99 %.0f  p99.9 %.0f  max %.0f  mean %.0f\n", l.P50US, l.P95US, l.P99US, l.P999US, l.MaxUS, l.MeanUS)
	for _, op := range []struct {
		name string
		r    OpResult
	}{{"GET", r.Get}, {"PUT", r.Put}} {
		l := op.r.Latency
		fmt.Fprintf(w, "  %-4s n=%-9d err=%-6d p50 %.0f  p95 %.0f  p99 %.0f  max %.0f\n", op.name, op.r.Count, op.r.Errors, l.P50US, l.P95US, l.P99US, l.MaxUS)
	}
	if r.Get.NotFound > 0 {
		fmt.Fprintf(w, "GET not found  %d\n", r.Get.NotFound)
	}
	if r.Cache.Available {
		fmt.Fprintf(w, "cache          hits %d  misses %d  hit rate %.2f%%  evictions %d\n", r.Cache.Hits, r.Cache.Misses, r.Cache.HitRate*100, r.Cache.Evictions)
	} else {
		fmt.Fprintf(w, "cache          (stats unavailable)\n")
	}
	for e, n := range r.ErrorTypes {
		fmt.Fprintf(w, "error sample   %dx %s\n", n, e)
	}
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
