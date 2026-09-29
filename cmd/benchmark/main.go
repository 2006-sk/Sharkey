// Command benchmark generates load against a running cluster and reports
// measured throughput and latency, or runs the offline key-redistribution
// experiment.
//
//	benchmark --address localhost:7100 --clients 100 --requests 1000000 \
//	          --read-ratio 0.8 --keyspace 100000 --distribution zipf
//
//	benchmark rebalance --keys 100000
//
// Two modes share one binary:
//
//   - Load mode (the default) drives a LIVE cluster. The engine is
//     internal/bench.Run: it opens one TCP connection per simulated client,
//     each with exactly one request outstanding (a closed-loop generator:
//     a client sends its next request only after the previous answer
//     arrives), optionally preloads every key and warms up, then measures for
//     --duration or --requests. Every latency is kept, so percentiles are
//     exact. Failure tests add a timeline, a probe key and fault commands
//     run on the benchmark's own clock. This file only maps flags onto
//     bench.Config and prints/saves the Result.
//
//   - `rebalance` mode needs no cluster at all. It is a pure computation over
//     the hash ring: how many keys change owner when nodes are added or
//     removed, compared with naive hash % N sharding, and how evenly keys
//     spread for different virtual-node counts (Test E in the README).
//
// Every run can write its complete result as JSON (--out). The scripts save
// those files under benchmarks/results/, and scripts/summarize.py builds all
// README tables and graphs from them, so no reported number is typed by hand.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"distkv/internal/bench"
	"distkv/internal/cmdutil"
	"distkv/internal/consistenthash"
)

// main dispatches on an optional leading sub-command word. `rebalance` must be
// the FIRST argument; anything else (including no arguments at all) is load
// mode, so the common case needs no sub-command. Each mode parses its own
// flags with its own FlagSet, so `benchmark rebalance -h` lists only the
// rebalance flags.
func main() {
	if len(os.Args) > 1 && os.Args[1] == "rebalance" {
		rebalance(os.Args[2:])
		return
	}
	load(os.Args[1:])
}

// load runs a live load test. The flags bind DIRECTLY to the fields of a
// bench.Config value via the XxxVar functions (pointer to the field), so
// after fs.Parse the struct is fully populated with no copying step. The same
// Config is embedded in the JSON result, which makes every result file
// self-describing: it records exactly which parameters produced it.
func load(args []string) {
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	var c bench.Config
	// --- identity and target ---
	// Free-form name stored in the result (scripts encode the configuration
	// in it, e.g. nodes and repeat number).
	fs.StringVar(&c.Label, "label", "benchmark", "name recorded in the result")
	// The coordinator: clients talk only to it, never to nodes directly.
	fs.StringVar(&c.Address, "address", "localhost:7100", "coordinator address")
	// --- load shape ---
	// Number of simulated clients = number of TCP connections = maximum
	// requests in flight. In a closed loop this, not a target rate, sets the
	// offered load: throughput ends up near clients / latency (Little's law).
	fs.IntVar(&c.Clients, "clients", 50, "concurrent clients (one connection each)")
	// Run length: either a fixed operation count or a fixed duration.
	fs.Int64Var(&c.Requests, "requests", 0, "total measured operations (0 = use --duration)")
	fs.DurationVar(&c.Duration, "duration", 10*time.Second, "measured duration when --requests is 0")
	// Warm-up runs the same workload unmeasured first, so connection set-up,
	// cache filling and Go runtime start-up effects (heap growth, goroutine
	// stacks) do not pollute the measured numbers. The suites use 3 s.
	fs.DurationVar(&c.Warmup, "warmup", 0, "unmeasured warm-up duration before measuring")
	// Each operation is a GET with this probability, otherwise a PUT.
	fs.Float64Var(&c.ReadRatio, "read-ratio", 0.8, "fraction of operations that are GETs")
	// Keys are bench-00000000 .. bench-(keyspace-1).
	fs.IntVar(&c.Keyspace, "keyspace", 100000, "number of distinct keys")
	fs.IntVar(&c.ValueSize, "value-size", 100, "value size in bytes")
	// uniform: every key equally likely. zipf: a few keys are very hot, the
	// shape of real traffic, which is what makes a cache useful (Test B).
	fs.StringVar(&c.Distribution, "distribution", "uniform", "key distribution: uniform | zipf")
	// Skew of the Zipf distribution; must be strictly between 0 and 1 for the
	// Gray et al. generator used here. 0.99 is YCSB's default.
	fs.Float64Var(&c.ZipfTheta, "zipf-theta", 0.99, "Zipf skew parameter in (0,1); YCSB default 0.99")
	// Without a preload, early GETs would hit missing keys, which are cheaper
	// to answer (NOT FOUND) and would flatter the read latency.
	fs.BoolVar(&c.Preload, "preload", true, "PUT every key once before measuring so GETs hit existing keys")
	// A request slower than this counts as an error and its connection is
	// replaced, because its state is unknown after a timeout.
	fs.DurationVar(&c.RequestTimeout, "timeout", 5*time.Second, "per-request timeout")
	// Seeds each client's private random generator. Repeats use different
	// seeds so they are independent samples; a fixed seed makes the key and
	// operation sequence reproducible.
	fs.Uint64Var(&c.Seed, "seed", 0, "random seed (0 = time-based)")
	// --- failure-test instrumentation ---
	// Width of the time buckets for ops/s, errors, p99 and healthy-node count
	// over time. Test D uses 100 ms to resolve the failure transient.
	fs.DurationVar(&c.TimelineInterval, "timeline-interval", time.Second, "bucket width for the throughput/latency timeline")
	// A probe is a separate connection repeatedly GETting one chosen key,
	// typically one whose primary is the node about to be killed. It shows
	// exactly what a single user of that key experiences during the fault.
	fs.StringVar(&c.ProbeKey, "probe-key", "", "also GET this key continuously on a separate connection")
	fs.DurationVar(&c.ProbeInterval, "probe-interval", 10*time.Millisecond, "delay between probe GETs")
	// Fault injection. The benchmark runs these shell commands itself at the
	// given offsets from the start of measurement. Doing it on the
	// benchmark's clock (rather than from a separate script with `sleep`)
	// puts the fault instant on the same time axis as the timeline and the
	// probes, so detection and recovery times can be computed precisely.
	fs.DurationVar(&c.FaultAt, "fault-at", 0, "run --fault-cmd this long after measuring starts")
	fs.StringVar(&c.FaultCmd, "fault-cmd", "", "shell command injecting a fault (e.g. kill -9 <pid>)")
	fs.DurationVar(&c.RecoverAt, "recover-at", 0, "run --recover-cmd this long after measuring starts")
	fs.StringVar(&c.RecoverCmd, "recover-cmd", "", "shell command undoing the fault")
	// --- output ---
	// JSON result path; the human-readable summary always goes to stdout.
	out := fs.String("out", "", "write the JSON result to this file")
	// Silence progress messages (preload/warm-up/running notes).
	quiet := fs.Bool("quiet", false, "suppress progress messages")
	// Profile the LOAD GENERATOR itself, to check that the client is not the
	// bottleneck (a benchmark that saturates its own CPU measures itself).
	cpuProfile := fs.String("cpuprofile", "", "write a CPU profile of the benchmark client to this file")
	fs.Parse(args)
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		// Sampling starts now and the deferred Stop flushes the profile when
		// load returns. Note: the os.Exit(1) paths below skip deferred calls,
		// so a failed run leaves an incomplete profile.
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Progress messages go to stderr so that stdout carries only the result
	// summary. With --quiet they go to /dev/null instead. os.Open opens it
	// read-only, so the writes actually fail with an error, but bench.Run
	// ignores errors from its progress writes, so the effect is silence.
	logger := os.Stderr
	if *quiet {
		logger, _ = os.Open(os.DevNull)
	}
	// One descriptor per simulated client: 250 clients would exceed macOS's
	// usual default soft limit of 256 once stdio and other files are counted.
	cmdutil.RaiseFileLimit(log.New(os.Stderr, "benchmark ", 0))
	// Ctrl-C cancels ctx; bench.Run's workers check it and stop early.
	ctx, stop := cmdutil.SignalContext()
	defer stop()

	// The whole experiment: connect, preload, warm up, measure, collect.
	res, err := bench.Run(ctx, c, logger)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchmark:", err)
		os.Exit(1)
	}
	// Human-readable summary on stdout.
	res.Print(os.Stdout)
	// Machine-readable JSON for summarize.py (creates parent directories).
	if *out != "" {
		if err := res.WriteJSON(*out); err != nil {
			fmt.Fprintln(os.Stderr, "write result:", err)
			os.Exit(1)
		}
		fmt.Printf("result written to %s\n", *out)
	}
}

// rebalance runs the offline key-redistribution experiment (Test E). It needs
// no cluster: bench.Rebalance builds hash rings in memory and counts, for
// --keys synthetic keys, how many change owner.
//
// Why it matters: with naive modulo sharding (owner = hash(key) % N) going
// from N to N+1 nodes changes the owner of about N/(N+1) of all keys (80% for
// 4 -> 5), which in a real system means moving most of the data. Consistent
// hashing moves only about 1/(N+1) (20% for 4 -> 5), and only to or from the
// node that was added or removed. The README's measured values are 21.5% vs
// 80.0%.
func rebalance(args []string) {
	fs := flag.NewFlagSet("rebalance", flag.ExitOnError)
	keys := fs.Int("keys", 100000, "number of keys")
	// Default = the ring's own default, so the experiment matches the running
	// system's configuration.
	vnodes := fs.Int("vnodes", consistenthash.DefaultVirtualNodes, "virtual nodes per node")
	out := fs.String("out", "", "write the JSON result to this file")
	fs.Parse(args)

	// Membership changes to measure, as {from, to} node counts: growth by
	// one at several sizes, plus two shrinks (5 -> 4, 8 -> 7) to show that
	// removal is symmetric.
	transitions := [][2]int{{1, 2}, {2, 3}, {3, 4}, {4, 5}, {8, 9}, {5, 4}, {8, 7}}
	// Also measure load balance across 4 nodes for 1, 8, 32, 128 and 256
	// vnodes per node, which shows why virtual nodes are needed: with 1 vnode
	// the busiest node held ~44% more than its fair share (README).
	res := bench.Rebalance(*keys, *vnodes, transitions, []int{1, 8, 32, 128, 256}, 4)

	// Table 1: keys moved per transition. "ideal" is the theoretical minimum
	// (|to - from| / max(from, to)); "only affected nodes" is the invariant
	// that every moved key went to an added node or came from a removed one.
	fmt.Printf("\n=== key redistribution: %d keys, %d vnodes/node ===\n", res.Keys, res.VirtualNodes)
	fmt.Printf("%-10s %22s %18s %12s %s\n", "change", "consistent hash moved", "modulo moved", "ideal", "only affected nodes")
	for _, t := range res.Transitions {
		fmt.Printf("%2d -> %-4d %13d (%5.2f%%) %9d (%5.2f%%) %10.2f%%  %v\n",
			t.From, t.To, t.ConsistentMoved, t.ConsistentMovedPct, t.ModuloMoved, t.ModuloMovedPct, t.IdealPct, t.OnlyAffectedMoved)
	}
	// Table 2: spread of keys per node for each vnode count. max/ideal of
	// 1.0 would be perfect balance; stddev is relative to the ideal load.
	fmt.Printf("\nload balance across %d nodes:\n", res.Balance[0].Nodes)
	fmt.Printf("%-8s %9s %9s %14s %12s\n", "vnodes", "min keys", "max keys", "max/ideal", "stddev %")
	for _, b := range res.Balance {
		fmt.Printf("%-8d %9d %9d %14.3f %11.2f%%\n", b.VirtualNodes, b.MinKeys, b.MaxKeys, b.MaxOverIdeal, b.StdDevPct)
	}
	if *out != "" {
		// Create the results directory if needed; 0o755 = rwxr-xr-x.
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		// Marshalling plain structs of numbers cannot fail, so the error is
		// ignored. Indented JSON keeps result files diff-friendly.
		b, _ := json.MarshalIndent(res, "", "  ")
		// Trailing newline so the file is a well-formed text file.
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("result written to %s\n", *out)
	}
}
