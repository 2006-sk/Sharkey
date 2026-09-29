// Command benchmark generates load against a running cluster and reports
// measured throughput and latency, or runs the offline key-redistribution
// experiment.
//
//	benchmark --address localhost:7100 --clients 100 --requests 1000000 \
//	          --read-ratio 0.8 --keyspace 100000 --distribution zipf
//
//	benchmark rebalance --keys 100000
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

func main() {
	if len(os.Args) > 1 && os.Args[1] == "rebalance" {
		rebalance(os.Args[2:])
		return
	}
	load(os.Args[1:])
}

func load(args []string) {
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	var c bench.Config
	fs.StringVar(&c.Label, "label", "benchmark", "name recorded in the result")
	fs.StringVar(&c.Address, "address", "localhost:7100", "coordinator address")
	fs.IntVar(&c.Clients, "clients", 50, "concurrent clients (one connection each)")
	fs.Int64Var(&c.Requests, "requests", 0, "total measured operations (0 = use --duration)")
	fs.DurationVar(&c.Duration, "duration", 10*time.Second, "measured duration when --requests is 0")
	fs.DurationVar(&c.Warmup, "warmup", 0, "unmeasured warm-up duration before measuring")
	fs.Float64Var(&c.ReadRatio, "read-ratio", 0.8, "fraction of operations that are GETs")
	fs.IntVar(&c.Keyspace, "keyspace", 100000, "number of distinct keys")
	fs.IntVar(&c.ValueSize, "value-size", 100, "value size in bytes")
	fs.StringVar(&c.Distribution, "distribution", "uniform", "key distribution: uniform | zipf")
	fs.Float64Var(&c.ZipfTheta, "zipf-theta", 0.99, "Zipf skew parameter in (0,1); YCSB default 0.99")
	fs.BoolVar(&c.Preload, "preload", true, "PUT every key once before measuring so GETs hit existing keys")
	fs.DurationVar(&c.RequestTimeout, "timeout", 5*time.Second, "per-request timeout")
	fs.Uint64Var(&c.Seed, "seed", 0, "random seed (0 = time-based)")
	fs.DurationVar(&c.TimelineInterval, "timeline-interval", time.Second, "bucket width for the throughput/latency timeline")
	fs.StringVar(&c.ProbeKey, "probe-key", "", "also GET this key continuously on a separate connection")
	fs.DurationVar(&c.ProbeInterval, "probe-interval", 10*time.Millisecond, "delay between probe GETs")
	fs.DurationVar(&c.FaultAt, "fault-at", 0, "run --fault-cmd this long after measuring starts")
	fs.StringVar(&c.FaultCmd, "fault-cmd", "", "shell command injecting a fault (e.g. kill -9 <pid>)")
	fs.DurationVar(&c.RecoverAt, "recover-at", 0, "run --recover-cmd this long after measuring starts")
	fs.StringVar(&c.RecoverCmd, "recover-cmd", "", "shell command undoing the fault")
	out := fs.String("out", "", "write the JSON result to this file")
	quiet := fs.Bool("quiet", false, "suppress progress messages")
	cpuProfile := fs.String("cpuprofile", "", "write a CPU profile of the benchmark client to this file")
	fs.Parse(args)
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	logger := os.Stderr
	if *quiet {
		logger, _ = os.Open(os.DevNull)
	}
	cmdutil.RaiseFileLimit(log.New(os.Stderr, "benchmark ", 0))
	ctx, stop := cmdutil.SignalContext()
	defer stop()

	res, err := bench.Run(ctx, c, logger)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchmark:", err)
		os.Exit(1)
	}
	res.Print(os.Stdout)
	if *out != "" {
		if err := res.WriteJSON(*out); err != nil {
			fmt.Fprintln(os.Stderr, "write result:", err)
			os.Exit(1)
		}
		fmt.Printf("result written to %s\n", *out)
	}
}

func rebalance(args []string) {
	fs := flag.NewFlagSet("rebalance", flag.ExitOnError)
	keys := fs.Int("keys", 100000, "number of keys")
	vnodes := fs.Int("vnodes", consistenthash.DefaultVirtualNodes, "virtual nodes per node")
	out := fs.String("out", "", "write the JSON result to this file")
	fs.Parse(args)

	transitions := [][2]int{{1, 2}, {2, 3}, {3, 4}, {4, 5}, {8, 9}, {5, 4}, {8, 7}}
	res := bench.Rebalance(*keys, *vnodes, transitions, []int{1, 8, 32, 128, 256}, 4)

	fmt.Printf("\n=== key redistribution: %d keys, %d vnodes/node ===\n", res.Keys, res.VirtualNodes)
	fmt.Printf("%-10s %22s %18s %12s %s\n", "change", "consistent hash moved", "modulo moved", "ideal", "only affected nodes")
	for _, t := range res.Transitions {
		fmt.Printf("%2d -> %-4d %13d (%5.2f%%) %9d (%5.2f%%) %10.2f%%  %v\n",
			t.From, t.To, t.ConsistentMoved, t.ConsistentMovedPct, t.ModuloMoved, t.ModuloMovedPct, t.IdealPct, t.OnlyAffectedMoved)
	}
	fmt.Printf("\nload balance across %d nodes:\n", res.Balance[0].Nodes)
	fmt.Printf("%-8s %9s %9s %14s %12s\n", "vnodes", "min keys", "max keys", "max/ideal", "stddev %")
	for _, b := range res.Balance {
		fmt.Printf("%-8d %9d %9d %14.3f %11.2f%%\n", b.VirtualNodes, b.MinKeys, b.MaxKeys, b.MaxOverIdeal, b.StdDevPct)
	}
	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("result written to %s\n", *out)
	}
}
