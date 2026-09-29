#!/usr/bin/env python3
"""Turn raw benchmark JSON (benchmarks/results/) into summary tables and graphs.

    python3 scripts/summarize.py [--primary mux]

Every number printed here is read from a result file written by
cmd/benchmark; nothing is typed in by hand. Repeated runs of one
configuration are reduced to the median, with the min-max range shown.

Outputs:
    benchmarks/SUMMARY.md    markdown tables (also injected into README.md between markers)
    benchmarks/graphs/*.png  graphs (needs matplotlib; skipped if missing)
"""
# ---------------------------------------------------------------- overview
# Data flow:  cmd/benchmark --out file.json  ->  benchmarks/results/<tag>/<suite>/
#             -> this script -> SUMMARY.md + README tables + PNG graphs.
#
# <tag> is the transport variant ("mux" = multiplexed, the current one;
# "sequential" = the pre-optimisation baseline). <suite> is one test:
# scaling (A), cache (B), concurrency (C), failure (D), rebalance (E),
# plus profile (coordinator CPU profile).
#
# Each JSON file is one run. Runs of the same configuration share a
# "label" (e.g. "scaling-rf3-n4") and differ in repeat number / seed.
# A table row = one label; each number in it = the MEDIAN over that
# label's runs. Why the median: with 3 repeats, one disturbed run (another
# program on the laptop grabbing the CPU) moves the mean but not the median.
# The min-max range is shown next to throughput so the reader can see the
# spread instead of trusting a single number.
#
# Only the standard library is required for the tables; matplotlib is
# optional and only needed for the graphs.
import argparse
import glob
import json
import os
import statistics
from collections import defaultdict

# Paths are derived from this file's location (scripts/..), so the script
# works from any working directory.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "benchmarks", "results")
GRAPHS = os.path.join(ROOT, "benchmarks", "graphs")

# Reference categorical palette (light surface), fixed slot order.
SURFACE = "#fcfcfb"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]


# ---------------------------------------------------------------- loading
# load: read every *.json in results/<tag>/<suite>/ and group the parsed
# results by their "label" field -> {label: [run, run, run]}. sorted() makes
# the order (and so runs[0], used for example timelines) deterministic.
def load(tag, suite):
    """Group result files of one suite by label."""
    groups = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(RESULTS, tag, suite, "*.json"))):
        with open(path) as f:
            r = json.load(f)
        groups[r["label"]].append(r)
    return groups


# med: apply fn to every run (fn picks one number out of a result dict) and
# return (median, min, max). Every table cell is built from this triple.
def med(runs, fn):
    vals = [fn(r) for r in runs]
    return statistics.median(vals), min(vals), max(vals)


# fmt_rate: just the median of a (median, min, max) triple, with thousands
# separators ("50,695").
def fmt_rate(m):
    return f"{m[0]:,.0f}"


# fmt_rng: "median (min-max)", or only the median when all runs agree.
def fmt_rng(m, unit=""):
    v, lo, hi = m
    if lo == hi:
        return f"{v:,.0f}{unit}"
    return f"{v:,.0f}{unit} ({lo:,.0f}-{hi:,.0f})"


# us: format a latency given in microseconds; switch to ms at >= 1000 us so
# the table stays readable across 60 us ... 1000 ms.
def us(v):
    return f"{v/1000:.2f} ms" if v >= 1000 else f"{v:.0f} µs"


# lat: median/min/max of one latency percentile (key like "p99_us") across
# runs; op="get"/"put" selects the per-operation latency instead of the
# overall one. The percentiles themselves were computed by the benchmark as
# exact nearest-rank percentiles over all samples of a run; here we only
# take the median of those per-run values.
def lat(runs, key, op=None):
    src = (lambda r: r[op]["latency"][key]) if op else (lambda r: r["latency"][key])
    return med(runs, src)


# host_line: one-line machine description (OS/arch, CPUs, Go version) taken
# from the first run, printed in the header so readers know where the
# numbers come from.
def host_line(runs):
    h = runs[0]["host"]
    return f"{h['os']}/{h['arch']}, {h['cpus']} CPUs, {h['go_version']}"


# ---------------------------------------------------------------- tables

# table: render a GitHub-flavoured Markdown table. The separator row uses
# ":---" (left-aligned) for the first column and "---:" (right-aligned) for
# all numeric columns, so digits line up.
def table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---:" if i else ":---" for i in range(len(headers))) + "|"]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(out)


# load_row: the standard row used by several tests: throughput with range,
# then p50/p95/p99/max of the overall latency, median error count and the
# number of runs (so a missing repeat is visible in the table).
def load_row(label, runs):
    return [
        label,
        fmt_rng(med(runs, lambda r: r["ops_per_sec"])),
        us(lat(runs, "p50_us")[0]),
        us(lat(runs, "p95_us")[0]),
        us(lat(runs, "p99_us")[0]),
        us(lat(runs, "max_us")[0]),
        f"{med(runs, lambda r: r['errors'])[0]:,.0f}",
        str(len(runs)),
    ]


# Column headers matching load_row's cells.
LOAD_HEADERS = ["config", "ops/s median (range)", "p50", "p95", "p99", "max", "errors", "runs"]


# scaling_section (Test A): one row per (RF, node count). Returns the
# Markdown plus `series` = {rf: [(nodes, (median, min, max) ops/s, p99)]}
# for the throughput-vs-nodes graph.
def scaling_section(tag):
    g = load(tag, "scaling")
    if not g:
        return "", {}
    rows, series = [], {}
    for rf in (3, 1):
        for n in (1, 2, 4, 8):
            runs = g.get(f"scaling-rf{rf}-n{n}")
            if not runs:
                continue
            # Effective RF: with fewer nodes than the replication factor, each
            # key can only have as many replicas as there are nodes.
            eff = min(rf, n)
            # Reuse load_row's cells but replace its label with a descriptive one.
            row = [f"{n} node{'s' if n > 1 else ''}, RF={rf} (effective {eff})"] + load_row("", runs)[1:]
            # Write coalescing = protocol frames per write() syscall, measured as
            # a before/after delta of cluster counters by the benchmark. It
            # explains the scaling curve (less batching with more nodes).
            # Older runs without the field get a dash. insert(-1, ...) places
            # the column just before the final "runs" column.
            if all(r.get("write_coalescing") for r in runs):
                cf = med(runs, lambda r: r["write_coalescing"]["coordinator_frames_per_write"])[0]
                nf = med(runs, lambda r: r["write_coalescing"]["node_frames_per_write"])[0]
                row.insert(-1, f"{cf:.2f} / {nf:.2f}")
            else:
                row.insert(-1, "—")
            rows.append(row)
            series.setdefault(rf, []).append((n, med(runs, lambda r: r["ops_per_sec"]), lat(runs, "p99_us")[0]))
    # The section's description line is filled from a run's recorded config
    # (clients, read ratio, keyspace, durations), not typed by hand, so it
    # cannot drift from what was actually run.
    any_runs = next(iter(g.values()))
    c = any_runs[0]["config"]
    md = f"### Test A — scaling with node count\n\n{c['clients']} clients, {c['read_ratio']:.0%} GET, uniform keys over {c['keyspace']:,}, {c['value_size']} B values, {c['duration_ns']/1e9:.0f} s per run after {c['warmup_ns']/1e9:.0f} s warm-up.\n\n"
    md += table(["config"] + LOAD_HEADERS[1:-1] + ["frames per write syscall (coordinator / nodes)", "runs"], rows) + "\n"
    return md, series


# cache_section (Test B): one row per (distribution, cache capacity). Uses
# GET latency (the cache only affects reads). Hit rate is the benchmark's
# delta of cluster-wide cache hits/misses over the measured phase, so
# preload and warm-up are excluded. Returns `data` for the cache bar chart.
def cache_section(tag):
    g = load(tag, "cache")
    if not g:
        return "", {}
    rows, data = [], {}
    for dist in ("zipf", "uniform"):
        for cap in (0, 10000):
            runs = g.get(f"cache-{dist}-cap{cap}")
            if not runs:
                continue
            hr = med(runs, lambda r: r["cache"]["hit_rate"] * 100)[0]
            name = f"{dist}, cache {'off' if cap == 0 else f'{cap:,} entries/node'}"
            rows.append([name, fmt_rng(med(runs, lambda r: r["ops_per_sec"])),
                         us(lat(runs, "p50_us", "get")[0]), us(lat(runs, "p95_us", "get")[0]), us(lat(runs, "p99_us", "get")[0]),
                         "—" if cap == 0 else f"{hr:.1f}%", str(len(runs))])
            data[(dist, cap)] = dict(ops=med(runs, lambda r: r["ops_per_sec"]), p50=lat(runs, "p50_us", "get")[0],
                                     p99=lat(runs, "p99_us", "get")[0], hit=hr)
    c = next(iter(g.values()))[0]["config"]
    md = f"### Test B — cache disabled vs enabled\n\n4 nodes, {c['clients']} clients, {c['read_ratio']:.0%} GET, {c['keyspace']:,} keys, Zipf θ={c['zipf_theta']} (hot keys) and uniform. Latency columns are GET latency measured by the client.\n\n"
    md += table(["config", "ops/s median (range)", "GET p50", "GET p95", "GET p99", "cache hit rate", "runs"], rows) + "\n"
    return md, data


# concurrency_section (Test C): one row per client count; returns points
# (clients, ops/s triple, p50, p99) for the latency/throughput graph.
def concurrency_section(tag):
    g = load(tag, "concurrency")
    if not g:
        return "", []
    rows, pts = [], []
    for c in (1, 10, 50, 100, 250):
        runs = g.get(f"concurrency-c{c}")
        if not runs:
            continue
        rows.append([f"{c} client{'s' if c > 1 else ''}"] + load_row("", runs)[1:])
        pts.append((c, med(runs, lambda r: r["ops_per_sec"]), lat(runs, "p50_us")[0], lat(runs, "p99_us")[0]))
    cfg = next(iter(g.values()))[0]["config"]
    md = f"### Test C — concurrency\n\n4 nodes (RF=3, W=2), {cfg['read_ratio']:.0%} GET, uniform keys over {cfg['keyspace']:,}.\n\n"
    md += table(["clients"] + LOAD_HEADERS[1:], rows) + "\n"
    return md, pts


# failure_section (Test D): one row per fault mode, each cell the median of
# a field of the benchmark's "fault" report (computed in the Go runner from
# the 100 ms timeline, the health samples and the probe samples).
# timelines[mode] keeps the FIRST run of each mode for the timeline graph:
# a median of timelines is not meaningful, so one representative run is
# plotted.
def failure_section(tag):
    g = load(tag, "failure")
    if not g:
        return "", {}
    rows, timelines = [], {}
    for mode in ("crash", "hang"):
        runs = g.get(f"failure-{mode}")
        if not runs:
            continue
        # Shorthand: median/min/max of one fault-report field across runs.
        f = lambda key: med(runs, lambda r: r["fault"][key])
        rows.append([
            "kill -9 (crash)" if mode == "crash" else "SIGSTOP (hang)",
            f"{med(runs, lambda r: r['total_ops'])[0]:,.0f}",
            f"{f('errors_after_fault')[0]:,.0f}",
            us(f("baseline_p99_us")[0]),
            us(f("peak_interval_p99_after_fault_us")[0]),
            us(f("max_latency_after_fault_us")[0]),
            f"{f('detection_ms')[0]:,.0f} ms",
            f"{f('probe_failures')[0]:,.0f}",
            us(f("probe_max_latency_after_fault_us")[0]),
            f"{f('probe_stall_end_ms')[0]:,.0f} ms",
            f"{f('recover_to_all_healthy_ms')[0]:,.0f} ms",
            str(len(runs)),
        ])
        timelines[mode] = runs[0]
    c = next(iter(g.values()))[0]["config"]
    md = (f"### Test D — primary failure under load\n\n4 nodes, {c['clients']} clients, {c['read_ratio']:.0%} GET, {c['duration_ns']/1e9:.0f} s run. "
          f"At t={c['fault_at_ns']/1e9:.0f} s the primary of `{c['probe_key']}` is killed (`kill -9`) or frozen (`kill -STOP`); at t={c['recover_at_ns']/1e9:.0f} s it is restarted / resumed. "
          f"A separate probe GETs that key every {c['probe_interval_ns']/1e6:.0f} ms. Medians over runs.\n\n")
    md += table(["fault", "total ops", "client errors after fault", "baseline p99", "peak 100ms-p99", "slowest op",
                 "detected after", "probe errors", "probe worst", "fallback settled", "recovered→all healthy", "runs"], rows) + "\n"
    md += ("\n*detected after*: fault → coordinator reports fewer healthy nodes. *fallback settled*: fault → end of the last probe "
           "slower than 10× its pre-fault median (0 = none). *recovered→all healthy*: restart/resume → node back to HEALTHY after resync.\n")
    return md, timelines


# rebalance_section (Test E): a single offline, deterministic run (no
# repeats needed). First table: % of keys that change owner for each
# membership change, consistent hashing vs hash % N vs the theoretical
# minimum (the fraction the added/removed node must take/give), plus the
# invariant that only keys of the changed node moved. Second table: how
# evenly keys spread over 4 nodes as the virtual-node count grows.
def rebalance_section(tag):
    path = os.path.join(RESULTS, tag, "rebalance", "rebalance.json")
    if not os.path.exists(path):
        return "", None
    with open(path) as f:
        r = json.load(f)
    rows = [[f"{t['from_nodes']} → {t['to_nodes']}", f"{t['consistent_moved_pct']:.2f}%", f"{t['modulo_moved_pct']:.2f}%",
             f"{t['ideal_pct']:.2f}%", "yes" if t["only_affected_nodes_changed"] else "NO"] for t in r["transitions"]]
    md = f"### Test E — key redistribution ({r['keys']:,} keys, {r['virtual_nodes']} vnodes/node)\n\n"
    md += table(["membership change", "consistent hashing: keys moved", "modulo: keys moved", "theoretical minimum", "only keys of added/removed nodes moved"], rows) + "\n\n"
    b_rows = [[str(b["virtual_nodes"]), f"{b['min_keys']:,}", f"{b['max_keys']:,}", f"{b['max_over_ideal']:.3f}", f"{b['stddev_pct']:.1f}%"] for b in r["balance"]]
    md += f"Load balance across {r['balance'][0]['nodes']} nodes vs virtual-node count:\n\n"
    md += table(["vnodes/node", "min keys", "max keys", "max ÷ ideal", "std-dev of load"], b_rows) + "\n"
    return md, r


# transport_section: compares identical labels from the two result trees
# (results/sequential vs results/mux). Only pairs present in BOTH are
# shown, and speed-up = median mux ops/s / median sequential ops/s.
def transport_section():
    """Before/after table: identical configurations under both transports."""
    pairs = [("scaling", "scaling-rf3-n4", "4 nodes RF=3, 100 clients"),
             ("scaling", "scaling-rf1-n4", "4 nodes RF=1, 100 clients"),
             ("concurrency", "concurrency-c1", "4 nodes, 1 client"),
             ("concurrency", "concurrency-c250", "4 nodes, 250 clients"),
             ("cache", "cache-zipf-cap10000", "4 nodes, zipf, cache on")]
    rows, data = [], []
    for suite, label, desc in pairs:
        a, b = load("sequential", suite).get(label), load("mux", suite).get(label)
        if not a or not b:
            continue
        ta, tb = med(a, lambda r: r["ops_per_sec"])[0], med(b, lambda r: r["ops_per_sec"])[0]
        pa, pb = lat(a, "p99_us")[0], lat(b, "p99_us")[0]
        rows.append([desc, f"{ta:,.0f}", f"{tb:,.0f}", f"{tb/ta:.2f}×", us(pa), us(pb)])
        data.append((desc, ta, tb))
    if not rows:
        return "", []
    md = "### Transport optimisation — before/after\n\nSame configurations, same machine. *sequential*: one request per connection round trip, pooled connections per node (up to 256 kept idle). *mux*: request IDs, one multiplexed connection per node, write coalescing with adaptive linger, inline handling on nodes. Both runs use the same benchmark client (one sequential connection per simulated client).\n\n"
    md += table(["configuration", "sequential ops/s", "multiplexed ops/s", "speed-up", "sequential p99", "multiplexed p99"], rows) + "\n"
    return md, data


# profile_section: parse `go tool pprof -top` text output with regexes.
#   "Duration: 10s"          wall time of the profile window
#   "Total samples = 45s"    CPU time sampled -> total/dur = cores busy
#   the "flat" (first) column of the syscall.rawsyscalln line = CPU time
#   spent inside that function, i.e. inside the read/write syscalls
#   themselves -> sysc/dur = cores spent in syscalls
# Divided by the ops/s of the load run that ran during the profile, this
# gives CPU per operation, the metric behind "the system is syscall-bound".
def profile_section():
    """Where coordinator CPU goes, from the saved pprof text + concurrent load run."""
    import re
    rows = []
    for tag in ("sequential", "mux"):
        d = os.path.join(RESULTS, tag, "profile")
        top, load_json = os.path.join(d, "coordinator-top.txt"), os.path.join(d, "load-during-profile.json")
        if not (os.path.exists(top) and os.path.exists(load_json)):
            continue
        text = open(top).read()
        dur = float(re.search(r"Duration: ([\d.]+)s", text).group(1))
        total = float(re.search(r"Total samples = ([\d.]+)s", text).group(1))
        m = re.search(r"^\s*([\d.]+)s\s+[\d.]+%.*syscall\.rawsyscalln", text, re.M)
        sysc = float(m.group(1)) if m else 0.0
        with open(load_json) as f:
            ops = json.load(f)["ops_per_sec"]
        rows.append([tag, f"{ops:,.0f}", f"{total/dur:.2f}", f"{sysc/dur:.2f}", f"{100*sysc/total:.0f}%",
                     f"{1e6*total/dur/ops:.0f} µs", f"{1e6*sysc/dur/ops:.0f} µs"])
    if not rows:
        return ""
    md = ("### Coordinator CPU profile under load\n\n4 nodes, 100 clients, 10 s CPU profile of the coordinator "
          "(`scripts/profile.sh`; raw `.pprof` and `-top` output in `benchmarks/results/<transport>/profile/`).\n\n")
    md += table(["transport", "ops/s during profile", "coordinator cores busy", "cores in syscalls", "share in syscalls",
                 "coordinator CPU per op", "syscall CPU per op"], rows) + "\n"
    return md


# ---------------------------------------------------------------- graphs

# ---------------------------------------------------------------- graphs (helpers)
# setup_mpl: import matplotlib lazily so the tables work without it. The
# "Agg" backend renders straight to PNG with no display (works over SSH and
# in CI). rcParams set one consistent, minimal style for every chart.
def setup_mpl():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed: skipping graphs")
        return None
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
        "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
        "axes.spines.top": False, "axes.spines.right": False, "axes.titleweight": "bold",
        "axes.titlesize": 12, "axes.axisbelow": True, "axes.titlelocation": "left", "font.size": 10, "legend.frameon": False,
    })
    return plt


# thousands: y-axis tick labels like "50k" instead of "50000".
def thousands(ax):
    from matplotlib.ticker import FuncFormatter
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1000:.0f}k" if v >= 1000 else f"{v:.0f}"))


# save: write a figure to benchmarks/graphs/<name> at 160 dpi. When the
# figure has a figure-level legend, leave the top 8% free for it.
def save(fig, name, tight=True):
    os.makedirs(GRAPHS, exist_ok=True)
    if tight:
        fig.tight_layout(rect=(0, 0, 1, 0.92) if fig.legends else None)
    fig.savefig(os.path.join(GRAPHS, name), dpi=160)
    print("wrote", os.path.relpath(os.path.join(GRAPHS, name), ROOT))


# graph_scaling: median throughput per node count, one line per RF, with a
# vertical bar per point spanning the min-max over repeats (vlines). Log2
# x axis because node counts double (1, 2, 4, 8).
def graph_scaling(plt, series):
    if not series:
        return
    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    for i, rf in enumerate(sorted(series, reverse=True)):
        pts = series[rf]
        xs = [p[0] for p in pts]
        ys = [p[1][0] for p in pts]
        ax.plot(xs, ys, color=SERIES[i], linewidth=2, marker="o", markersize=7, label=f"RF={rf}")
        ax.vlines(xs, [p[1][1] for p in pts], [p[1][2] for p in pts], color=SERIES[i], linewidth=1.5, alpha=0.6)
        ax.annotate(f"RF={rf}", (xs[-1], ys[-1]), textcoords="offset points", xytext=(8, 0), va="center", color=INK2)
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8], ["1", "2", "4", "8"])
    ax.set_xlim(0.8, 12)
    ax.set_ylim(bottom=0)
    thousands(ax)
    ax.set_xlabel("storage nodes (all on one machine)")
    ax.set_ylabel("throughput (ops/s)")
    ax.set_title("Throughput vs node count")
    ax.legend(loc="lower left")
    save(fig, "throughput_vs_nodes.png")
    plt.close(fig)


# graph_concurrency: two panels. Left: throughput vs clients (flattens at
# saturation). Right: p50 and p99 in ms vs clients (grows once the system is
# saturated: extra clients only add queueing). Log x axis because client
# counts span 1..250.
def graph_concurrency(plt, pts):
    if not pts:
        return
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9.6, 3.8))
    xs = [p[0] for p in pts]
    a1.plot(xs, [p[1][0] for p in pts], color=SERIES[0], linewidth=2, marker="o", markersize=7)
    a1.set_xscale("log")
    a1.set_xticks(xs, [str(x) for x in xs])
    a1.set_ylim(bottom=0)
    thousands(a1)
    a1.set_xlabel("concurrent clients")
    a1.set_ylabel("throughput (ops/s)")
    a1.set_title("Throughput vs concurrent clients")
    for key, idx, color in (("p50", 2, SERIES[0]), ("p99", 3, SERIES[1])):
        ys = [p[idx] / 1000 for p in pts]
        a2.plot(xs, ys, color=color, linewidth=2, marker="o", markersize=7, label=key)
        a2.annotate(key, (xs[-1], ys[-1]), textcoords="offset points", xytext=(8, 0), va="center", color=INK2)
    a2.set_xscale("log")
    a2.set_xticks(xs, [str(x) for x in xs])
    a2.set_xlim(right=xs[-1] * 2)
    a2.set_ylim(bottom=0)
    a2.set_xlabel("concurrent clients")
    a2.set_ylabel("latency (ms)")
    a2.set_title("Latency vs concurrent clients")
    a2.legend(loc="upper left")
    save(fig, "latency_vs_clients.png")
    plt.close(fig)


# graph_cache: three small bar charts (throughput, GET p99, hit rate), with
# grouped bars (off/on) per distribution. Hit rate has only the "on" bar.
def graph_cache(plt, data):
    if not data:
        return
    fig, axes = plt.subplots(1, 3, figsize=(10.5, 3.6))
    dists = [d for d in ("zipf", "uniform") if (d, 0) in data and (d, 10000) in data]
    width = 0.36
    for ax, (key, title, scale, fmt) in zip(axes, [
        ("ops", "Throughput (ops/s)", 1, lambda v: f"{v/1000:.1f}k"),
        ("p99", "GET p99 (ms)", 1 / 1000, lambda v: f"{v:.2f}"),
        ("hit", "Cache hit rate (%)", 1, lambda v: f"{v:.0f}%"),
    ]):
        for j, cap in enumerate((0, 10000)):
            if key == "hit" and cap == 0:
                continue
            vals = []
            for d in dists:
                v = data[(d, cap)][key]
                vals.append((v[0] if isinstance(v, tuple) else v) * scale)
            # Grouped bars: offset the two bars of a group by +/- half a width.
            xs = [i + (j - 0.5) * width if key != "hit" else i for i in range(len(dists))]
            bars = ax.bar(xs, vals, width=width * 0.94, color=SERIES[j], label="cache off" if cap == 0 else "cache on")
            for b, v in zip(bars, vals):
                ax.annotate(fmt(v), (b.get_x() + b.get_width() / 2, b.get_height()), textcoords="offset points",
                            xytext=(0, 3), ha="center", fontsize=8, color=INK2)
        ax.set_xticks(range(len(dists)), dists)
        ax.set_title(title)
        ax.set_ylim(bottom=0, top=ax.get_ylim()[1] * 1.12)
        ax.grid(axis="x", visible=False)
    thousands(axes[0])
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2, bbox_to_anchor=(0.5, 1.0))
    fig.subplots_adjust(top=0.8)
    save(fig, "cache_on_vs_off.png")
    plt.close(fig)


# graph_rebalance: for node ADDITIONS only, % of keys moved by consistent
# hashing vs hash % N, side by side. The gap between the bars is the point
# of consistent hashing.
def graph_rebalance(plt, r):
    if not r:
        return
    ts = [t for t in r["transitions"] if t["to_nodes"] > t["from_nodes"]]
    fig, ax = plt.subplots(figsize=(7.2, 3.8))
    width = 0.38
    labels = [f"{t['from_nodes']}→{t['to_nodes']}" for t in ts]
    for j, (key, name) in enumerate((("consistent_moved_pct", "consistent hashing"), ("modulo_moved_pct", "hash % N"))):
        vals = [t[key] for t in ts]
        bars = ax.bar([i + (j - 0.5) * width for i in range(len(ts))], vals, width=width * 0.94, color=SERIES[j], label=name)
        for b, v in zip(bars, vals):
            ax.annotate(f"{v:.0f}%", (b.get_x() + b.get_width() / 2, v), textcoords="offset points", xytext=(0, 3),
                        ha="center", fontsize=8, color=INK2)
    ax.set_xticks(range(len(ts)), labels)
    ax.set_ylim(0, 100)
    ax.set_xlabel("nodes before → after adding one")
    ax.set_ylabel("keys that change owner (%)")
    ax.set_title(f"Key redistribution after adding a node ({r['keys']:,} keys)")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left")
    save(fig, "key_redistribution.png")
    plt.close(fig)


# graph_failure: for each fault mode, throughput per 100 ms bucket (top) and
# p99 and max latency per bucket (bottom, log scale because a 1 s timeout
# stall and a 1 ms normal p99 must both be visible). Dashed line = moment the
# fault command finished, dotted = moment recovery finished; both are the
# timestamps the benchmark recorded on its own clock.
def graph_failure(plt, timelines):
    if not timelines:
        return
    fig, axes = plt.subplots(2, len(timelines), figsize=(5.2 * len(timelines), 5.6), sharex=True, squeeze=False)
    for col, (mode, r) in enumerate(sorted(timelines.items())):
        tl = r["timeline"]
        xs = [p["t_ms"] / 1000 for p in tl]
        top, bot = axes[0][col], axes[1][col]
        top.plot(xs, [p["ops_per_sec"] for p in tl], color=SERIES[0], linewidth=1.5)
        bot.plot(xs, [p["p99_us"] / 1000 for p in tl], color=SERIES[1], linewidth=1.5, label="p99 per 100 ms")
        bot.plot(xs, [p["max_us"] / 1000 for p in tl], color=SERIES[2], linewidth=1, label="max per 100 ms")
        f = r["fault"]
        for ax in (top, bot):
            ax.axvline(f["fault_at_ms"] / 1000, color=INK2, linewidth=1, linestyle="--")
            ax.axvline(f["recover_at_ms"] / 1000, color=INK2, linewidth=1, linestyle=":")
        top.annotate(" fault", (f["fault_at_ms"] / 1000, 0.02), xycoords=("data", "axes fraction"), color=INK2, fontsize=8)
        top.annotate(" recover", (f["recover_at_ms"] / 1000, 0.02), xycoords=("data", "axes fraction"), color=INK2, fontsize=8)
        top.set_ylim(bottom=0)
        thousands(top)
        top.set_title(f"{'kill -9' if mode == 'crash' else 'SIGSTOP'}: throughput (ops/s)")
        bot.set_yscale("log")
        from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter
        bot.yaxis.set_major_locator(LogLocator(base=10))
        bot.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        bot.yaxis.set_minor_formatter(NullFormatter())
        bot.set_title("latency (ms, log scale)")
        bot.set_xlabel("seconds")
        bot.legend(loc="upper right", fontsize=8)
    save(fig, "failure_timeline.png")
    plt.close(fig)


# graph_transport: horizontal grouped bars, sequential vs multiplexed
# median throughput for each shared configuration.
def graph_transport(plt, data):
    if not data:
        return
    fig, ax = plt.subplots(figsize=(7.6, 3.8))
    width = 0.38
    for j, name in enumerate(("sequential", "multiplexed")):
        vals = [d[1 + j] for d in data]
        bars = ax.barh([i + (j - 0.5) * width for i in range(len(data))], vals, height=width * 0.94, color=SERIES[j], label=name)
        for b, v in zip(bars, vals):
            ax.annotate(f"{v/1000:.1f}k", (v, b.get_y() + b.get_height() / 2), textcoords="offset points", xytext=(4, 0),
                        va="center", fontsize=8, color=INK2)
    ax.set_yticks(range(len(data)), [d[0] for d in data])
    ax.invert_yaxis()
    ax.set_xlim(right=ax.get_xlim()[1] * 1.12)
    from matplotlib.ticker import FuncFormatter
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1000:.0f}k"))
    ax.set_xlabel("throughput (ops/s)")
    ax.set_title("Transport: sequential vs multiplexed")
    ax.grid(axis="y", visible=False)
    ax.legend(loc="lower right")
    save(fig, "transport_before_after.png")
    plt.close(fig)


# ---------------------------------------------------------------- main
# main: choose the primary result set, build every section, write
# SUMMARY.md, splice the same tables into README.md, then draw the graphs.
# Sections whose results are missing return "" and are simply skipped, so a
# partial result set still produces a (partial) summary.
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary", default=None, help="result set to summarize (default: mux if present, else sequential)")
    args = ap.parse_args()
    # Prefer "mux" (current transport) when present.
    tags = [t for t in ("mux", "sequential") if os.path.isdir(os.path.join(RESULTS, t))]
    if not tags:
        raise SystemExit(f"no results under {RESULTS}")
    primary = args.primary or tags[0]

    sections, host = [], None
    s_md, s_series = scaling_section(primary)
    b_md, b_data = cache_section(primary)
    c_md, c_pts = concurrency_section(primary)
    d_md, d_tl = failure_section(primary)
    e_md, e_res = rebalance_section(primary)
    t_md, t_data = transport_section()
    p_md = profile_section()
    for md in (s_md, b_md, c_md, d_md, e_md, t_md, p_md):
        if md:
            sections.append(md)
    # Machine description from the first live-cluster suite that has results.
    for suite in ("scaling", "concurrency", "cache", "failure"):
        g = load(primary, suite)
        if g:
            host = host_line(next(iter(g.values())))
            break

    header = (f"# Benchmark summary\n\nGenerated by `scripts/summarize.py` from the raw JSON in "
              f"`benchmarks/results/{primary}/`. Machine: {host}. The coordinator, all storage nodes and the "
              f"load generator run on this one machine over loopback TCP, so they compete for the same CPUs.\n")
    out = header + "\n" + "\n".join(sections)
    path = os.path.join(ROOT, "benchmarks", "SUMMARY.md")
    with open(path, "w") as f:
        f.write(out)
    print("wrote", os.path.relpath(path, ROOT))

    # Inject the generated tables into README.md between markers, so every
    # number in the README comes from this script.
    readme = os.path.join(ROOT, "README.md")
    begin, end = "<!-- BEGIN GENERATED RESULTS -->", "<!-- END GENERATED RESULTS -->"
    if os.path.exists(readme):
        with open(readme) as f:
            text = f.read()
        if begin in text and end in text:
            # Replace everything between the two markers (the markers stay),
            # demoting headings one level (### -> ####) to fit under README's
            # "## Benchmark results". Re-running is idempotent: the old tables
            # are always replaced, never appended to.
            body = out.split("\n", 2)[2]  # drop the "# Benchmark summary" title
            body = body.replace("### ", "#### ")
            text = text[: text.index(begin) + len(begin)] + "\n" + body + "\n" + text[text.index(end):]
            with open(readme, "w") as f:
                f.write(text)
            print("updated README.md results section")

    # Graphs last, and only if matplotlib is available.
    plt = setup_mpl()
    if plt:
        graph_scaling(plt, s_series)
        graph_concurrency(plt, c_pts)
        graph_cache(plt, b_data)
        graph_rebalance(plt, e_res)
        graph_failure(plt, d_tl)
        graph_transport(plt, t_data)


if __name__ == "__main__":
    main()
