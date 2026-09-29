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
import argparse
import glob
import json
import os
import statistics
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "benchmarks", "results")
GRAPHS = os.path.join(ROOT, "benchmarks", "graphs")

# Reference categorical palette (light surface), fixed slot order.
SURFACE = "#fcfcfb"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]


def load(tag, suite):
    """Group result files of one suite by label."""
    groups = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(RESULTS, tag, suite, "*.json"))):
        with open(path) as f:
            r = json.load(f)
        groups[r["label"]].append(r)
    return groups


def med(runs, fn):
    vals = [fn(r) for r in runs]
    return statistics.median(vals), min(vals), max(vals)


def fmt_rate(m):
    return f"{m[0]:,.0f}"


def fmt_rng(m, unit=""):
    v, lo, hi = m
    if lo == hi:
        return f"{v:,.0f}{unit}"
    return f"{v:,.0f}{unit} ({lo:,.0f}-{hi:,.0f})"


def us(v):
    return f"{v/1000:.2f} ms" if v >= 1000 else f"{v:.0f} µs"


def lat(runs, key, op=None):
    src = (lambda r: r[op]["latency"][key]) if op else (lambda r: r["latency"][key])
    return med(runs, src)


def host_line(runs):
    h = runs[0]["host"]
    return f"{h['os']}/{h['arch']}, {h['cpus']} CPUs, {h['go_version']}"


# ---------------------------------------------------------------- tables

def table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---:" if i else ":---" for i in range(len(headers))) + "|"]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(out)


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


LOAD_HEADERS = ["config", "ops/s median (range)", "p50", "p95", "p99", "max", "errors", "runs"]


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
            eff = min(rf, n)
            row = [f"{n} node{'s' if n > 1 else ''}, RF={rf} (effective {eff})"] + load_row("", runs)[1:]
            if all(r.get("write_coalescing") for r in runs):
                cf = med(runs, lambda r: r["write_coalescing"]["coordinator_frames_per_write"])[0]
                nf = med(runs, lambda r: r["write_coalescing"]["node_frames_per_write"])[0]
                row.insert(-1, f"{cf:.2f} / {nf:.2f}")
            else:
                row.insert(-1, "—")
            rows.append(row)
            series.setdefault(rf, []).append((n, med(runs, lambda r: r["ops_per_sec"]), lat(runs, "p99_us")[0]))
    any_runs = next(iter(g.values()))
    c = any_runs[0]["config"]
    md = f"### Test A — scaling with node count\n\n{c['clients']} clients, {c['read_ratio']:.0%} GET, uniform keys over {c['keyspace']:,}, {c['value_size']} B values, {c['duration_ns']/1e9:.0f} s per run after {c['warmup_ns']/1e9:.0f} s warm-up.\n\n"
    md += table(["config"] + LOAD_HEADERS[1:-1] + ["frames per write syscall (coordinator / nodes)", "runs"], rows) + "\n"
    return md, series


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


def failure_section(tag):
    g = load(tag, "failure")
    if not g:
        return "", {}
    rows, timelines = [], {}
    for mode in ("crash", "hang"):
        runs = g.get(f"failure-{mode}")
        if not runs:
            continue
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


def thousands(ax):
    from matplotlib.ticker import FuncFormatter
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1000:.0f}k" if v >= 1000 else f"{v:.0f}"))


def save(fig, name, tight=True):
    os.makedirs(GRAPHS, exist_ok=True)
    if tight:
        fig.tight_layout(rect=(0, 0, 1, 0.92) if fig.legends else None)
    fig.savefig(os.path.join(GRAPHS, name), dpi=160)
    print("wrote", os.path.relpath(os.path.join(GRAPHS, name), ROOT))


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary", default=None, help="result set to summarize (default: mux if present, else sequential)")
    args = ap.parse_args()
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
            body = out.split("\n", 2)[2]  # drop the "# Benchmark summary" title
            body = body.replace("### ", "#### ")
            text = text[: text.index(begin) + len(begin)] + "\n" + body + "\n" + text[text.index(end):]
            with open(readme, "w") as f:
                f.write(text)
            print("updated README.md results section")

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
