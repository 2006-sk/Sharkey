#!/usr/bin/env bash
# Shared helpers for launching a local cluster from the bin/ binaries.
# Source this file; do not execute it.
#
# Layout (defaults):
#   coordinator  TCP :7100   HTTP :8100
#   node i       TCP :710i   HTTP :810i   (i = 1..N)
#
# ---------------------------------------------------------------------------
# WHAT THIS FILE IS
# ---------------------------------------------------------------------------
# Every benchmark/validation script in scripts/ does `source scripts/lib.sh`.
# "Sourcing" runs this file inside the caller's shell (not a child shell), so
# the variables (ROOT, BIN, COORD_PORT, ...) and functions (start_cluster,
# stop_cluster, run_one, ...) defined here become part of the caller. That is
# how a tiny script like bench_concurrency.sh can say `start_cluster 4 10000`.
#
# The cluster is real: separate OS processes (bin/node, bin/coordinator) that
# talk over loopback TCP. That is different from internal/testcluster, which
# runs everything in-process for Go tests. Real processes are needed here
# because the failure benchmark must `kill -9` / `kill -STOP` a node, and
# because benchmark numbers should include real process and syscall costs.
#
# ORCHESTRATION IDEAS USED BELOW
#   * PID files: each background process's PID ($!) is written to
#     .run/pids/<name>.pid. Later commands (stop_cluster, the failure test's
#     `kill -9 $pid`) read those files instead of guessing with `pkill`, so
#     they only ever signal processes this toolkit started.
#   * Readiness checks: a process being started is not the same as it being
#     ready to serve. wait_ready polls until the process answers a real
#     application-level PING, and fails fast if the process died.
#   * Cleanup traps: callers do `trap stop_cluster EXIT`, so the cluster is
#     torn down however the script ends (success, error under `set -e`, or
#     Ctrl-C), and no orphan processes keep ports busy for the next run.
# ---------------------------------------------------------------------------

# Strict mode, inherited by every script that sources this file:
#   -e           exit on the first failing command (a failed start is fatal),
#   -u           using an unset variable is an error (catches typos),
#   -o pipefail  a pipeline fails if ANY stage fails, not only the last one.
set -euo pipefail

# ROOT = repository root, computed from this file's own location
# (BASH_SOURCE[0] is lib.sh even when sourced), so scripts work from any cwd.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Where `go build` puts coordinator, node, client and benchmark.
BIN="$ROOT/bin"
# Runtime state: logs/ (stdout+stderr of every process) and pids/ (PID files).
# Overridable via the environment, like all ${VAR:-default} settings below.
RUN_DIR="${RUN_DIR:-$ROOT/.run}"
# Coordinator protocol port; its HTTP admin port is this + 1000 (8100).
COORD_PORT="${COORD_PORT:-7100}"
# Node i listens on NODE_BASE_PORT + i, i.e. 7101, 7102, ... by default.
NODE_BASE_PORT="${NODE_BASE_PORT:-7100}"
mkdir -p "$RUN_DIR/logs" "$RUN_DIR/pids"

# macOS defaults the soft open-file limit to 256; hundreds of benchmark
# clients need more. The binaries also raise it themselves.
#
# Why it matters: every TCP connection is a file descriptor. With 250
# benchmark clients, the coordinator holds 250 client sockets plus its node
# connections; past the limit, accept()/dial() fail with "too many open
# files". `ulimit -n` sets the SOFT limit for this shell and its children.
# Fallback chain: try 10240; if the HARD limit is lower, raise the soft limit
# to the hard limit (`ulimit -Hn`); if even that fails, carry on (`|| true`,
# needed because of `set -e`) since the Go binaries also try via setrlimit
# (cmdutil.RaiseFileLimit).
ulimit -n 10240 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

# log <message...>: timestamped progress line on STDERR. Stderr keeps stdout
# clean for data (e.g. primary_of's output is captured with $(...)).
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# build: compile all four commands into bin/. `./cmd/...` matches every main
# package under cmd/; `-o bin/` (trailing slash) writes one binary per
# package. Run in a subshell `( ... )` so the `cd` does not leak to the caller.
build() {
  log "building binaries into bin/"
  (cd "$ROOT" && go build -o "$BIN/" ./cmd/...)
}

# node_port <i>: protocol port of node i (7100 + i by default). Bash
# arithmetic $(( )) does the addition.
node_port() { echo $((NODE_BASE_PORT + $1)); }

# node_addrs <n>: comma-separated "localhost:7101,localhost:7102,..." list,
# the format the coordinator's --nodes flag expects.
# `${out:+,}` expands to "," only when $out is already non-empty, so there is
# no leading comma before the first address.
node_addrs() {
  local n=$1 out=""
  for i in $(seq 1 "$n"); do out+="${out:+,}localhost:$(node_port "$i")"; done
  echo "$out"
}

# start_node <index> [cache_capacity]
#
# Launches bin/node in the background (`&`), appending its output to
# .run/logs/node<i>.log (>> so a restarted node's log keeps the history of
# the previous incarnation, useful when debugging a failure run).
# `$!` is the PID of the most recent background job; it is saved to the
# node's PID file and then used by wait_ready to watch THAT process.
# The failure benchmark calls this function again (via its recover command)
# to restart a killed node on the same port with empty memory.
start_node() {
  local i=$1 cache=${2:-10000} port
  port=$(node_port "$i")
  "$BIN/node" --port "$port" --cache-capacity "$cache" >>"$RUN_DIR/logs/node$i.log" 2>&1 &
  echo $! >"$RUN_DIR/pids/node$i.pid"
  wait_ready "$!" "localhost:$port" "node $i"
}

# wait_ready <pid> <addr> <name>: our process is alive AND answers PING.
# (Checking only that the port accepts connections is not enough: another
# program may already own it, e.g. macOS AirPlay on :7000.)
#
# Loop: ask bin/client to send a protocol-level health request (PING) to
# addr with a 500 ms timeout. Until it succeeds:
#   * `kill -0 pid` sends no signal; it only tests that the process exists.
#     If our process already exited (bad flag, port in use), fail at once
#     instead of waiting out the whole deadline.
#   * SECONDS is a bash builtin counting seconds since the shell started, so
#     `deadline=$((SECONDS + 10))` is a 10-second timeout without `date`.
#   * poll every 50 ms: fast startup is detected quickly, cheaply.
# After success, check once more that OUR pid is alive: if the PING was
# answered by some other process that already held the port, our process
# will have died with "address already in use" and this catches it.
wait_ready() {
  local pid=$1 addr=$2 name=$3 deadline=$((SECONDS + 10))
  until "$BIN/client" --addr "$addr" --timeout 500ms health >/dev/null 2>&1; do
    if ! kill -0 "$pid" 2>/dev/null; then
      log "$name (pid $pid) exited during startup; see $RUN_DIR/logs/"
      return 1
    fi
    if ((SECONDS > deadline)); then log "timeout waiting for $name at $addr"; return 1; fi
    sleep 0.05
  done
  kill -0 "$pid" 2>/dev/null || { log "$name exited; is $addr already in use?"; return 1; }
}

# node_pid <i>: read node i's PID from its PID file (used by the failure
# benchmark and validate.sh to kill exactly that node).
node_pid() { cat "$RUN_DIR/pids/node$1.pid"; }

# node_index_for_addr localhost:7102 -> 2
# `${1##*:}` strips the longest prefix matching "*:", leaving the port; the
# index is the port minus NODE_BASE_PORT (the inverse of node_port).
node_index_for_addr() { local port=${1##*:}; echo $((port - NODE_BASE_PORT)); }

# start_cluster <nodes> [cache_capacity] [extra coordinator flags...]
#
# 1. `shift 2 || shift $#`: drop the two positional args so "$@" holds only
#    the extra coordinator flags. If fewer than 2 args were given, `shift 2`
#    fails, and `shift $#` drops whatever there was instead (keeps set -e
#    happy).
# 2. stop_cluster quiet: always start from a clean slate. Each benchmark
#    configuration gets a FRESH cluster so state (cache contents, counters,
#    connection pools) from the previous run cannot leak into the next.
# 3. start nodes first, one by one, each waited on until ready; the
#    coordinator is started last so its first health checks find every node
#    up (otherwise it would begin by marking nodes unhealthy).
# 4. start the coordinator with the node list, record its PID, wait ready.
start_cluster() {
  local n=$1 cache=${2:-10000}
  shift 2 || shift $#
  stop_cluster quiet
  for i in $(seq 1 "$n"); do start_node "$i" "$cache"; done
  "$BIN/coordinator" --port "$COORD_PORT" --nodes "$(node_addrs "$n")" "$@" \
    >>"$RUN_DIR/logs/coordinator.log" 2>&1 &
  echo $! >"$RUN_DIR/pids/coordinator.pid"
  wait_ready "$!" "localhost:$COORD_PORT" coordinator
  log "cluster up: coordinator :$COORD_PORT, $n nodes ($(node_addrs "$n")), cache=$cache $*"
}

# stop_cluster [quiet]: stop every process that has a PID file.
#
# For each .run/pids/*.pid:
#   * `[[ -e $f ]] || continue`: if no PID files exist, the glob stays
#     literal ("*.pid"); skip it rather than cat a nonexistent file.
#   * SIGCONT first: a process frozen with SIGSTOP (the hang test) cannot
#     act on SIGTERM until it runs again, so resume it before terminating.
#   * plain `kill` sends SIGTERM, which the Go binaries turn into a graceful
#     shutdown (cmdutil.SignalContext cancels their context).
#   * `|| true`: the process may already be gone; that is fine under set -e.
# Then wait (up to 10 s) until the coordinator port and node 1's port stop
# accepting connections (`nc -z` = just test connect). Starting the next
# cluster before the old processes release their ports would make the new
# ones fail with "address already in use".
stop_cluster() {
  local f pid
  for f in "$RUN_DIR"/pids/*.pid; do
    [[ -e $f ]] || continue
    pid=$(cat "$f")
    kill -CONT "$pid" 2>/dev/null || true # in case a test left it stopped
    kill "$pid" 2>/dev/null || true
    rm -f "$f"
  done
  # Wait for ports to free up.
  local deadline=$((SECONDS + 10))
  while nc -z localhost "$COORD_PORT" 2>/dev/null || nc -z localhost "$(node_port 1)" 2>/dev/null; do
    ((SECONDS > deadline)) && break
    sleep 0.05
  done
  [[ ${1:-} == quiet ]] || log "cluster stopped"
}

# primary_of <key> -> host:port of the key's primary (via the coordinator)
# `client locate` prints JSON with the key's ordered replica list; element 0
# is the primary (the first node clockwise on the hash ring). A one-line
# Python filter extracts it because bash has no JSON parser.
primary_of() {
  "$BIN/client" --addr "localhost:$COORD_PORT" locate "$1" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["replicas"][0]["addr"])'
}

# wait_all_healthy <expected_nodes> [timeout_s]
# Poll the coordinator's health document every 100 ms until healthy_nodes
# equals the expected count. Used after a failure run (a restarted node must
# be detected and resynced before the next repeat) and in validate.sh after
# restarting node 2. The Python snippet exits 0/1, which drives `until`.
wait_all_healthy() {
  local want=$1 deadline=$((SECONDS + ${2:-30}))
  until "$BIN/client" --addr "localhost:$COORD_PORT" health 2>/dev/null |
    python3 -c "import json,sys; d=json.load(sys.stdin); sys.exit(0 if d['healthy_nodes']==$want else 1)"; do
    if ((SECONDS > deadline)); then log "timeout waiting for $want healthy nodes"; return 1; fi
    sleep 0.1
  done
}

# ---- benchmark helpers -------------------------------------------------------
# Defaults for every benchmark suite; each can be overridden from the
# environment, e.g. `DURATION=5s REPEATS=1 scripts/bench_cache.sh` for a
# quick smoke run. The README's published numbers use these defaults:
# 3 s warm-up, 15 s measured, 3 repeats, 100,000 keys, 100-byte values.
RESULTS="${RESULTS:-$ROOT/benchmarks/results}"
DURATION="${DURATION:-15s}"
WARMUP="${WARMUP:-3s}"
REPEATS="${REPEATS:-3}"
KEYSPACE="${KEYSPACE:-100000}"
VALUE_SIZE="${VALUE_SIZE:-100}"
# Transport variant tag stored in result paths so before/after runs coexist.
# (results/sequential/ holds the pre-optimisation baseline, results/mux/ the
# multiplexed transport; summarize.py compares the two.)
TRANSPORT_TAG="${TRANSPORT_TAG:-mux}"

# run_one <suite> <label> <repeat> [benchmark flags...]: one measured run.
#
# * Output file: results/<tag>/<suite>/<label>-r<repeat>.json. The label is
#   stored inside the JSON too; summarize.py groups files by label and takes
#   the median across repeats.
# * --seed "$r": each repeat uses a different, but reproducible, random seed
#   (seed 1, 2, 3), so repeats are not accidentally identical key sequences.
# * The log line records the machine's load average before the run
#   (`sysctl vm.loadavg` on macOS, /proc/loadavg on Linux). A high value
#   means other work was competing for the CPU (see README "Benchmark
#   hygiene note").
# * The benchmark's human-readable report is filtered with grep down to the
#   headline lines for the console; `|| true` because grep exits 1 when
#   nothing matches and that should not abort the suite under pipefail. The
#   full data is in the JSON file.
run_one() {
  local suite=$1 label=$2 r=$3
  shift 3
  mkdir -p "$RESULTS/$TRANSPORT_TAG/$suite"
  log "[$suite] $label (repeat $r/$REPEATS, load avg $(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || cut -d' ' -f1 /proc/loadavg))"
  "$BIN/benchmark" --address "localhost:$COORD_PORT" --label "$label" \
    --duration "$DURATION" --warmup "$WARMUP" --keyspace "$KEYSPACE" --value-size "$VALUE_SIZE" \
    --seed "$r" --quiet --out "$RESULTS/$TRANSPORT_TAG/$suite/$label-r$r.json" "$@" |
    grep -E 'throughput|latency \(us\)|cache  |errors' >&2 || true
}

# run_bench <suite> <label> [benchmark flags...]: REPEATS consecutive runs.
# Note: the published suites do NOT use this; they call run_one inside an
# outer repeat loop so repeats are interleaved across configurations (see
# bench_scaling.sh for why back-to-back repeats were abandoned).
run_bench() {
  local suite=$1 label=$2 r
  shift 2
  for r in $(seq 1 "$REPEATS"); do run_one "$suite" "$label" "$r" "$@"; done
}
