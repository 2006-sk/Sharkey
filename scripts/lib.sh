#!/usr/bin/env bash
# Shared helpers for launching a local cluster from the bin/ binaries.
# Source this file; do not execute it.
#
# Layout (defaults):
#   coordinator  TCP :7100   HTTP :8100
#   node i       TCP :710i   HTTP :810i   (i = 1..N)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"
RUN_DIR="${RUN_DIR:-$ROOT/.run}"
COORD_PORT="${COORD_PORT:-7100}"
NODE_BASE_PORT="${NODE_BASE_PORT:-7100}"
mkdir -p "$RUN_DIR/logs" "$RUN_DIR/pids"

# macOS defaults the soft open-file limit to 256; hundreds of benchmark
# clients need more. The binaries also raise it themselves.
ulimit -n 10240 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

build() {
  log "building binaries into bin/"
  (cd "$ROOT" && go build -o "$BIN/" ./cmd/...)
}

node_port() { echo $((NODE_BASE_PORT + $1)); }

node_addrs() {
  local n=$1 out=""
  for i in $(seq 1 "$n"); do out+="${out:+,}localhost:$(node_port "$i")"; done
  echo "$out"
}

# start_node <index> [cache_capacity]
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

node_pid() { cat "$RUN_DIR/pids/node$1.pid"; }

# node_index_for_addr localhost:7102 -> 2
node_index_for_addr() { local port=${1##*:}; echo $((port - NODE_BASE_PORT)); }

# start_cluster <nodes> [cache_capacity] [extra coordinator flags...]
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
primary_of() {
  "$BIN/client" --addr "localhost:$COORD_PORT" locate "$1" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["replicas"][0]["addr"])'
}

# wait_all_healthy <expected_nodes> [timeout_s]
wait_all_healthy() {
  local want=$1 deadline=$((SECONDS + ${2:-30}))
  until "$BIN/client" --addr "localhost:$COORD_PORT" health 2>/dev/null |
    python3 -c "import json,sys; d=json.load(sys.stdin); sys.exit(0 if d['healthy_nodes']==$want else 1)"; do
    if ((SECONDS > deadline)); then log "timeout waiting for $want healthy nodes"; return 1; fi
    sleep 0.1
  done
}

# ---- benchmark helpers -------------------------------------------------------
RESULTS="${RESULTS:-$ROOT/benchmarks/results}"
DURATION="${DURATION:-15s}"
WARMUP="${WARMUP:-3s}"
REPEATS="${REPEATS:-3}"
KEYSPACE="${KEYSPACE:-100000}"
VALUE_SIZE="${VALUE_SIZE:-100}"
# Transport variant tag stored in result paths so before/after runs coexist.
TRANSPORT_TAG="${TRANSPORT_TAG:-mux}"

# run_one <suite> <label> <repeat> [benchmark flags...]: one measured run.
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
run_bench() {
  local suite=$1 label=$2 r
  shift 2
  for r in $(seq 1 "$REPEATS"); do run_one "$suite" "$label" "$r" "$@"; done
}
