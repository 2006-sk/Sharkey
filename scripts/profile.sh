#!/usr/bin/env bash
# Capture CPU profiles of the coordinator and one storage node under load
# (4 nodes, 100 clients) and save the top functions as text.
#
# How: every binary exposes Go's net/http/pprof on its HTTP admin port
# (protocol port + 1000). GET /debug/pprof/profile?seconds=10 makes the
# process sample its own CPU usage for 10 s and return a profile file.
# We start those downloads in the background, generate load during them,
# and then turn the profiles into text tables with `go tool pprof -top`.
# summarize.py reads coordinator-top.txt to compute "syscall CPU per op"
# (the finding that the system is syscall-bound, README "Where the time
# goes").
source "$(dirname "$0")/lib.sh"
build
trap stop_cluster EXIT
OUT="$RESULTS/$TRANSPORT_TAG/profile"
mkdir -p "$OUT"
start_cluster 4 10000
# A short first run whose only job is to preload every key and warm caches
# and connections, so the profiled window sees steady-state behavior.
"$BIN/benchmark" --clients 100 --duration 3s --keyspace "$KEYSPACE" --quiet >/dev/null # preload + warm
# Start both 10 s profiles in the background and remember their PIDs.
curl -s "localhost:$((COORD_PORT + 1000))/debug/pprof/profile?seconds=10" -o "$OUT/coordinator.pprof" &
p1=$!
curl -s "localhost:$(($(node_port 1) + 1000))/debug/pprof/profile?seconds=10" -o "$OUT/node1.pprof" &
p2=$!
# Load for 12 s, longer than the 10 s profile so the whole profiling window
# is under load. --preload=false: keys already exist from the first run.
# Its JSON (ops/s during the profile) lets summarize.py compute CPU per op.
"$BIN/benchmark" --clients 100 --duration 12s --keyspace "$KEYSPACE" --preload=false --quiet \
  --label profile-load --out "$OUT/load-during-profile.json" | grep -E 'throughput|latency \(us\)'
wait "$p1" "$p2" # not a bare wait: the cluster processes are also children of this shell
# pprof needs the binary for symbol names; -nodecount=25 keeps the top 25.
(cd "$ROOT" && go tool pprof -top -nodecount=25 "$BIN/coordinator" "$OUT/coordinator.pprof" >"$OUT/coordinator-top.txt" 2>/dev/null)
(cd "$ROOT" && go tool pprof -top -nodecount=25 "$BIN/node" "$OUT/node1.pprof" >"$OUT/node1-top.txt" 2>/dev/null)
log "profiles saved in $OUT"
