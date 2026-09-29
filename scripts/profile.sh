#!/usr/bin/env bash
# Capture CPU profiles of the coordinator and one storage node under load
# (4 nodes, 100 clients) and save the top functions as text.
source "$(dirname "$0")/lib.sh"
build
trap stop_cluster EXIT
OUT="$RESULTS/$TRANSPORT_TAG/profile"
mkdir -p "$OUT"
start_cluster 4 10000
"$BIN/benchmark" --clients 100 --duration 3s --keyspace "$KEYSPACE" --quiet >/dev/null # preload + warm
curl -s "localhost:$((COORD_PORT + 1000))/debug/pprof/profile?seconds=10" -o "$OUT/coordinator.pprof" &
p1=$!
curl -s "localhost:$(($(node_port 1) + 1000))/debug/pprof/profile?seconds=10" -o "$OUT/node1.pprof" &
p2=$!
"$BIN/benchmark" --clients 100 --duration 12s --keyspace "$KEYSPACE" --preload=false --quiet \
  --label profile-load --out "$OUT/load-during-profile.json" | grep -E 'throughput|latency \(us\)'
wait "$p1" "$p2" # not a bare wait: the cluster processes are also children of this shell
(cd "$ROOT" && go tool pprof -top -nodecount=25 "$BIN/coordinator" "$OUT/coordinator.pprof" >"$OUT/coordinator-top.txt" 2>/dev/null)
(cd "$ROOT" && go tool pprof -top -nodecount=25 "$BIN/node" "$OUT/node1.pprof" >"$OUT/node1-top.txt" 2>/dev/null)
log "profiles saved in $OUT"
