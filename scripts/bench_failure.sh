#!/usr/bin/env bash
# TEST D — kill the primary of a known key during continuous traffic.
#
#   crash: SIGKILL the node (peers see connection refused/reset), restart
#          it later (empty, new boot ID -> resync).
#   hang:  SIGSTOP the node (connections stay open, requests just time out),
#          SIGCONT it later.
#
# The benchmark runs the fault/recovery commands itself so their timing is
# measured on the same clock as the traffic.
source "$(dirname "$0")/lib.sh"
build
trap stop_cluster EXIT
PROBE_KEY=${PROBE_KEY:-bench-00000042}
FDURATION=${FDURATION:-25s}
for mode in crash hang; do
  for r in $(seq 1 "$REPEATS"); do
    start_cluster 4 10000
    # Preload once so the probe key exists and its primary is known.
    "$BIN/client" put "$PROBE_KEY" probe >/dev/null
    primary=$(primary_of "$PROBE_KEY")
    idx=$(node_index_for_addr "$primary")
    pid=$(node_pid "$idx")
    log "[failure] $mode: probe key $PROBE_KEY -> primary $primary (node $idx, pid $pid)"
    if [[ $mode == crash ]]; then
      fault="kill -9 $pid"
      recover="bash -c 'source \"$ROOT/scripts/lib.sh\"; start_node $idx 10000'"
    else
      fault="kill -STOP $pid"
      recover="kill -CONT $pid"
    fi
    mkdir -p "$RESULTS/$TRANSPORT_TAG/failure"
    "$BIN/benchmark" --address "localhost:$COORD_PORT" --label "failure-$mode" \
      --duration "$FDURATION" --warmup 2s --keyspace "$KEYSPACE" --value-size "$VALUE_SIZE" \
      --clients 50 --read-ratio 0.8 --distribution uniform --seed "$r" \
      --timeline-interval 100ms --probe-key "$PROBE_KEY" --probe-interval 5ms \
      --fault-at 8s --fault-cmd "$fault" --recover-at 16s --recover-cmd "$recover" \
      --quiet --out "$RESULTS/$TRANSPORT_TAG/failure/failure-$mode-r$r.json" | sed -n '/fault injection/,$p' >&2
    wait_all_healthy 4 30
  done
done
