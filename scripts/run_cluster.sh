#!/usr/bin/env bash
# Start a local cluster in the foreground; Ctrl-C stops it.
#   scripts/run_cluster.sh [nodes=4] [cache_capacity=10000] [coordinator flags...]
source "$(dirname "$0")/lib.sh"
NODES=${1:-4}
CACHE=${2:-10000}
shift 2 || shift $#
build
trap stop_cluster EXIT INT TERM
start_cluster "$NODES" "$CACHE" "$@"
cat <<INFO

  coordinator   localhost:$COORD_PORT   (HTTP: http://localhost:$((COORD_PORT + 1000))/stats)
$(for i in $(seq 1 "$NODES"); do printf '  node %-8s localhost:%s   (HTTP: http://localhost:%s/stats)\n' "$i" "$(node_port "$i")" "$(($(node_port "$i") + 1000))"; done)
  logs          $RUN_DIR/logs/

  try:  bin/client put hello world && bin/client get hello
  Ctrl-C to stop.
INFO
wait
