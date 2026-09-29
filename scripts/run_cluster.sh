#!/usr/bin/env bash
# Start a local cluster in the foreground; Ctrl-C stops it.
#   scripts/run_cluster.sh [nodes=4] [cache_capacity=10000] [coordinator flags...]
#
# Used by `make run-cluster`. Processes run in the background of this shell;
# the shell itself stays in the foreground (the final `wait`) so that one
# Ctrl-C shuts everything down.
source "$(dirname "$0")/lib.sh"
NODES=${1:-4}
CACHE=${2:-10000}
# Leave only the extra coordinator flags in "$@" (see start_cluster).
shift 2 || shift $#
build
# Run stop_cluster on normal exit, Ctrl-C (INT) and `kill` (TERM), so no
# node or coordinator process outlives this script.
trap stop_cluster EXIT INT TERM
start_cluster "$NODES" "$CACHE" "$@"
# Print a small "where is everything" banner. The unquoted heredoc
# delimiter (INFO) lets $variables and $(...) expand inside it.
cat <<INFO

  coordinator   localhost:$COORD_PORT   (HTTP: http://localhost:$((COORD_PORT + 1000))/stats)
$(for i in $(seq 1 "$NODES"); do printf '  node %-8s localhost:%s   (HTTP: http://localhost:%s/stats)\n' "$i" "$(node_port "$i")" "$(($(node_port "$i") + 1000))"; done)
  logs          $RUN_DIR/logs/

  try:  bin/client put hello world && bin/client get hello
  Ctrl-C to stop.
INFO
# Block until the background processes exit (or Ctrl-C fires the trap).
wait
