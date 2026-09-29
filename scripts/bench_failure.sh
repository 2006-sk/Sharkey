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
#
# Why the two modes differ so much:
#   * A killed process's sockets are closed by the kernel, so the coordinator
#     gets an immediate error ("connection refused/reset") and can fall back
#     to another replica right away (README: detected in ~15 ms).
#   * A stopped process keeps its sockets open; the kernel even completes
#     TCP handshakes into the listen backlog, but nothing ever answers. The
#     only signal is silence, so the coordinator must wait for its request
#     timeout (1 s) before it can suspect the node. This is the classic
#     "a slow node is harder to handle than a dead one" lesson.
#
# Why the benchmark (not this script) injects the fault: if the script ran
# `sleep 8; kill ...` separately, the two clocks would drift by process
# start-up time and scheduling. Passing --fault-cmd/--fault-at lets the load
# generator run the command at t=8 s on its own timeline, so the fault
# moment lines up exactly with its 100 ms throughput/latency buckets.
source "$(dirname "$0")/lib.sh"
build
trap stop_cluster EXIT
# The "watched" key: the benchmark probes it every 5 ms, and its primary is
# the node we break. bench-00000042 is also inside the benchmark's keyspace.
PROBE_KEY=${PROBE_KEY:-bench-00000042}
# Longer than the other suites: 8 s healthy baseline, 8 s of fault, 9 s of
# recovery observation.
FDURATION=${FDURATION:-25s}
# Here the modes are the outer loop and repeats the inner loop; each run
# starts from a fresh cluster anyway, and a restart must be fully healthy
# (wait_all_healthy) before the next run begins.
for mode in crash hang; do
  for r in $(seq 1 "$REPEATS"); do
    start_cluster 4 10000
    # Preload once so the probe key exists and its primary is known.
    "$BIN/client" put "$PROBE_KEY" probe >/dev/null
    # Ask the coordinator which node is the key's primary, then map that
    # address to our node index and its PID (from the PID file). This is
    # why lib.sh keeps PID files: we must kill exactly this process.
    primary=$(primary_of "$PROBE_KEY")
    idx=$(node_index_for_addr "$primary")
    pid=$(node_pid "$idx")
    log "[failure] $mode: probe key $PROBE_KEY -> primary $primary (node $idx, pid $pid)"
    if [[ $mode == crash ]]; then
      # kill -9 (SIGKILL) cannot be caught: no graceful shutdown, like a
      # power loss for an in-memory store.
      fault="kill -9 $pid"
      # Restart = start a NEW process on the same port. The benchmark runs
      # the command with `sh -c`, which does not know our bash functions, so
      # the command re-sources lib.sh inside bash and calls start_node. The
      # new process has empty memory and a new boot ID; the coordinator
      # notices the boot-ID change and resyncs it from the other replicas.
      # start_node also waits for readiness, so the command returns (and the
      # benchmark timestamps recovery) when the process is serving.
      recover="bash -c 'source \"$ROOT/scripts/lib.sh\"; start_node $idx 10000'"
    else
      # SIGSTOP freezes the process (cannot be caught); SIGCONT resumes it
      # with all of its memory intact.
      fault="kill -STOP $pid"
      recover="kill -CONT $pid"
    fi
    mkdir -p "$RESULTS/$TRANSPORT_TAG/failure"
    # Flags worth noting:
    #   --timeline-interval 100ms  fine-grained buckets to see the dip shape
    #   --probe-key/--probe-interval  a dedicated connection GETs the key
    #                              every 5 ms: worst-case view of one key
    #   --fault-at 8s / --recover-at 16s  times on the benchmark's own clock
    #   --clients 50               moderate load, below saturation
    # Only the "--- fault injection ---" section of the report is shown on
    # the console (sed prints from that line to the end); the JSON has all.
    "$BIN/benchmark" --address "localhost:$COORD_PORT" --label "failure-$mode" \
      --duration "$FDURATION" --warmup 2s --keyspace "$KEYSPACE" --value-size "$VALUE_SIZE" \
      --clients 50 --read-ratio 0.8 --distribution uniform --seed "$r" \
      --timeline-interval 100ms --probe-key "$PROBE_KEY" --probe-interval 5ms \
      --fault-at 8s --fault-cmd "$fault" --recover-at 16s --recover-cmd "$recover" \
      --quiet --out "$RESULTS/$TRANSPORT_TAG/failure/failure-$mode-r$r.json" | sed -n '/fault injection/,$p' >&2
    # Do not start the next repeat until the cluster has fully recovered.
    wait_all_healthy 4 30
  done
done
