#!/usr/bin/env bash
# TEST C — throughput/latency vs number of concurrent clients, 4 nodes.
# Repeats are interleaved across configs (see bench_scaling.sh).
#
# Each benchmark client is closed-loop: one connection, one outstanding
# request, the next request is sent only after the previous reply arrives.
# So N clients means at most N requests in flight. Expected shape:
#   * few clients: throughput grows ~linearly, latency stays flat;
#   * past saturation (~100 clients here): throughput plateaus and extra
#     clients only wait in queues, so latency grows ~ clients / throughput
#     (Little's law).
#
# Unlike the cache and scaling tests, the cluster configuration does not
# change between configs (only the client count does), so ONE cluster is
# started and reused for every run. Each run still preloads and warms up.
source "$(dirname "$0")/lib.sh"
build
trap stop_cluster EXIT
start_cluster 4 10000
for r in $(seq 1 "$REPEATS"); do
  for c in 1 10 50 100 250; do
    run_one concurrency "concurrency-c$c" "$r" --clients "$c" --read-ratio 0.8 --distribution uniform
  done
done
