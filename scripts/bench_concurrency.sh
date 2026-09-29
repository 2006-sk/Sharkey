#!/usr/bin/env bash
# TEST C — throughput/latency vs number of concurrent clients, 4 nodes.
# Repeats are interleaved across configs (see bench_scaling.sh).
source "$(dirname "$0")/lib.sh"
build
trap stop_cluster EXIT
start_cluster 4 10000
for r in $(seq 1 "$REPEATS"); do
  for c in 1 10 50 100 250; do
    run_one concurrency "concurrency-c$c" "$r" --clients "$c" --read-ratio 0.8 --distribution uniform
  done
done
