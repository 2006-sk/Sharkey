#!/usr/bin/env bash
# TEST B — cache disabled vs enabled (10,000 entries/node), 4 nodes.
# Zipfian (theta 0.99) is the main comparison: hot keys exist, so the cache
# can absorb them. Uniform is included as a contrast (cache << keyspace).
# Repeats are interleaved across configs (see bench_scaling.sh).
source "$(dirname "$0")/lib.sh"
build
trap stop_cluster EXIT
CLIENTS=${CLIENTS:-100}
for r in $(seq 1 "$REPEATS"); do
  for dist in zipf uniform; do
    for cache in 0 10000; do
      start_cluster 4 "$cache"
      run_one cache "cache-$dist-cap$cache" "$r" --clients "$CLIENTS" --read-ratio 0.9 --distribution "$dist"
    done
  done
done
