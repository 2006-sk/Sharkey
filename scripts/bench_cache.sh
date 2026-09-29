#!/usr/bin/env bash
# TEST B — cache disabled vs enabled (10,000 entries/node), 4 nodes.
# Zipfian (theta 0.99) is the main comparison: hot keys exist, so the cache
# can absorb them. Uniform is included as a contrast (cache << keyspace).
# Repeats are interleaved across configs (see bench_scaling.sh).
#
# Matrix: {zipf, uniform} x {cache 0 = off, 10000 entries/node} x REPEATS.
# The cache capacity is a NODE flag (--cache-capacity), so every config
# needs its own freshly started cluster; start_cluster stops the old one.
#
# 90% GETs (--read-ratio 0.9) because a read cache only matters for reads.
# Hit rate is not computed here: the benchmark reads the cluster's cache
# hit/miss counters before and after the measured phase and records the
# DELTA, so preload and warm-up traffic are excluded.
#
# Why uniform shows a low hit rate: 4 nodes x 10,000 entries can hold only a
# fraction of 100,000 keys x 3 replicas, and with uniform access every key is
# equally likely, so most GETs miss. With Zipf, a small hot set gets most of
# the traffic and fits in cache (README reports 90.9% vs 40.0%).
source "$(dirname "$0")/lib.sh"
build
# Tear the cluster down however the script exits (see lib.sh).
trap stop_cluster EXIT
CLIENTS=${CLIENTS:-100}
# Outer loop = repeat number, inner loops = configurations: every config runs
# once, then every config runs again (interleaving).
for r in $(seq 1 "$REPEATS"); do
  for dist in zipf uniform; do
    for cache in 0 10000; do
      start_cluster 4 "$cache"
      run_one cache "cache-$dist-cap$cache" "$r" --clients "$CLIENTS" --read-ratio 0.9 --distribution "$dist"
    done
  done
done
