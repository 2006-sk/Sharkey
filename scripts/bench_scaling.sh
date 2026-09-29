#!/usr/bin/env bash
# TEST A — throughput/latency vs node count.
# Two series: RF=3/W=2 (the default; with 1-2 nodes the effective RF is 1-2)
# and RF=1 (pure sharding, isolates the cost of replication).
#
# Repeats are interleaved (every config once, then every config again) so a
# transient burst of other activity on the machine spreads across configs
# instead of wiping out one of them.
source "$(dirname "$0")/lib.sh"
build
trap stop_cluster EXIT
CLIENTS=${CLIENTS:-100}
for r in $(seq 1 "$REPEATS"); do
  for rf in 3 1; do
    for n in 1 2 4 8; do
      start_cluster "$n" 10000 --replication "$rf" --write-quorum "$((rf < 2 ? 1 : 2))"
      run_one scaling "scaling-rf$rf-n$n" "$r" --clients "$CLIENTS" --read-ratio 0.8 --distribution uniform
    done
  done
done
