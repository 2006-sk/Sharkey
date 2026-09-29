#!/usr/bin/env bash
# TEST A — throughput/latency vs node count.
# Two series: RF=3/W=2 (the default; with 1-2 nodes the effective RF is 1-2)
# and RF=1 (pure sharding, isolates the cost of replication).
#
# Repeats are interleaved (every config once, then every config again) so a
# transient burst of other activity on the machine spreads across configs
# instead of wiping out one of them.
#
# Why interleaving matters (README "Benchmark hygiene note"): the first
# version ran repeat 1, 2, 3 of a config back to back. When something else
# on the laptop got busy for a minute, all three repeats of ONE config were
# hit and its median was ~40% low, which looks like a real effect. With
# interleaving, the same disturbance lands on one repeat of several configs,
# and taking the median of 3 repeats discards it.
#
# Caveat when reading the results: all nodes run on ONE machine, so more
# nodes means more processes competing for the same CPUs. This test shows
# sharding works at 1/2/4/8 nodes; it does not demonstrate horizontal
# scalability.
source "$(dirname "$0")/lib.sh"
build
trap stop_cluster EXIT
CLIENTS=${CLIENTS:-100}
for r in $(seq 1 "$REPEATS"); do
  for rf in 3 1; do
    for n in 1 2 4 8; do
      # Write quorum must not exceed the replication factor: W=2 for RF=3,
      # W=1 for RF=1 (bash ternary inside $(( ))). The coordinator caps the
      # effective RF at the node count, which is why "effective RF" appears
      # in the summary table.
      start_cluster "$n" 10000 --replication "$rf" --write-quorum "$((rf < 2 ? 1 : 2))"
      run_one scaling "scaling-rf$rf-n$n" "$r" --clients "$CLIENTS" --read-ratio 0.8 --distribution uniform
    done
  done
done
