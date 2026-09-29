#!/usr/bin/env bash
# TEST E — key redistribution when nodes are added/removed: consistent
# hashing (128 vnodes) vs modulo sharding, plus load balance vs vnode count.
source "$(dirname "$0")/lib.sh"
build
mkdir -p "$RESULTS/$TRANSPORT_TAG/rebalance"
"$BIN/benchmark" rebalance --keys "${KEYS:-100000}" --out "$RESULTS/$TRANSPORT_TAG/rebalance/rebalance.json"
