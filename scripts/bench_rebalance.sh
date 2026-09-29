#!/usr/bin/env bash
# TEST E — key redistribution when nodes are added/removed: consistent
# hashing (128 vnodes) vs modulo sharding, plus load balance vs vnode count.
#
# This is an OFFLINE experiment: `benchmark rebalance` builds hash rings in
# memory, assigns KEYS keys to owners before and after a membership change,
# and counts how many changed owner. No cluster is started and no network is
# involved, so the result is deterministic and not affected by machine load.
#
# The point it proves: adding a 5th node moves ~1/5 of keys with consistent
# hashing (only the keys the new node takes over), but ~4/5 with
# `hash(key) % N`, because changing N remaps almost every key.
source "$(dirname "$0")/lib.sh"
build
mkdir -p "$RESULTS/$TRANSPORT_TAG/rebalance"
"$BIN/benchmark" rebalance --keys "${KEYS:-100000}" --out "$RESULTS/$TRANSPORT_TAG/rebalance/rebalance.json"
