#!/usr/bin/env bash
# End-to-end validation against a real 4-node cluster (separate processes,
# real TCP). Every step exits non-zero on failure. Output is also saved under
# benchmarks/results/validation/.
#
# This is `make validate`. Where the Go integration tests (tests/) use an
# in-process cluster, this script uses the real binaries and real signals,
# and checks correctness (not speed) at scale: 10,000 keys, direct reads
# from every replica, a real `kill -9`, and a restart that must be resynced.
#
# "Exits non-zero on failure" works because lib.sh sets `set -e` and each
# bin/client bulk-* / verify-* command exits non-zero on any mismatch, so the
# first wrong value stops the script before "VALIDATION PASSED" is printed.
source "$(dirname "$0")/lib.sh"
COUNT=${COUNT:-10000}
OUT="$ROOT/benchmarks/results/validation"
mkdir -p "$OUT"
LOG="$OUT/validation-$(date +%Y%m%d-%H%M%S).log"
# Redirect this script's stdout and stderr through `tee` (process
# substitution), so everything is shown on the terminal AND saved to $LOG.
exec > >(tee "$LOG") 2>&1

# Client command bound to the coordinator. Left unquoted where used ($C) so
# the shell splits it into the program and its flags.
C="$BIN/client --addr localhost:$COORD_PORT"
# step <text>: print a visible header for each stage of the scenario.
step() { printf '\n==> %s\n' "$*"; }

build
trap stop_cluster EXIT
step "1. start a 4-node cluster (N=3, W=2, R=1)"
start_cluster 4 10000

# Keys are "<prefix><6-digit index>" (a-000000 ...) and each value is
# "value-of-<key>", derived from the key, so bulk-get can check every value
# without having to remember what was written.
step "2. PUT $COUNT keys"
$C bulk-put --count "$COUNT" --prefix a-

step "3. GET all $COUNT keys and verify values"
$C bulk-get --count "$COUNT" --prefix a-

# verify-replication asks the coordinator where each key lives (locate) and
# then reads each replica DIRECTLY from the node, bypassing the coordinator.
# That proves the data is physically on all 3 owners, not just readable.
step "4. verify replication: each key present and correct on all 3 replicas (direct node reads)"
$C verify-replication --count "$COUNT" --prefix a-

# --every 2 selects keys whose index i has i % 2 == 0 (every second key).
# With --expect-deleted, those keys must be NOT FOUND and all the others
# must still be present with the right value.
step "5. DELETE every 2nd key, verify deletes (coordinator and every replica)"
$C bulk-delete --count "$COUNT" --prefix a- --every 2
$C bulk-get --count "$COUNT" --prefix a- --expect-deleted --every 2
$C verify-replication --count "$COUNT" --prefix a- --expect-deleted --every 2

# The PID comes from the PID file written by start_node.
step "6. kill -9 node 2 ($(node_pid 2))"
kill -9 "$(node_pid 2)"

# No sleep here on purpose: reads must succeed even before the coordinator
# has marked node 2 down, by falling back to another replica.
step "7. immediately GET everything again (replica fallback, before/while failure is detected)"
$C bulk-get --count "$COUNT" --prefix a- --expect-deleted --every 2

step "8. PUT $COUNT new keys while node 2 is down (W=2 of 3 still reachable)"
$C bulk-put --count "$COUNT" --prefix b-
$C bulk-get --count "$COUNT" --prefix b-
$C health

# The restarted node has lost everything (in-memory store). The coordinator
# sees a new boot ID, resyncs it from the other replicas, and only then
# reports it HEALTHY again; wait_all_healthy waits for that (up to 60 s).
step "9. restart node 2 (empty: in-memory storage) and wait for resync"
start_node 2 10000
wait_all_healthy 4 60
$C health
# Show the coordinator's log lines about node 2 / resync as evidence.
grep -E 'localhost:7102|resync' "$RUN_DIR/logs/coordinator.log" | tail -4

# The strongest check: the restarted node must now hold the "a-" keys it
# owned before the crash, NOT hold the deleted ones (tombstones must have
# been synced too), and hold the "b-" keys written while it was down.
step "10. verify the restarted node holds every key it replicates (including writes and deletes it missed)"
$C verify-replication --count "$COUNT" --prefix a- --expect-deleted --every 2
$C verify-replication --count "$COUNT" --prefix b-

# Print a compact digest of the coordinator's stats JSON: request counts,
# replication/health counters and per-node key and sync counts.
step "11. coordinator stats"
$C stats | python3 -c '
import json,sys; s=json.load(sys.stdin)
print("requests=%d gets=%d puts=%d deletes=%d errors=%d" % (s["requests"], s["gets"], s["puts"], s["deletes"], s["request_errors"]))
print("replication:", json.dumps(s["replication"]))
print("health:", json.dumps(s["health"]))
for n in s["nodes"]:
    st = n.get("stats") or {}
    print("  %s %-8s keys=%-6s repl_ops=%-6s sync_ops=%-6s transitions=%d" % (n["addr"], n["state"], st.get("storage",{}).get("keys"), st.get("replication_ops"), st.get("sync_ops"), n["state_transitions"]))
'
step "VALIDATION PASSED"
