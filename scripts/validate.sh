#!/usr/bin/env bash
# End-to-end validation against a real 4-node cluster (separate processes,
# real TCP). Every step exits non-zero on failure. Output is also saved under
# benchmarks/results/validation/.
source "$(dirname "$0")/lib.sh"
COUNT=${COUNT:-10000}
OUT="$ROOT/benchmarks/results/validation"
mkdir -p "$OUT"
LOG="$OUT/validation-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1

C="$BIN/client --addr localhost:$COORD_PORT"
step() { printf '\n==> %s\n' "$*"; }

build
trap stop_cluster EXIT
step "1. start a 4-node cluster (N=3, W=2, R=1)"
start_cluster 4 10000

step "2. PUT $COUNT keys"
$C bulk-put --count "$COUNT" --prefix a-

step "3. GET all $COUNT keys and verify values"
$C bulk-get --count "$COUNT" --prefix a-

step "4. verify replication: each key present and correct on all 3 replicas (direct node reads)"
$C verify-replication --count "$COUNT" --prefix a-

step "5. DELETE every 2nd key, verify deletes (coordinator and every replica)"
$C bulk-delete --count "$COUNT" --prefix a- --every 2
$C bulk-get --count "$COUNT" --prefix a- --expect-deleted --every 2
$C verify-replication --count "$COUNT" --prefix a- --expect-deleted --every 2

step "6. kill -9 node 2 ($(node_pid 2))"
kill -9 "$(node_pid 2)"

step "7. immediately GET everything again (replica fallback, before/while failure is detected)"
$C bulk-get --count "$COUNT" --prefix a- --expect-deleted --every 2

step "8. PUT $COUNT new keys while node 2 is down (W=2 of 3 still reachable)"
$C bulk-put --count "$COUNT" --prefix b-
$C bulk-get --count "$COUNT" --prefix b-
$C health

step "9. restart node 2 (empty: in-memory storage) and wait for resync"
start_node 2 10000
wait_all_healthy 4 60
$C health
grep -E 'localhost:7102|resync' "$RUN_DIR/logs/coordinator.log" | tail -4

step "10. verify the restarted node holds every key it replicates (including writes and deletes it missed)"
$C verify-replication --count "$COUNT" --prefix a- --expect-deleted --every 2
$C verify-replication --count "$COUNT" --prefix b-

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
