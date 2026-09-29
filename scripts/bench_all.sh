#!/usr/bin/env bash
# Run every benchmark suite, then regenerate the summary and graphs.
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/bench_rebalance.sh
scripts/bench_scaling.sh
scripts/bench_cache.sh
scripts/bench_concurrency.sh
scripts/bench_failure.sh
python3 scripts/summarize.py
