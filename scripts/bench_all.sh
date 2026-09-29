#!/usr/bin/env bash
# Run every benchmark suite, then regenerate the summary and graphs.
#
# This is what `make benchmark-all` runs (roughly 25 minutes with the
# defaults). Each suite starts and stops its own clusters, so the suites are
# independent and can also be run one at a time.
#
# Order: rebalance first because it is offline (pure computation on the hash
# ring, no cluster) and fast; then the four live-cluster suites; finally
# summarize.py reads all the raw JSON and rewrites benchmarks/SUMMARY.md,
# benchmarks/graphs/*.png and the generated tables inside README.md.
#
# `set -euo pipefail`: if any suite fails, stop here rather than summarizing
# a partial, misleading result set.
set -euo pipefail
# Run from the repository root no matter where the script was invoked from
# ($0 is this script's path; its parent's parent is the repo root).
cd "$(dirname "$0")/.."
scripts/bench_rebalance.sh
scripts/bench_scaling.sh
scripts/bench_cache.sh
scripts/bench_concurrency.sh
scripts/bench_failure.sh
python3 scripts/summarize.py
