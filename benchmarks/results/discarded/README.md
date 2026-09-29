# Discarded runs

These Test A (scaling) results were superseded and are kept only for transparency.

The first runs executed each configuration's 3 repeats back-to-back. Some
4- and 8-node runs came out 40-45% below the same configuration measured in
Test C (e.g. 28.7k vs 50.8k ops/s for 4 nodes/100 clients), with unstable
per-second timelines. Re-running the identical configuration in isolation gave
~50-52k, and the machine was shared with other foreground activity at the
time. The scaling suite was therefore changed to interleave repeats (every
configuration once, then again) and record the machine load average per run,
and re-run for both transports. Nothing in README/SUMMARY uses these files.

`mux-scaling-uninstrumented/` is a clean, interleaved re-run (results
matched the final run within ~2%) that was superseded only because the
benchmark was then extended to record write-coalescing counters; the final
`mux/scaling/` run includes them.
