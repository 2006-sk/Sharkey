# Distributed key-value store — developer commands.
#
# Make basics used here (for readers new to Make):
#   * `target: prerequisites` followed by TAB-indented recipe lines; each
#     recipe line runs in its own shell.
#   * `.PHONY` targets are commands, not files: `make build` always runs even
#     if a file or directory called "build" exists.
#   * `## text` after a target is not special to Make; the `help` target greps
#     for it to print a self-documenting list of commands.
#   * `$$` in a recipe is a literal `$` for the shell (Make eats one `$`).
#
# Variables: `?=` sets a default the environment can override
# (`GO=go1.23 make build`); `:=` is a plain, immediately expanded assignment.
GO      ?= go
BIN     := bin
PKGS    := ./...

.PHONY: help build test race vet fmt fmt-check check run-cluster stop-cluster \
        benchmark benchmark-scaling benchmark-cache benchmark-concurrency \
        benchmark-failure benchmark-rebalance benchmark-all summarize \
        validate docker-up docker-down clean

# help: find every "target: ... ## description" line in this Makefile and
# print it with the target name in cyan. The leading `@` stops Make from
# echoing the command itself.
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# build: compile every main package under cmd/ (coordinator, node, client,
# benchmark) into bin/. The project uses only the standard library.
build: ## Build coordinator, node, client and benchmark into bin/
	$(GO) build -o $(BIN)/ ./cmd/...

# test: unit tests in internal/... plus the integration tests in tests/,
# which start in-process clusters over real loopback TCP.
test: ## Run unit + integration tests
	$(GO) test $(PKGS)

# race: same tests with the race detector, which instruments memory accesses
# and fails on unsynchronised concurrent access. Slower, but essential for a
# system built from goroutines, locks and atomics.
race: ## Run all tests under the race detector
	$(GO) test -race $(PKGS)

# vet: static checks for common mistakes (bad Printf verbs, copied locks...).
vet: ## go vet
	$(GO) vet $(PKGS)

# fmt: rewrite all Go files into canonical gofmt style.
fmt: ## gofmt all sources
	gofmt -w .

# fmt-check: CI-style check that changes nothing. `gofmt -l` lists files that
# would change; `test -z` succeeds only if that list is empty, otherwise
# print the offending files and fail.
fmt-check: ## Fail if any file is not gofmt'ed
	@test -z "$$(gofmt -l .)" || (echo "not gofmt'ed:"; gofmt -l .; exit 1)

# check: no recipe of its own; it just depends on three other targets, so
# `make check` runs formatting check, vet and race tests in that order.
check: fmt-check vet race ## fmt-check + vet + race tests

# run-cluster: 4 nodes with 10,000-entry caches + coordinator on :7100-7104,
# in the foreground (see scripts/run_cluster.sh).
run-cluster: ## Run coordinator + 4 nodes locally in the foreground (Ctrl-C stops)
	scripts/run_cluster.sh 4 10000

# stop-cluster: lib.sh functions only exist inside a shell that sourced it,
# hence `bash -c 'source ... && stop_cluster'`. Stops processes via their
# PID files in .run/pids/.
stop-cluster: ## Stop a cluster started by the scripts
	bash -c 'source scripts/lib.sh && stop_cluster'

# benchmark: a quick, single, unsaved run for a first impression. Depends on
# `build`. The trap tears the temporary cluster down when bash exits. The
# backslash-newline continues the single shell command onto the next line.
benchmark: build ## Quick 10s benchmark against a fresh 4-node cluster
	bash -c 'source scripts/lib.sh && trap stop_cluster EXIT && start_cluster 4 10000 && \
	  bin/benchmark --clients 100 --duration 10s --read-ratio 0.8 --keyspace 100000 --distribution zipf'

# The published benchmark suites (README "Benchmark results"). Each script
# builds, starts fresh clusters, and writes raw JSON to
# benchmarks/results/$(TRANSPORT_TAG, default mux)/<suite>/.
benchmark-scaling: ## TEST A: throughput vs node count (1/2/4/8 nodes)
	scripts/bench_scaling.sh

benchmark-cache: ## TEST B: cache disabled vs enabled (zipf + uniform)
	scripts/bench_cache.sh

benchmark-concurrency: ## TEST C: 1/10/50/100/250 clients
	scripts/bench_concurrency.sh

benchmark-failure: ## TEST D: crash and hang the primary of a key under load
	scripts/bench_failure.sh

benchmark-rebalance: ## TEST E: key movement, consistent hashing vs modulo
	scripts/bench_rebalance.sh

benchmark-all: ## Run every suite, then summarize
	scripts/bench_all.sh

# summarize: raw JSON -> benchmarks/SUMMARY.md, benchmarks/graphs/*.png and
# the generated tables between the markers in README.md.
summarize: ## Regenerate benchmarks/SUMMARY.md and graphs from raw results
	python3 scripts/summarize.py

# validate: end-to-end correctness scenario with real processes and kill -9
# (see scripts/validate.sh).
validate: ## End-to-end validation against a real 4-node cluster
	scripts/validate.sh

# Docker: the same 4-node cluster from docker-compose.yml. `--build` rebuilds
# the image first so it contains the current source.
docker-up: ## docker compose: coordinator + 4 nodes
	docker compose up --build

docker-down: ## Stop the compose cluster
	docker compose down

# clean: stop any script-started cluster first (the leading `-` tells Make to
# ignore a failure of that line), then delete binaries and runtime state.
# benchmarks/results/ is kept: raw results are the evidence for the README.
clean: ## Remove binaries and runtime state (keeps benchmark results)
	-bash -c 'source scripts/lib.sh && stop_cluster quiet' 2>/dev/null
	rm -rf $(BIN) .run
