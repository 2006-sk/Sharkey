# Distributed key-value store — developer commands.
GO      ?= go
BIN     := bin
PKGS    := ./...

.PHONY: help build test race vet fmt fmt-check check run-cluster stop-cluster \
        benchmark benchmark-scaling benchmark-cache benchmark-concurrency \
        benchmark-failure benchmark-rebalance benchmark-all summarize \
        validate docker-up docker-down clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

build: ## Build coordinator, node, client and benchmark into bin/
	$(GO) build -o $(BIN)/ ./cmd/...

test: ## Run unit + integration tests
	$(GO) test $(PKGS)

race: ## Run all tests under the race detector
	$(GO) test -race $(PKGS)

vet: ## go vet
	$(GO) vet $(PKGS)

fmt: ## gofmt all sources
	gofmt -w .

fmt-check: ## Fail if any file is not gofmt'ed
	@test -z "$$(gofmt -l .)" || (echo "not gofmt'ed:"; gofmt -l .; exit 1)

check: fmt-check vet race ## fmt-check + vet + race tests

run-cluster: ## Run coordinator + 4 nodes locally in the foreground (Ctrl-C stops)
	scripts/run_cluster.sh 4 10000

stop-cluster: ## Stop a cluster started by the scripts
	bash -c 'source scripts/lib.sh && stop_cluster'

benchmark: build ## Quick 10s benchmark against a fresh 4-node cluster
	bash -c 'source scripts/lib.sh && trap stop_cluster EXIT && start_cluster 4 10000 && \
	  bin/benchmark --clients 100 --duration 10s --read-ratio 0.8 --keyspace 100000 --distribution zipf'

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

summarize: ## Regenerate benchmarks/SUMMARY.md and graphs from raw results
	python3 scripts/summarize.py

validate: ## End-to-end validation against a real 4-node cluster
	scripts/validate.sh

docker-up: ## docker compose: coordinator + 4 nodes
	docker compose up --build

docker-down: ## Stop the compose cluster
	docker compose down

clean: ## Remove binaries and runtime state (keeps benchmark results)
	-bash -c 'source scripts/lib.sh && stop_cluster quiet' 2>/dev/null
	rm -rf $(BIN) .run
