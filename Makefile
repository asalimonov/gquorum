.DEFAULT_GOAL := help

.PHONY: help test test-unit test-unit-race test-race test-function test-function-race test-functional test-functional-race test-example test-cluster test-cluster-build test-cluster-run test-perf test-perf-run bench run-example stop-example clean-example lint lint-tools lint-staged install-hooks proto proto-check generate

# Bound concurrent filesystem fixtures while running independent unit cases.
UNIT_PARALLEL ?= 8
UNIT_PACKAGES ?= 4
FUNCTIONAL_PARALLEL ?= 8
FUNCTIONAL_PACKAGES ?= 4
FUNCTIONAL_ARGS ?=
FUNCTIONAL_TIMEOUT ?= 8m
CLUSTER_PARALLEL ?= 8
PERF_PARALLEL ?= 1
CLUSTER_IMAGE ?= gquorum-test:local
CLUSTER_ARGS ?=
CLUSTER_TIMEOUT ?= 10m
BENCH_TIME ?= 200ms
TEST_REPORTS_DIR ?= test-reports
UNIT_ARGS ?=

help: ## List available targets and their descriptions
	@awk 'BEGIN { FS = ":.*## "; print "Usage: make <target>\n\nTargets:" } /^[a-zA-Z0-9_-]+:.*## / { printf "  %-20s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

test-unit: ## Run uncached unit tests in all five modules
	@python3 scripts/test_report.py unit --reports-dir "$(TEST_REPORTS_DIR)" --parallel=$(UNIT_PARALLEL) --packages=$(UNIT_PACKAGES) -- $(UNIT_ARGS)

test-unit-race: ## Run unit tests with the race detector
	@python3 scripts/test_report.py unit --race --reports-dir "$(TEST_REPORTS_DIR)" --parallel=$(UNIT_PARALLEL) --packages=$(UNIT_PACKAGES) -- $(UNIT_ARGS)

test: test-unit ## Compile all five modules and run fast, uncached unit tests

test-race: test-unit-race ## Run fast unit tests with the race detector

REVIVE := .tools/lint/revive-v1.17.1/revive
REVIVE_PACKAGES := .tools/lint/revive-v1.17.1/revive-packages
GOLANGCI := .tools/lint/golangci-v2.14.0/golangci-lint
STATICCHECK := .tools/lint/staticcheck-v0.8.2/staticcheck

$(REVIVE):
	mkdir -p "$(dir $(REVIVE))"
	GOBIN="$(CURDIR)/$(dir $(REVIVE))" go install github.com/mgechev/revive@v1.17.1

$(GOLANGCI):
	mkdir -p "$(dir $(GOLANGCI))"
	GOBIN="$(CURDIR)/$(dir $(GOLANGCI))" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0

$(STATICCHECK):
	mkdir -p "$(dir $(STATICCHECK))"
	GOBIN="$(CURDIR)/$(dir $(STATICCHECK))" go install honnef.co/go/tools/cmd/staticcheck@v0.8.2

$(REVIVE_PACKAGES): scripts/revive/main.go scripts/revive/go.mod scripts/revive/go.sum
	cd scripts/revive && go build -o "$(CURDIR)/$(REVIVE_PACKAGES)" .

lint: $(REVIVE) $(REVIVE_PACKAGES) $(STATICCHECK) $(GOLANGCI) ## Vet all variants; apply TiDB golangci rules to production and revive/unused checks to all sources
	python3 scripts/lint.py --revive "$(REVIVE_PACKAGES)" --staticcheck "$(STATICCHECK)" --golangci "$(GOLANGCI)"

lint-tools: $(REVIVE) $(REVIVE_PACKAGES) $(STATICCHECK) $(GOLANGCI) ## Install the pinned lint tools

lint-staged: ## Lint staged Go files using their exact index contents
	@python3 scripts/lint_staged.py

install-hooks: ## Enable the repository's pre-commit lint hook
	git config --local core.hooksPath .githooks

test-function: ## Run isolated functional, protocol and storage tests
	@python3 scripts/test_report.py function --reports-dir "$(TEST_REPORTS_DIR)" --parallel=$(FUNCTIONAL_PARALLEL) --packages=$(FUNCTIONAL_PACKAGES) --timeout=$(FUNCTIONAL_TIMEOUT) -- $(FUNCTIONAL_ARGS)

test-function-race: ## Run functional tests with the race detector
	@python3 scripts/test_report.py function --race --reports-dir "$(TEST_REPORTS_DIR)" --parallel=$(FUNCTIONAL_PARALLEL) --packages=$(FUNCTIONAL_PACKAGES) --timeout=$(FUNCTIONAL_TIMEOUT) -- $(FUNCTIONAL_ARGS)

test-functional: test-function ## Alias for test-function

test-functional-race: test-function-race ## Alias for test-function-race

test-example: ## Test the three-node Docker Compose example
	@python3 scripts/test_report.py example --reports-dir "$(TEST_REPORTS_DIR)" --timeout=4m

test-cluster-build: ## Build the Docker process-test application image
	docker build -f tests/cluster/testapp/Dockerfile -t $(CLUSTER_IMAGE) .

test-cluster: ## Build the image and run Docker process tests
	@python3 scripts/test_report.py cluster --build-image --image "$(CLUSTER_IMAGE)" --parallel=$(CLUSTER_PARALLEL) --reports-dir "$(TEST_REPORTS_DIR)" --timeout=$(CLUSTER_TIMEOUT) -- $(CLUSTER_ARGS)

# Separate module keeps Docker tooling out of the library and unit-test loop.
test-cluster-run: ## Run Docker cluster tests using an existing image
	@python3 scripts/test_report.py cluster --image "$(CLUSTER_IMAGE)" --parallel=$(CLUSTER_PARALLEL) --reports-dir "$(TEST_REPORTS_DIR)" --timeout=$(CLUSTER_TIMEOUT) -- $(CLUSTER_ARGS)

# Each performance case owns a separate deadline, including generation cleanup.
test-perf: test-cluster-build ## Build the image and run process performance checks
	$(MAKE) test-perf-run

test-perf-run: ## Run process performance checks using an existing image
	$(MAKE) test-cluster-run CLUSTER_PARALLEL=$(PERF_PARALLEL) CLUSTER_ARGS='-v -run ^TestPerf_'

bench: ## Run planner and metadata snapshot benchmarks
	go test -run '^$$' -bench '^(BenchmarkPlanner|BenchmarkMetaSnapshot)' -benchmem -benchtime=$(BENCH_TIME) ./topology ./internal/metaengine

run-example: ## Build and start the three-node Compose example
	docker compose -f examples/cluster/compose.yaml up --build -d --wait --wait-timeout 90
	@echo 'HTTP endpoints: http://localhost:18081 http://localhost:18082 http://localhost:18083'
	@echo 'Demonstrations: examples/cluster/README.md'

stop-example: ## Stop the Compose example and preserve its data
	docker compose -f examples/cluster/compose.yaml down

clean-example: ## Stop the Compose example and delete its data volumes
	docker compose -f examples/cluster/compose.yaml down --volumes

generate: proto ## Generate protocol code with pinned Go generators

# protoc is a development prerequisite; generators match the checked-in pins.
proto: ## Generate meta, load adapter and test-application protocols (requires protoc)
	mkdir -p .tools
	GOBIN="$(CURDIR)/.tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
	GOBIN="$(CURDIR)/.tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
	PATH="$(CURDIR)/.tools:$$PATH" protoc --go_out=. --go_opt=module=github.com/asalimonov/gquorum --go-grpc_out=. --go-grpc_opt=module=github.com/asalimonov/gquorum internal/metapb/meta.proto
	PATH="$(CURDIR)/.tools:$$PATH" protoc --go_out=. --go_opt=module=github.com/asalimonov/gquorum --go-grpc_out=. --go-grpc_opt=module=github.com/asalimonov/gquorum transport/loadgrpc/internal/loadpb/load.proto
	PATH="$(CURDIR)/.tools:$$PATH" protoc --go_out=tests/cluster --go_opt=module=github.com/asalimonov/gquorum/tests/cluster --go-grpc_out=tests/cluster --go-grpc_opt=module=github.com/asalimonov/gquorum/tests/cluster tests/cluster/testapp/load.proto

proto-check: proto ## Regenerate protocol code and verify it has no diff
	git diff --exit-code -- internal/metapb transport/loadgrpc/internal/loadpb tests/cluster/testapp/loadpb

.PHONY: run-scheduler-example stop-scheduler-example clean-scheduler-example test-scheduler-example
run-scheduler-example: ## Build and run the three-node durable task scheduler
	docker compose -f examples/scheduler/compose.yaml up --build -d --wait --wait-timeout 120

stop-scheduler-example: ## Stop the scheduler example and preserve its data
	docker compose -f examples/scheduler/compose.yaml down

clean-scheduler-example: ## Stop the scheduler example and delete its own data volumes
	docker compose -f examples/scheduler/compose.yaml down --volumes

.PHONY: run-cache-example stop-cache-example clean-cache-example test-cache-example
run-cache-example: ## Run cache with client balancing (CACHE_WORKERS=1..256, default 8)
	docker compose -f examples/inmemory-cache/compose.yaml up --build -d --wait --remove-orphans --wait-timeout 120

stop-cache-example: ## Stop the cache example and preserve its Raft and topology volumes
	docker compose -f examples/inmemory-cache/compose.yaml down --remove-orphans

clean-cache-example: ## Stop the cache example and delete its own volumes
	docker compose -f examples/inmemory-cache/compose.yaml down --volumes --remove-orphans

test-scheduler-example: ## Verify scheduler execution, transfer, crashes and quorum recovery in Docker
	@python3 scripts/test_report.py example --reports-dir "$(TEST_REPORTS_DIR)" --timeout=6m -- -run=^TestSchedulerExample

test-cache-example: ## Verify cache recovery across one-node and full process restarts
	@python3 scripts/test_report.py example --reports-dir "$(TEST_REPORTS_DIR)" --timeout=5m -- -run=^TestCacheExample
