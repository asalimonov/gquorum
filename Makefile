.DEFAULT_GOAL := help

.PHONY: help test test-unit test-unit-race test-race test-function test-function-race test-functional test-functional-race test-example test-cluster test-cluster-build test-cluster-run test-perf test-perf-run bench run-example stop-example clean-example lint proto proto-check generate

# Bound concurrent filesystem fixtures while running independent unit cases.
UNIT_PARALLEL ?= 4
UNIT_PACKAGES ?= 4
FUNCTIONAL_ARGS ?=
FUNCTIONAL_TIMEOUT ?= 10m
CLUSTER_IMAGE ?= gquorum-test:local
CLUSTER_ARGS ?=
CLUSTER_TIMEOUT ?= 45m
BENCH_TIME ?= 200ms

help: ## List available targets and their descriptions
	@awk 'BEGIN { FS = ":.*## "; print "Usage: make <target>\n\nTargets:" } /^[a-zA-Z0-9_-]+:.*## / { printf "  %-20s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

test-unit: ## Run uncached unit tests in all three modules
	go test -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./...
	go -C examples/cluster test -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./...
	go -C tests/cluster test -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./testapp/...

test-unit-race: ## Run unit tests with the race detector
	go test -race -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./...
	go -C examples/cluster test -race -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./...
	go -C tests/cluster test -race -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./testapp/...

test: test-unit ## Compile all three modules and run fast, uncached unit tests

test-race: test-unit-race ## Run fast unit tests with the race detector

lint: ## Run go vet for unit and functional builds in all three modules
	go vet ./...
	go -C examples/cluster vet ./...
	go -C tests/cluster vet ./testapp/...
	go vet -tags=functional ./...
	go -C examples/cluster vet -tags=functional ./...
	go -C tests/cluster vet -tags=functional ./testapp/...

test-function: ## Run non-Docker functional tests from *_func_test.go files
	go test -tags=functional -count=1 -timeout=$(FUNCTIONAL_TIMEOUT) $(FUNCTIONAL_ARGS) ./...
	go -C examples/cluster test -tags=functional -count=1 -timeout=$(FUNCTIONAL_TIMEOUT) $(FUNCTIONAL_ARGS) ./...
	go -C tests/cluster test -tags=functional -count=1 -timeout=$(FUNCTIONAL_TIMEOUT) $(FUNCTIONAL_ARGS) ./testapp/...

test-function-race: ## Run functional tests with the race detector
	go test -tags=functional -race -count=1 -timeout=$(FUNCTIONAL_TIMEOUT) $(FUNCTIONAL_ARGS) ./...
	go -C examples/cluster test -tags=functional -race -count=1 -timeout=$(FUNCTIONAL_TIMEOUT) $(FUNCTIONAL_ARGS) ./...
	go -C tests/cluster test -tags=functional -race -count=1 -timeout=$(FUNCTIONAL_TIMEOUT) $(FUNCTIONAL_ARGS) ./testapp/...

test-functional: test-function ## Alias for test-function

test-functional-race: test-function-race ## Alias for test-function-race

test-example: ## Test the three-node Docker Compose example
	go -C tests/cluster test -tags example -count=1 -timeout=4m -run '^TestComposeExample' .

test-cluster-build: ## Build the Docker process-test application image
	docker build -f tests/cluster/testapp/Dockerfile -t $(CLUSTER_IMAGE) .

test-cluster: test-cluster-build ## Build the image and run Docker process tests
	$(MAKE) test-cluster-run

# Separate module keeps Docker tooling out of the library and unit-test loop.
test-cluster-run: ## Run Docker cluster tests using an existing image
	GQUORUM_TEST_IMAGE=$(CLUSTER_IMAGE) go -C tests/cluster test -tags=clustertest -count=1 -timeout=$(CLUSTER_TIMEOUT) $(CLUSTER_ARGS) .

# Each performance case owns a separate deadline, including generation cleanup.
test-perf: test-cluster-build ## Build the image and run process performance checks
	$(MAKE) test-perf-run

test-perf-run: ## Run process performance checks using an existing image
	$(MAKE) test-cluster-run CLUSTER_ARGS='-v -run ^TestPerf_'

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
proto: ## Generate meta and test-application protobuf code (requires protoc)
	mkdir -p .tools
	GOBIN="$(CURDIR)/.tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
	GOBIN="$(CURDIR)/.tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
	PATH="$(CURDIR)/.tools:$$PATH" protoc --go_out=. --go_opt=module=github.com/asalimonov/gquorum --go-grpc_out=. --go-grpc_opt=module=github.com/asalimonov/gquorum internal/metapb/meta.proto
	PATH="$(CURDIR)/.tools:$$PATH" protoc --go_out=tests/cluster --go_opt=module=github.com/asalimonov/gquorum/tests/cluster --go-grpc_out=tests/cluster --go-grpc_opt=module=github.com/asalimonov/gquorum/tests/cluster tests/cluster/testapp/load.proto

proto-check: proto ## Regenerate protocol code and verify it has no diff
	git diff --exit-code -- internal/metapb tests/cluster/testapp/loadpb
