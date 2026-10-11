.PHONY: test test-unit test-unit-race test-race test-example test-fixture test-cluster test-cluster-build test-cluster-run test-perf test-perf-run bench run-example stop-example clean-example lint proto proto-check generate

# Bound concurrent filesystem fixtures while running independent unit cases.
UNIT_PARALLEL ?= 4
UNIT_PACKAGES ?= 4
CLUSTER_IMAGE ?= gquorum-test:local
CLUSTER_ARGS ?=
CLUSTER_TIMEOUT ?= 45m
BENCH_TIME ?= 200ms

test-unit:
	go test -short -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./...
	go -C tests/cluster test -short -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./testapp/...

test-unit-race:
	go test -race -short -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./...
	go -C tests/cluster test -race -short -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./testapp/...

test:
	go test ./...
	go -C tests/cluster test ./testapp/...

test-race:
	go test -race ./...
	go -C tests/cluster test -race ./testapp/...

lint:
	go vet ./...
	go -C tests/cluster vet ./testapp/...

test-fixture:
	go -C tests/cluster test -count=1 -timeout=120s ./testapp/...

test-example:
	go -C tests/cluster test -tags example -count=1 -timeout=4m -run '^TestComposeExample' .

test-cluster-build:
	docker build -f tests/cluster/testapp/Dockerfile -t $(CLUSTER_IMAGE) .

test-cluster: test-cluster-build
	$(MAKE) test-cluster-run

# Separate module keeps Docker tooling out of the library and unit-test loop.
test-cluster-run:
	GQUORUM_TEST_IMAGE=$(CLUSTER_IMAGE) go -C tests/cluster test -tags cluster -count=1 -timeout=$(CLUSTER_TIMEOUT) $(CLUSTER_ARGS) ./...

# Each performance case owns a separate deadline, including generation cleanup.
test-perf: test-cluster-build
	$(MAKE) test-perf-run

test-perf-run:
	$(MAKE) test-cluster-run CLUSTER_ARGS='-v -run ^TestPerf_'

bench:
	go test -run '^$$' -bench '^(BenchmarkPlanner|BenchmarkMetaSnapshot)' -benchmem -benchtime=$(BENCH_TIME) ./topology ./internal/metaengine

run-example:
	docker compose -f examples/cluster/compose.yaml up --build -d --wait --wait-timeout 90
	@echo 'HTTP endpoints: http://localhost:18081 http://localhost:18082 http://localhost:18083'
	@echo 'Demonstrations: examples/cluster/README.md'

stop-example:
	docker compose -f examples/cluster/compose.yaml down

clean-example:
	docker compose -f examples/cluster/compose.yaml down --volumes

generate: proto

# protoc is a development prerequisite; generators match the checked-in pins.
proto:
	mkdir -p .tools
	GOBIN="$(CURDIR)/.tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
	GOBIN="$(CURDIR)/.tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
	PATH="$(CURDIR)/.tools:$$PATH" protoc --go_out=. --go_opt=module=github.com/asalimonov/gquorum --go-grpc_out=. --go-grpc_opt=module=github.com/asalimonov/gquorum internal/metapb/meta.proto
	PATH="$(CURDIR)/.tools:$$PATH" protoc --go_out=tests/cluster --go_opt=module=github.com/asalimonov/gquorum/tests/cluster --go-grpc_out=tests/cluster --go-grpc_opt=module=github.com/asalimonov/gquorum/tests/cluster tests/cluster/testapp/load.proto

proto-check: proto
	git diff --exit-code -- internal/metapb tests/cluster/testapp/loadpb
