.PHONY: test test-unit test-unit-race test-race test-example run-example stop-example clean-example lint proto proto-check generate

# Bound concurrent filesystem fixtures while running independent unit cases.
UNIT_PARALLEL ?= 4
UNIT_PACKAGES ?= 4

test-unit:
	go test -short -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./...

test-unit-race:
	go test -race -short -count=1 -parallel=$(UNIT_PARALLEL) -p=$(UNIT_PACKAGES) ./...

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	go vet ./...

test-example:
	go test -count=1 -timeout=120s ./examples/cluster

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
	PATH="$(CURDIR)/.tools:$$PATH" protoc --go_out=. --go_opt=module=github.com/asalimonov/gquorum --go-grpc_out=. --go-grpc_opt=module=github.com/asalimonov/gquorum examples/cluster/load.proto

proto-check: proto
	git diff --exit-code -- internal/metapb examples/cluster/loadpb
