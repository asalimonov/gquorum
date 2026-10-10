.PHONY: test test-race lint proto proto-check

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	go vet ./...

# protoc is a development prerequisite; generators match the checked-in pins.
proto:
	mkdir -p .tools
	GOBIN="$(CURDIR)/.tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
	GOBIN="$(CURDIR)/.tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
	PATH="$(CURDIR)/.tools:$$PATH" protoc --go_out=. --go_opt=module=github.com/asalimonov/gquorum --go-grpc_out=. --go-grpc_opt=module=github.com/asalimonov/gquorum internal/metapb/meta.proto

proto-check: proto
	git diff --exit-code -- internal/metapb
