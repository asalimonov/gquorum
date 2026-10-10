.PHONY: test test-race lint

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	go vet ./...
