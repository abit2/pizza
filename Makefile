PROTO_SRC = task/task.proto
PROTO_DIR = task
GO_OUT_DIR = task/

.PHONY: proto
proto:
	protoc \
		-I=$(PROTO_DIR) \
		--go_out=$(GO_OUT_DIR) \
		$(PROTO_SRC)

.PHONY: lint
lint:
	golangci-lint run --fix

.PHONY: test
test:
	go test -v -race ./... -count=3
