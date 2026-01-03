# GoDFS Makefile
# This file automates common development tasks

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Binary names
MASTER_BINARY=godfs-master
CLIENT_BINARY=godfs-client

# Directories
CMD_DIR=./cmd
BUILD_DIR=./bin
PROTO_DIR=./proto
API_DIR=./api

# Protoc settings
PROTOC=/opt/homebrew/opt/protobuf/bin/protoc
PROTOC_GEN_GO=$(shell go env GOPATH)/bin/protoc-gen-go
PROTOC_GEN_GO_GRPC=$(shell go env GOPATH)/bin/protoc-gen-go-grpc

.PHONY: all build clean proto run-master run-client test help

# Default target
all: proto build

# Build all binaries
build: build-master build-client

build-master:
	@echo "Building master server..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(MASTER_BINARY) $(CMD_DIR)/master

build-client:
	@echo "Building client..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(CLIENT_BINARY) $(CMD_DIR)/client

# Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	@mkdir -p $(API_DIR)
	PATH="$$PATH:$$(go env GOPATH)/bin" $(PROTOC) \
		--go_out=$(API_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(API_DIR) \
		--go-grpc_opt=paths=source_relative \
		-I$(PROTO_DIR) \
		$(PROTO_DIR)/*.proto
	@# Move generated files from api/proto to api/
	@if [ -d "$(API_DIR)/proto" ]; then mv $(API_DIR)/proto/* $(API_DIR)/ && rmdir $(API_DIR)/proto; fi

# Run the master server
run-master:
	@echo "Starting master server..."
	$(GORUN) $(CMD_DIR)/master/main.go

# Run the client (use: make run-client ARGS="upload myfile.txt")
run-client:
	$(GORUN) $(CMD_DIR)/client/main.go $(ARGS)

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -rf ./data
	@rm -f $(API_DIR)/*.pb.go

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# Install protoc plugins
install-proto-plugins:
	@echo "Installing protoc plugins..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Help
help:
	@echo "GoDFS Makefile Commands:"
	@echo ""
	@echo "  make              - Generate proto and build all binaries"
	@echo "  make build        - Build all binaries"
	@echo "  make proto        - Generate Go code from proto files"
	@echo "  make run-master   - Run the master server"
	@echo "  make run-client   - Run the client (use ARGS for commands)"
	@echo "  make test         - Run all tests"
	@echo "  make clean        - Remove build artifacts"
	@echo "  make deps         - Download and tidy dependencies"
	@echo "  make help         - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make run-client ARGS=\"upload myfile.txt\""
	@echo "  make run-client ARGS=\"list\""
	@echo "  make run-client ARGS=\"download myfile.txt\""
