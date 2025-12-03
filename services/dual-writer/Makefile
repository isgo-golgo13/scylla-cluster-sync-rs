.PHONY: build run test docker-build docker-run clean fmt clippy bench

SERVICE_NAME := dual-writer
DOCKER_IMAGE := ghcr.io/yourusername/$(SERVICE_NAME)
VERSION := $(shell git describe --tags --always --dirty)

# CQL Proxy port (was 8080 for HTTP, now 9042 for CQL)
CQL_PORT := 9042

build:
	cargo build --release

run: build
	./target/release/$(SERVICE_NAME) \
		--bind-addr 0.0.0.0:$(CQL_PORT) \
		--config config.yaml \
		--filter-config config/filter-rules.yaml

test:
	cargo test

# Integration test with Python
test-integration: build
	@echo "🧪 Running CQL proxy integration test..."
	./target/release/$(SERVICE_NAME) \
		--bind-addr 0.0.0.0:$(CQL_PORT) \
		--config config.yaml \
		--filter-config config/filter-rules.yaml & \
	SERVER_PID=$$! ; \
	sleep 3 ; \
	python test_cql_proxy.py ; \
	TEST_RESULT=$$? ; \
	kill $$SERVER_PID ; \
	exit $$TEST_RESULT

docker-build:
	docker build -f Dockerfile -t $(DOCKER_IMAGE):$(VERSION) -t $(DOCKER_IMAGE):latest ../..

docker-run:
	docker run --rm -p $(CQL_PORT):$(CQL_PORT) \
		-v $(PWD)/config.yaml:/app/config.yaml:ro \
		-v $(PWD)/config/filter-rules.yaml:/app/filter-rules.yaml:ro \
		-e DUAL_WRITER_SOURCE_USERNAME=$(SOURCE_USER) \
		-e DUAL_WRITER_SOURCE_PASSWORD=$(SOURCE_PASS) \
		-e DUAL_WRITER_TARGET_USERNAME=$(TARGET_USER) \
		-e DUAL_WRITER_TARGET_PASSWORD=$(TARGET_PASS) \
		$(DOCKER_IMAGE):latest

clean:
	cargo clean

fmt:
	cargo fmt

clippy:
	cargo clippy -- -D warnings

bench:
	cargo bench

# Build entire workspace
build-all:
	cargo build --workspace --release

# Test entire workspace
test-all:
	cargo test --workspace

# Check compilation without building
check:
	cargo check --workspace