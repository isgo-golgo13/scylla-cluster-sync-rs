# =============================================================================
# Scylla Cluster Sync - Makefile
# =============================================================================

# Docker configuration - YOUR Docker Hub
DOCKER_REGISTRY ?= docker.io/isgogolgo13
PROJECT_NAME := scylla-cluster-sync
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
RUST_VERSION := 1.83

# Service names
SERVICES := dual-writer dual-reader sstable-loader

# =============================================================================
# Build Targets
# =============================================================================

.PHONY: build build-release build-dual-writer build-dual-reader build-sstable-loader

build:
	cargo build --workspace

build-release:
	cargo build --release --workspace

build-dual-writer:
	cargo build --release --bin dual-writer

build-dual-reader:
	cargo build --release --bin dual-reader

build-sstable-loader:
	cargo build --release --bin sstable-loader

# =============================================================================
# Docker Targets
# =============================================================================

.PHONY: docker-build docker-build-dual-writer docker-build-dual-reader docker-build-sstable-loader
.PHONY: docker-push docker-push-dual-writer docker-push-dual-reader docker-push-sstable-loader

docker-build: docker-build-dual-writer docker-build-dual-reader docker-build-sstable-loader

docker-build-dual-writer:
	@echo "Building dual-writer Docker image..."
	docker build -f services/dual-writer/Dockerfile.dual-writer \
		-t $(DOCKER_REGISTRY)/dual-writer:$(VERSION) \
		-t $(DOCKER_REGISTRY)/dual-writer:latest .

docker-build-dual-reader:
	@echo "Building dual-reader Docker image..."
	docker build -f services/dual-reader/Dockerfile.dual-reader \
		-t $(DOCKER_REGISTRY)/dual-reader:$(VERSION) \
		-t $(DOCKER_REGISTRY)/dual-reader:latest .

docker-build-sstable-loader:
	@echo "Building sstable-loader Docker image..."
	docker build -f services/sstable-loader/Dockerfile.sstable-loader \
		-t $(DOCKER_REGISTRY)/sstable-loader:$(VERSION) \
		-t $(DOCKER_REGISTRY)/sstable-loader:latest .

docker-push: docker-push-dual-writer docker-push-dual-reader docker-push-sstable-loader

docker-push-dual-writer:
	@echo "Pushing dual-writer to $(DOCKER_REGISTRY)..."
	docker push $(DOCKER_REGISTRY)/dual-writer:$(VERSION)
	docker push $(DOCKER_REGISTRY)/dual-writer:latest

docker-push-dual-reader:
	@echo "Pushing dual-reader to $(DOCKER_REGISTRY)..."
	docker push $(DOCKER_REGISTRY)/dual-reader:$(VERSION)
	docker push $(DOCKER_REGISTRY)/dual-reader:latest

docker-push-sstable-loader:
	@echo "Pushing sstable-loader to $(DOCKER_REGISTRY)..."
	docker push $(DOCKER_REGISTRY)/sstable-loader:$(VERSION)
	docker push $(DOCKER_REGISTRY)/sstable-loader:latest

# =============================================================================
# Docker Compose
# =============================================================================

.PHONY: docker-up docker-down docker-logs

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

# =============================================================================
# Development
# =============================================================================

.PHONY: fmt clippy test check clean

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

check:
	cargo check --workspace

clean:
	cargo clean

# =============================================================================
# CI/CD
# =============================================================================

.PHONY: ci pre-commit

ci: fmt clippy test build-release

pre-commit: fmt clippy test

# =============================================================================
# Help
# =============================================================================

.PHONY: help

help:
	@echo "Scylla Cluster Sync - Available targets:"
	@echo ""
	@echo "  Build:"
	@echo "    build              - Build all services (debug)"
	@echo "    build-release      - Build all services (release)"
	@echo ""
	@echo "  Docker:"
	@echo "    docker-build       - Build all Docker images"
	@echo "    docker-push        - Push all images to $(DOCKER_REGISTRY)"
	@echo ""
	@echo "  Development:"
	@echo "    fmt                - Format code"
	@echo "    clippy             - Run linter"
	@echo "    test               - Run tests"
	@echo "    clean              - Clean build artifacts"