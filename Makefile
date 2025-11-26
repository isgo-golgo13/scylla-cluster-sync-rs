# =============================================================================
# Scylla Cluster Sync - Root Makefile
# Zero-downtime database migration toolkit
# =============================================================================

.PHONY: all build build-release test clean fmt clippy check \
        docker-build docker-push docker-up docker-down docker-logs \
        dual-writer dual-reader sstable-loader \
        help

# Configuration
DOCKER_REGISTRY ?= ghcr.io/enginevector
PROJECT_NAME := scylla-cluster-sync
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
RUST_VERSION := 1.83

# Default target
all: build

# =============================================================================
# Build Targets
# =============================================================================

## Build all services (debug mode)
build:
	@echo "Building all services (debug)..."
	cargo build --workspace

## Build all services (release mode)
build-release:
	@echo "Building all services (release)..."
	cargo build --workspace --release

## Build individual services
dual-writer:
	@echo "Building dual-writer..."
	cargo build --release --bin dual-writer

dual-reader:
	@echo "Building dual-reader..."
	cargo build --release --bin dual-reader

sstable-loader:
	@echo "Building sstable-loader..."
	cargo build --release --bin sstable-loader

# =============================================================================
# Code Quality
# =============================================================================

## Run all tests
test:
	@echo "Running tests..."
	cargo test --workspace

## Run tests with output
test-verbose:
	@echo "Running tests (verbose)..."
	cargo test --workspace -- --nocapture

## Format code
fmt:
	@echo "Formatting code..."
	cargo fmt --all

## Check formatting without changing files
fmt-check:
	@echo "Checking formatting..."
	cargo fmt --all -- --check

## Run clippy linter
clippy:
	@echo "Running clippy..."
	cargo clippy --workspace -- -D warnings

## Run clippy with fixes
clippy-fix:
	@echo "Running clippy with fixes..."
	cargo clippy --workspace --fix --allow-dirty

## Quick check (no codegen)
check:
	@echo "Checking code..."
	cargo check --workspace

## Fix warnings automatically
fix:
	@echo "Fixing warnings..."
	cargo fix --workspace --allow-dirty

# =============================================================================
# Docker Targets
# =============================================================================

## Build all Docker images
docker-build: docker-build-dual-writer docker-build-dual-reader docker-build-sstable-loader
	@echo "All Docker images built!"

## Build dual-writer Docker image
docker-build-dual-writer:
	@echo "Building dual-writer Docker image..."
	docker build -f services/dual-writer/Dockerfile.dual-writer \
		-t $(DOCKER_REGISTRY)/dual-writer:$(VERSION) \
		-t $(DOCKER_REGISTRY)/dual-writer:latest .

## Build dual-reader Docker image
docker-build-dual-reader:
	@echo "Building dual-reader Docker image..."
	docker build -f services/dual-reader/Dockerfile.dual-reader \
		-t $(DOCKER_REGISTRY)/dual-reader:$(VERSION) \
		-t $(DOCKER_REGISTRY)/dual-reader:latest .

## Build sstable-loader Docker image
docker-build-sstable-loader:
	@echo "Building sstable-loader Docker image..."
	docker build -f services/sstable-loader/Dockerfile.sstable-loader \
		-t $(DOCKER_REGISTRY)/sstable-loader:$(VERSION) \
		-t $(DOCKER_REGISTRY)/sstable-loader:latest .

## Push all Docker images
docker-push:
	@echo "Pushing Docker images..."
	docker push $(DOCKER_REGISTRY)/dual-writer:$(VERSION)
	docker push $(DOCKER_REGISTRY)/dual-writer:latest
	docker push $(DOCKER_REGISTRY)/dual-reader:$(VERSION)
	docker push $(DOCKER_REGISTRY)/dual-reader:latest
	docker push $(DOCKER_REGISTRY)/sstable-loader:$(VERSION)
	docker push $(DOCKER_REGISTRY)/sstable-loader:latest

## Start all services with Docker Compose
docker-up:
	@echo "Starting services..."
	docker-compose up -d

## Start services and follow logs
docker-up-logs:
	@echo "Starting services with logs..."
	docker-compose up

## Stop all services
docker-down:
	@echo "Stopping services..."
	docker-compose down

## Stop all services and remove volumes
docker-clean:
	@echo "Stopping services and removing volumes..."
	docker-compose down -v

## View logs
docker-logs:
	docker-compose logs -f

## View logs for specific service
docker-logs-service:
	docker-compose logs -f $(SVC)

# =============================================================================
# Development Helpers
# =============================================================================

## Start local ScyllaDB for development
dev-db:
	@echo "Starting local ScyllaDB instances..."
	docker-compose up -d scylla-source scylla-target
	@echo "Waiting for ScyllaDB to be ready..."
	@sleep 30
	@echo "ScyllaDB ready!"
	@echo "  Source: localhost:9042"
	@echo "  Target: localhost:9043"

## Run dual-writer locally
run-dual-writer: build
	@echo "Running dual-writer..."
	RUST_LOG=dual_writer=debug ./target/debug/dual-writer \
		--config config/dual-writer.yaml \
		--port 8080

## Run dual-reader locally
run-dual-reader: build
	@echo "Running dual-reader..."
	RUST_LOG=dual_reader=debug ./target/debug/dual-reader \
		--config config/dual-reader.yaml \
		--port 8082

## Run sstable-loader locally
run-sstable-loader: build
	@echo "Running sstable-loader..."
	RUST_LOG=sstable_loader=debug ./target/debug/sstable-loader \
		--config config/sstable-loader.yaml \
		--port 8081

## Watch and rebuild on changes
watch:
	@echo "Watching for changes..."
	cargo watch -x 'build --workspace'

## Generate documentation
docs:
	@echo "Generating documentation..."
	cargo doc --workspace --no-deps --open

# =============================================================================
# Cleanup
# =============================================================================

## Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean

## Clean everything including Docker
clean-all: clean docker-clean
	@echo "Cleaned build artifacts and Docker volumes"

# =============================================================================
# CI/CD Helpers
# =============================================================================

## Run all CI checks
ci: fmt-check clippy test
	@echo "All CI checks passed!"

## Pre-commit hook
pre-commit: fmt clippy test
	@echo "Pre-commit checks passed!"

# =============================================================================
# Help
# =============================================================================

## Show this help
help:
	@echo "Scylla Cluster Sync - Available Commands"
	@echo ""
	@echo "Build:"
	@echo "  make build            - Build all services (debug)"
	@echo "  make build-release    - Build all services (release)"
	@echo "  make dual-writer      - Build dual-writer only (release)"
	@echo "  make dual-reader      - Build dual-reader only (release)"
	@echo "  make sstable-loader   - Build sstable-loader only (release)"
	@echo ""
	@echo "Code Quality:"
	@echo "  make test             - Run all tests"
	@echo "  make fmt              - Format code"
	@echo "  make clippy           - Run linter"
	@echo "  make check            - Quick syntax check"
	@echo "  make fix              - Auto-fix warnings"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build     - Build all Docker images"
	@echo "  make docker-up        - Start all services"
	@echo "  make docker-down      - Stop all services"
	@echo "  make docker-logs      - Follow logs"
	@echo "  make docker-clean     - Stop and remove volumes"
	@echo ""
	@echo "Development:"
	@echo "  make dev-db           - Start local ScyllaDB"
	@echo "  make run-dual-writer  - Run dual-writer locally"
	@echo "  make run-dual-reader  - Run dual-reader locally"
	@echo "  make run-sstable-loader - Run sstable-loader locally"
	@echo "  make watch            - Watch and rebuild"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean            - Clean build artifacts"
	@echo "  make clean-all        - Clean everything"