# =============================================================================
# Scylla Cluster Sync - Makefile
# =============================================================================

DOCKER_REGISTRY ?= docker.io/isgogolgo13
PROJECT_NAME := scylla-cluster-sync
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
RUST_VERSION := 1.83

# Target platform for GKE/EKS (linux/amd64)
PLATFORM ?= linux/amd64
BUILDX_BUILDER := multiarch

# =============================================================================
# Build Targets
# =============================================================================

.PHONY: build build-release

build:
	cargo build --workspace

build-release:
	cargo build --release --workspace

# =============================================================================
# Docker Buildx Setup
# =============================================================================

.PHONY: docker-setup

docker-setup:
	@echo "Setting up Docker buildx for cross-platform builds..."
	@docker buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || \
		docker buildx create --name $(BUILDX_BUILDER) --use --bootstrap
	@docker buildx use $(BUILDX_BUILDER)

# =============================================================================
# Docker Build (Cross-Platform)
# =============================================================================

.PHONY: docker-build docker-build-dual-writer docker-build-dual-reader docker-build-sstable-loader

docker-build: docker-setup docker-build-dual-writer docker-build-dual-reader docker-build-sstable-loader
	@echo "All images built for $(PLATFORM)"

docker-build-dual-writer: docker-setup
	@echo "Building dual-writer for $(PLATFORM)..."
	docker buildx build --platform $(PLATFORM) \
		-f services/dual-writer/Dockerfile.dual-writer \
		-t $(DOCKER_REGISTRY)/dual-writer:$(VERSION) \
		-t $(DOCKER_REGISTRY)/dual-writer:latest \
		--load .

docker-build-dual-reader: docker-setup
	@echo "Building dual-reader for $(PLATFORM)..."
	docker buildx build --platform $(PLATFORM) \
		-f services/dual-reader/Dockerfile.dual-reader \
		-t $(DOCKER_REGISTRY)/dual-reader:$(VERSION) \
		-t $(DOCKER_REGISTRY)/dual-reader:latest \
		--load .

docker-build-sstable-loader: docker-setup
	@echo "Building sstable-loader for $(PLATFORM)..."
	docker buildx build --platform $(PLATFORM) \
		-f services/sstable-loader/Dockerfile.sstable-loader \
		-t $(DOCKER_REGISTRY)/sstable-loader:$(VERSION) \
		-t $(DOCKER_REGISTRY)/sstable-loader:latest \
		--load .

# =============================================================================
# Docker Push
# =============================================================================

.PHONY: docker-push docker-push-dual-writer docker-push-dual-reader docker-push-sstable-loader

docker-push: docker-push-dual-writer docker-push-dual-reader docker-push-sstable-loader
	@echo "All images pushed to $(DOCKER_REGISTRY)"

docker-push-dual-writer:
	@echo "Pushing dual-writer..."
	docker push $(DOCKER_REGISTRY)/dual-writer:$(VERSION)
	docker push $(DOCKER_REGISTRY)/dual-writer:latest

docker-push-dual-reader:
	@echo "Pushing dual-reader..."
	docker push $(DOCKER_REGISTRY)/dual-reader:$(VERSION)
	docker push $(DOCKER_REGISTRY)/dual-reader:latest

docker-push-sstable-loader:
	@echo "Pushing sstable-loader..."
	docker push $(DOCKER_REGISTRY)/sstable-loader:$(VERSION)
	docker push $(DOCKER_REGISTRY)/sstable-loader:latest

# =============================================================================
# Docker Build + Push (Single Step - Faster)
# =============================================================================

.PHONY: docker-release docker-release-dual-writer docker-release-dual-reader docker-release-sstable-loader

docker-release: docker-setup docker-release-dual-writer docker-release-dual-reader docker-release-sstable-loader
	@echo "All images built and pushed to $(DOCKER_REGISTRY)"

docker-release-dual-writer: docker-setup
	@echo "Building and pushing dual-writer for $(PLATFORM)..."
	docker buildx build --platform $(PLATFORM) \
		-f services/dual-writer/Dockerfile.dual-writer \
		-t $(DOCKER_REGISTRY)/dual-writer:$(VERSION) \
		-t $(DOCKER_REGISTRY)/dual-writer:latest \
		--push .

docker-release-dual-reader: docker-setup
	@echo "Building and pushing dual-reader for $(PLATFORM)..."
	docker buildx build --platform $(PLATFORM) \
		-f services/dual-reader/Dockerfile.dual-reader \
		-t $(DOCKER_REGISTRY)/dual-reader:$(VERSION) \
		-t $(DOCKER_REGISTRY)/dual-reader:latest \
		--push .

docker-release-sstable-loader: docker-setup
	@echo "Building and pushing sstable-loader for $(PLATFORM)..."
	docker buildx build --platform $(PLATFORM) \
		-f services/sstable-loader/Dockerfile.sstable-loader \
		-t $(DOCKER_REGISTRY)/sstable-loader:$(VERSION) \
		-t $(DOCKER_REGISTRY)/sstable-loader:latest \
		--push .

# =============================================================================
# Docker Compose (Local Development)
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
	docker buildx prune -f

# =============================================================================
# TUI Dashboard
# =============================================================================

.PHONY: tui-demo tui-dash tui-live

tui-demo:
	@echo "Starting TUI Dashboard (Demo Mode)..."
	cargo run --bin tui-dash -- --demo

tui-dash:
	@echo "Starting TUI Dashboard (Live Mode)..."
	cargo run --bin tui-dash

## make tui-live API_URL=http://sstable-loader.prod:9092
tui-live:
	@echo "Starting TUI Dashboard (Live Mode)..."
	cargo run --bin tui-dash -- --api-url $(API_URL)

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
	@echo ""
	@echo "Scylla Cluster Sync - Makefile"
	@echo "=============================="
	@echo ""
	@echo "Docker (cross-platform for GKE/EKS):"
	@echo "  docker-build     - Build all images for $(PLATFORM) (local)"
	@echo "  docker-push      - Push all images to $(DOCKER_REGISTRY)"
	@echo "  docker-release   - Build AND push in one step (faster)"
	@echo ""
	@echo "Build:"
	@echo "  build            - Build all services (debug)"
	@echo "  build-release    - Build all services (release)"
	@echo ""
	@echo "TUI Dashboard:"
	@echo "  tui-demo         - Run TUI dashboard in demo mode (no DB required)"
	@echo "  tui-dash         - Run TUI dashboard (connects to sstable-loader API)"
	@echo ""
	@echo "Development:"
	@echo "  fmt              - Format code"
	@echo "  clippy           - Run linter"
	@echo "  test             - Run tests"
	@echo "  clean            - Clean build artifacts"
	@echo ""
	@echo "Configuration:"
	@echo "  DOCKER_REGISTRY  - $(DOCKER_REGISTRY)"
	@echo "  PLATFORM         - $(PLATFORM)"
	@echo "  VERSION          - $(VERSION)"
	@echo ""