.PHONY: all build test clean docker-build docker-push fmt clippy

SERVICES := dual-writer sstable-loader dual-reader
DOCKER_REGISTRY := ghcr.io/yourusername
VERSION := $(shell git describe --tags --always --dirty)

all: build

build:
	cargo build --release

test:
	cargo test --workspace

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

clean:
	cargo clean

docker-build:
	@for service in $(SERVICES); do \
		echo "Building $$service:$(VERSION)"; \
		docker build -f services/$$service/Dockerfile \
			-t $(DOCKER_REGISTRY)/$$service:$(VERSION) \
			-t $(DOCKER_REGISTRY)/$$service:latest .; \
	done

docker-push: docker-build
	@for service in $(SERVICES); do \
		echo "Pushing $$service:$(VERSION)"; \
		docker push $(DOCKER_REGISTRY)/$$service:$(VERSION); \
		docker push $(DOCKER_REGISTRY)/$$service:latest; \
	done

run-dual-writer:
	cd services/dual-writer && cargo run -- --config ../../config/dual-writer.yaml

run-sstable-loader:
	cd services/sstable-loader && cargo run -- --config ../../config/sstable-loader.yaml

run-dual-reader:
	cd services/dual-reader && cargo run -- --config ../../config/dual-reader.yaml

docker-compose-up:
	docker-compose up -d

docker-compose-down:
	docker-compose down -v