.PHONY: build run test docker-build docker-run clean

SERVICE_NAME := dual-reader
DOCKER_IMAGE := ghcr.io/yourusername/$(SERVICE_NAME)
VERSION := $(shell git describe --tags --always --dirty)

build:
	cargo build --release

run: build
	./target/release/$(SERVICE_NAME) --config config.yaml

test:
	cargo test

docker-build:
	docker build -f Dockerfile -t $(DOCKER_IMAGE):$(VERSION) -t $(DOCKER_IMAGE):latest ../..

docker-run:
	docker run --rm -p 8082:8082 \
		-v $(PWD)/config.yaml:/app/config.yaml:ro \
		$(DOCKER_IMAGE):latest

clean:
	cargo clean

fmt:
	cargo fmt

clippy:
	cargo clippy -- -D warnings