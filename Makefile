.PHONY: all build test fmt clippy doc ci docker secrets clean

all: fmt clippy test doc

build:
	cargo build --workspace --all-features

test:
	cargo test --workspace --all-features -- --test-threads=1

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-features -- -D warnings

doc:
	cargo doc --workspace --all-features --no-deps

ci:
	./scripts/ci.sh

docker:
	./scripts/generate-secrets.sh
	docker compose -f deploy/container/docker-compose.yml up -d --build

docker-ollama:
	./scripts/generate-secrets.sh
	docker compose \
	  -f deploy/container/docker-compose.yml \
	  -f deploy/container/docker-compose.ollama.yml \
	  --profile ollama up -d --build

docker-monitoring:
	./scripts/generate-secrets.sh
	docker compose \
	  -f deploy/container/docker-compose.yml \
	  -f deploy/container/docker-compose.monitoring.yml \
	  --profile monitoring up -d --build

secrets:
	./scripts/generate-secrets.sh

clean:
	cargo clean
	rm -rf target/
