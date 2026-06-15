# syntax=docker/dockerfile:1.7
# Multi-stage, multi-arch build for the DASH retrieval engine.
#
# Stages:
#   builder  - rust:1.83-bookworm with the full toolchain, builds release
#              binaries for the entire workspace and strips them.
#   dev      - extends builder, adds cargo-watch for local hot-reload.
#   runtime  - debian:bookworm-slim with the three service binaries, a
#              dispatcher entrypoint, an HTTP healthcheck, and a non-root
#              user. Final production image.
#
# Build the runtime image:
#   docker build -f deploy/container/Dockerfile -t dash:latest .
#
# Build the dev image (with cargo-watch):
#   docker build -f deploy/container/Dockerfile --target dev -t dash:dev .
#
# Multi-arch build (linux/amd64, linux/arm64):
#   docker buildx build --platform linux/amd64,linux/arm64 \
#       -f deploy/container/Dockerfile -t dash:latest --load .

ARG RUST_VERSION=1.83
ARG DEBIAN_RELEASE=bookworm

FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder
WORKDIR /build
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the workspace manifest and member crate sources in one layer.
# cargo's incremental build keeps the recompile fast when only sources
# inside pkg/, services/, tests/, or tools/ change; the layer is only
# invalidated wholesale by changes to Cargo.toml or Cargo.lock.
COPY Cargo.toml Cargo.lock ./
COPY pkg ./pkg
COPY services ./services
COPY tests ./tests
COPY tools ./tools
RUN cargo build --release --workspace --locked

# Strip debug info from the three service binaries. The image size drops
# by roughly half once debug sections are removed.
RUN set -eux; \
    for bin in \
        target/release/ingestion \
        target/release/retrieval \
        target/release/segment-maintenance-daemon; do \
        if [ -f "$bin" ]; then \
            strip "$bin"; \
        else \
            echo "warning: $bin not built by cargo build --workspace" >&2; \
        fi; \
    done

FROM builder AS dev
RUN cargo install cargo-watch --locked
WORKDIR /build

FROM debian:${DEBIAN_RELEASE}-slim AS runtime
ARG UID=10001

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        wget \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid ${UID} dash \
    && useradd --uid ${UID} --gid dash --home /opt/dash --shell /usr/sbin/nologin dash

WORKDIR /opt/dash
RUN mkdir -p \
        /opt/dash/bin \
        /var/lib/dash/wal \
        /var/lib/dash/segments \
        /var/lib/dash/state

COPY --from=builder --chown=dash:dash /build/target/release/ingestion /opt/dash/bin/ingestion
COPY --from=builder --chown=dash:dash /build/target/release/retrieval /opt/dash/bin/retrieval
COPY --from=builder --chown=dash:dash /build/target/release/segment-maintenance-daemon /opt/dash/bin/segment-maintenance-daemon
COPY --chown=dash:dash deploy/container/scripts/entrypoint.sh /opt/dash/bin/dash-entrypoint
COPY --chown=dash:dash deploy/container/scripts/healthcheck.sh /opt/dash/bin/dash-healthcheck
RUN chmod 0755 /opt/dash/bin/dash-entrypoint /opt/dash/bin/dash-healthcheck \
    && chown -R dash:dash /opt/dash /var/lib/dash

USER dash
ENV DASH_HOME=/opt/dash \
    PATH=/opt/dash/bin:$PATH

EXPOSE 8080 8081

# Default healthcheck probes the retrieval service on 127.0.0.1:8080/health.
# docker-compose overrides this per service via DASH_HEALTHCHECK_URL.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/opt/dash/bin/dash-healthcheck"]

ENTRYPOINT ["/opt/dash/bin/dash-entrypoint"]
CMD ["--serve"]
