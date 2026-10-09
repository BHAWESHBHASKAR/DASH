# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
# Multi-stage, multi-arch build for the DASH services.
#
# One Dockerfile produces one image per service. Every image carries all
# service binaries and the same dispatcher entrypoint; the SERVICE build
# argument sets the default DASH_BIN, which selects the binary that runs
# (ingestion | retrieval | control-plane | segment-maintenance-daemon).
# DASH_BIN can still be overridden at run time.
#
# Naming scheme used by compose, Helm, raw manifests and release.yml:
#   ghcr.io/<owner>/dash-<service>:<tag>
#
# Stages:
#   builder  - rust toolchain image; builds release binaries for the whole
#              workspace and strips them.
#   dev      - extends builder, adds cargo-watch for local hot-reload.
#   runtime  - debian slim with the service binaries (root-owned, read-only
#              for the runtime user), the entrypoint, an HTTP healthcheck
#              and a non-root user. Final production image.
#
# Build one runtime image:
#   docker build -f deploy/container/Dockerfile --build-arg SERVICE=ingestion \
#       -t dash-ingestion:local .
#
# Build the dev image (with cargo-watch):
#   docker build -f deploy/container/Dockerfile --target dev -t dash:dev .
#
# Multi-arch build (linux/amd64, linux/arm64):
#   docker buildx build --platform linux/amd64,linux/arm64 \
#       -f deploy/container/Dockerfile --build-arg SERVICE=retrieval \
#       -t dash-retrieval:local --load .

# Base images are pinned by digest (tag@sha256:<multi-arch index digest>) so
# a rebuild of the same commit uses the same bytes; Dependabot
# (.github/dependabot.yml, docker ecosystem) proposes tag and digest bumps.
# The rust tag must match the channel in rust-toolchain.toml. Digests were
# read with `docker buildx imagetools inspect <image>:<tag>`; see
# docs/operations/supply-chain.md for how to re-resolve them.
FROM rust:1.99-bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6 AS builder
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
        target/release/control-plane \
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

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime
ARG UID=10001
ARG SERVICE=retrieval

RUN case "${SERVICE}" in \
        ingestion|retrieval|control-plane|segment-maintenance-daemon) ;; \
        *) echo "unsupported SERVICE build-arg: ${SERVICE}" >&2; exit 2 ;; \
    esac \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        wget \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid ${UID} dash \
    && useradd --uid ${UID} --gid dash --home-dir /nonexistent --no-create-home --shell /usr/sbin/nologin dash

# Binaries and scripts are owned by root and not writable by the runtime
# user. Only the data directories below belong to the dash user.
WORKDIR /opt/dash
COPY --from=builder --chown=root:root --chmod=0755 /build/target/release/ingestion /opt/dash/bin/ingestion
COPY --from=builder --chown=root:root --chmod=0755 /build/target/release/retrieval /opt/dash/bin/retrieval
COPY --from=builder --chown=root:root --chmod=0755 /build/target/release/control-plane /opt/dash/bin/control-plane
COPY --from=builder --chown=root:root --chmod=0755 /build/target/release/segment-maintenance-daemon /opt/dash/bin/segment-maintenance-daemon
COPY --chown=root:root --chmod=0755 deploy/container/scripts/entrypoint.sh /opt/dash/bin/dash-entrypoint
COPY --chown=root:root --chmod=0755 deploy/container/scripts/healthcheck.sh /opt/dash/bin/dash-healthcheck

RUN install -d -o dash -g dash -m 0750 \
        /var/lib/dash \
        /var/lib/dash/wal \
        /var/lib/dash/segments \
        /var/lib/dash/state \
        /var/lib/dash/audit

USER dash
ENV DASH_HOME=/opt/dash \
    DASH_BIN=${SERVICE} \
    PATH=/opt/dash/bin:$PATH

EXPOSE 8080 8081 8090

# dash-healthcheck probes the readiness endpoint of the selected service
# (/v1/ready on 8080 for retrieval, 8081 for ingestion, and
# /v1/control-plane/ready on 8090 for the control plane). Set
# DASH_HEALTHCHECK_URL to override the target.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/opt/dash/bin/dash-healthcheck"]

ENTRYPOINT ["/opt/dash/bin/dash-entrypoint"]
# No default CMD. The entrypoint sets --serve for ingestion, retrieval and
# control-plane when no command is supplied; segment-maintenance-daemon
# starts with no arguments.
