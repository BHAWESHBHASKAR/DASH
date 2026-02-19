FROM rust:1.83-bookworm AS builder
WORKDIR /src

COPY . .
RUN cargo build --release -p ingestion -p retrieval -p indexer -p benchmark-smoke

FROM debian:bookworm-slim
RUN useradd -m -u 10001 dash
WORKDIR /opt/dash

COPY --from=builder /src/target/release/ingestion /opt/dash/bin/ingestion
COPY --from=builder /src/target/release/retrieval /opt/dash/bin/retrieval
COPY --from=builder /src/target/release/segment-maintenance-daemon /opt/dash/bin/segment-maintenance-daemon
COPY --from=builder /src/target/release/benchmark-smoke /opt/dash/bin/benchmark-smoke

RUN mkdir -p /var/lib/dash && chown -R dash:dash /opt/dash /var/lib/dash
USER dash

ENV DASH_RETRIEVAL_BIND=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/opt/dash/bin/retrieval", "--serve"]
