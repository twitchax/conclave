# syntax=docker/dockerfile:1
#
# Multi-stage build for `conclave serve` (PRD-0009 T-003). `cargo-chef` caches the dependency layer
# so source-only changes rebuild fast. Config is env-driven (CONCLAVE_BIND / CONCLAVE_DATA_DIR /
# CONCLAVE_ADMINS); the embedded SurrealKV store lives on the /data volume. TLS terminates at the
# platform edge — the server speaks plain WS on its internal port (DESIGN §11/§12).

FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# Plan the dependency graph from the manifests only.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Cook dependencies (cached), then build the binary.
FROM chef AS builder
# Install the pinned nightly (rust-toolchain.toml) BEFORE cooking, so deps and the final build use
# the same toolchain and the cook layer is reused.
COPY rust-toolchain.toml .
RUN rustup show
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin conclave

# Minimal runtime: a slim glibc base (aws-lc-sys / SurrealKV binaries need glibc), non-root.
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /nonexistent --shell /usr/sbin/nologin conclave \
    && mkdir -p /data && chown conclave:conclave /data
COPY --from=builder /app/target/release/conclave /usr/local/bin/conclave
USER conclave
ENV CONCLAVE_BIND=0.0.0.0:4390 \
    CONCLAVE_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 4390
# Admins come from CONCLAVE_ADMINS (set a Fly secret, e.g. `you=<pubkey-b64>`); config is env-driven.
ENTRYPOINT ["conclave", "serve"]
