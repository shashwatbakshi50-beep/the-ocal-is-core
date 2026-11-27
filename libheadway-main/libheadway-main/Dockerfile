# Builds the headway-cli binary from the libheadway Rust workspace.
FROM rust:bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake git pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY common/Cargo.toml common/Cargo.lock ./common/
COPY common/headway ./common/headway
COPY common/headway-cli ./common/headway-cli
COPY common/uniffi-bindgen ./common/uniffi-bindgen
COPY common/uniffi-bindgen-swift ./common/uniffi-bindgen-swift

WORKDIR /src/common
RUN cargo build --release -p headway-cli

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/common/target/release/headway-cli /usr/local/bin/headway-cli

RUN useradd --create-home --uid 10001 headway \
    && mkdir -p /data/tiles/user /data/tiles/system \
    && chown -R headway:headway /data

USER headway

ENV HEADWAY_STORAGE_DIR=/data \
    HEADWAY_BIND=0.0.0.0:9123

EXPOSE 9123

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -fsS http://127.0.0.1:9123/status || exit 1

CMD ["headway-cli", "serve"]
