# syntax=docker/dockerfile:1

FROM rust:1.88-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src

ARG JANUS_FEATURES=
RUN if [ -n "$JANUS_FEATURES" ]; then \
        apt-get update \
        && apt-get install -y --no-install-recommends capnproto protobuf-compiler \
        && rm -rf /var/lib/apt/lists/* \
        && cargo build --release --features "$JANUS_FEATURES"; \
    else \
        cargo build --release; \
    fi

FROM debian:bookworm-slim AS runtime

RUN useradd --system --uid 10001 --home /nonexistent --shell /usr/sbin/nologin janus \
    && mkdir -p /app \
    && chown janus:janus /app

COPY --from=builder /app/target/release/janus /usr/local/bin/janus

USER janus
WORKDIR /app

ENV JANUS_BIND=0.0.0.0:6380
EXPOSE 6380
EXPOSE 6381
EXPOSE 6382
EXPOSE 6383

ENTRYPOINT ["/usr/local/bin/janus"]
