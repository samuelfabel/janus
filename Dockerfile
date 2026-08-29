# syntax=docker/dockerfile:1

FROM rust:1.83-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim AS runtime

RUN useradd --system --uid 10001 --home /nonexistent --shell /usr/sbin/nologin janus \
    && mkdir -p /app \
    && chown janus:janus /app

COPY --from=builder /app/target/release/janus /usr/local/bin/janus

USER janus
WORKDIR /app

ENV JANUS_BIND=0.0.0.0:6380
EXPOSE 6380

ENTRYPOINT ["/usr/local/bin/janus"]
