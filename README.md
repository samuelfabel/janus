# Janus

![CI](https://img.shields.io/github/actions/workflow/status/samuelfabel/janus/ci.yml?branch=main&label=CI)
![License](https://img.shields.io/github/license/samuelfabel/janus)

A modular **data kernel** written in Rust.

Janus explores protocols, storage engines, and cache building blocks behind a small execution core — not as a drop-in replacement for existing databases.

## Overview

- Layered design: transport, protocol, serializer, kernel, storage
- First milestone targets TCP + RESP + in-memory key/value (`SET` / `GET` / `DELETE` / `EXPIRE` / `TTL` / `SAVE` / `MULTI` / `EXEC` / `DISCARD` / `INFO` / `TRACES` / `AUTH`)
- Modular storage: `StorageEngine` trait + `Box<dyn StorageEngine>` in the Kernel (dependency inversion)
- Default plugin: `MemoryStorageEngine` (`HashMap`); pedagogical second plugin: `BTreeStorageEngine` (`BTreeMap`)
- Optional snapshot persistence via `--dbfile` / `JANUS_DBFILE`
- Optional append-only WAL via `--wal` / `JANUS_WAL` (mutually exclusive with `--dbfile`)
- Concurrent TCP clients share one in-memory store (`Arc<Mutex<Kernel>>`) over Tokio async TCP
- Library callers can provide a per-connection protocol factory with `listen_with_protocol` or `serve_with_protocol`; the command-line server remains TCP + RESP
- An experimental, library-only HTTP/3 edge is available through `janus-http3` and `serve_with_http3`; it uses caller-supplied TLS certificates and leaves the CLI on TCP + RESP
- Optional Cap'n Proto RPC edge (`janus-capnp-rpc`) exposes the same command/response domain over a dedicated TCP listener; enable it with Cargo `--features capnp`, then select Cap'n Proto alone or alongside RESP while both listeners share one Kernel
- Phase 10 experiment: in-process **primary → replica** via `ReplicationSink` + `apply_replication_record` (no Raft / cluster)
- Pedagogical **transactions**: Redis-style `MULTI` / `EXEC` / `DISCARD` (command queue + commit/abort; no WATCH / ACID claims)
- Pedagogical **metrics**: Kernel counters inspected via `INFO` (not Prometheus / OpenTelemetry)
- Pedagogical **tracing**: Kernel span ring buffer inspected via `TRACES` (not OTLP / W3C Trace Context)
- Pedagogical **authentication**: optional `requirepass` + `AUTH <password>` with a per-connection `NOAUTH` gate (no ACL / users / TLS)
- Pedagogical **authorization**: `Full` / `ReadOnly` roles chosen by the password used in `AUTH`, with a per-connection `NOPERM` gate for writes (no ACL / users / TLS)

This project does **not**:

- Replace Redis or PostgreSQL
- Aim to be production-ready in early versions
- Bundle every Redis command or clustered topology on day one
- Run a multi-node cluster or consensus protocol in the default server binary
- Provide WATCH, Lua scripting, or full ACID transactions
- Export Prometheus / OpenTelemetry metrics or distributed tracing
- Provide ACLs (per-command or per-key permissions), or TLS for the default TCP/RESP listener

## Status

TCP listen with RESP `SET` / `GET` / `DEL` / `EXPIRE` / `TTL` / `SAVE` / `MULTI` / `EXEC` / `DISCARD` / `INFO` / `TRACES` / `AUTH` over a pluggable in-memory store (lazy key expiry, optional snapshot file or WAL). Networking uses **Tokio** (`#[tokio::main]`, task per connection); all connections share one Kernel under a `Mutex`. The composition root injects storage via `build_storage()` — **Memory** by default; `BTreeStorageEngine` proves the plugin seam in tests. Default bind `0.0.0.0:6380`.

**Replication (Phase 10, pedagogical):** `cargo run` remains a **single-node** Memory primary. An optional `ReplicationSink` on the Kernel notifies after successful Set / Delete / Expire; a second in-process Kernel can apply those records (`apply_replication_record`). Covered by the harness under `crates/janus-kernel/src/replication/harness.rs` — not a production cluster, Raft, failover, or sharding.

**Transactions (pedagogical):** `MULTI` queues subsequent commands; `EXEC` applies them in order and returns a RESP array of results; `DISCARD` aborts without applying. Covered by the Tokio TCP e2e harness — not WATCH, nested MULTI, or distributed transactions.

**Metrics (pedagogical):** the Kernel tracks `total_commands`, `keyspace_hits`, and `keyspace_misses`. `INFO` returns a `# Stats` text bulk string with those counters. Covered by the Tokio TCP e2e harness — not Prometheus, OpenTelemetry, histograms, or tracing.

**Tracing (pedagogical):** the Kernel keeps a fixed-capacity ring buffer of command spans (`id` + command name). `TRACES` returns a `# Traces` text bulk string with the recent sequence. Covered by the Tokio TCP e2e harness — not OpenTelemetry, OTLP, or W3C Trace Context.

**Authentication (pedagogical):** the Kernel accepts an optional `requirepass` (`Kernel::with_requirepass`). When set, each TCP/RESP connection starts unauthenticated and every command except `AUTH <password>` returns `-NOAUTH Authentication required.`; a matching `AUTH` returns `+OK` and unlocks that connection only, a wrong one returns `-ERR invalid password`. Without a requirepass the server stays open and `AUTH` returns `-ERR AUTH called without any password configured`. `AUTH` inside `MULTI` is rejected, never queued. The default `cargo run` server has **no** password; requirepass is set in the composition root and is covered by the Tokio TCP e2e harness — not ACL, multiple users, `AUTH username password`, or TLS on the default TCP/RESP listener.

**Authorization (pedagogical):** next to `requirepass` (role `Full`), the Kernel accepts an optional read-only password (`Kernel::with_readonly_pass`, role `ReadOnly`). The password used in the last successful `AUTH` sets the role of that connection; a failed `AUTH` keeps the current role. A `ReadOnly` connection can run reads (`GET`, `TTL`, `INFO`, `TRACES`) and transaction control, but write commands (`SET`, `DEL`, `EXPIRE`, `SAVE`) return `-NOPERM this connection is read-only` without reaching the Kernel: inside `MULTI` they are not queued, and they are not counted in `INFO` or recorded in `TRACES`. With only a read-only password configured, every authenticated connection is `ReadOnly`. Roles are per connection and covered by the Tokio TCP e2e harness; the default `cargo run` server has **no** passwords — not ACLs, named users, Redis command categories, or TLS.

## Install / build

Requirements: [Rust](https://www.rust-lang.org/tools/install) (stable, edition 2021). Tokio is pulled in via `Cargo.toml` for the async TCP edge. Enabling Cap'n Proto RPC also requires the Cap'n Proto schema compiler (`capnp`) during the build; the default RESP build does not.

```bash
git clone https://github.com/samuelfabel/janus.git
cd janus
cargo test --workspace
cargo build --release
```

## Workspace layout

Janus is a Cargo workspace with one crate per layer. Dependencies point downward only, so the compiler rejects an upper layer leaking into a lower one.

| Crate | Layer | Depends on |
| --- | --- | --- |
| `janus-types` | `Command` / `Response` | — |
| `janus-storage` | `StorageEngine`, Memory / BTree engines, snapshot, WAL | — |
| `janus-kernel` | Kernel, roles, replication | `janus-types`, `janus-storage` |
| `janus-serializer` | `Serializer` + RESP codec | `janus-types` |
| `janus-protocol` | `Protocol` + RESP and HTTP protocol adapters | `janus-types`, `janus-serializer`, `janus-kernel`, `http` |
| `janus-transport` | Tokio TCP accept loop + connection instance | `janus-protocol`, `janus-serializer`, `janus-kernel` |
| `janus-http3` | Optional HTTP/3 server edge over Quinn | `janus-protocol`, `h3`, `h3-quinn`, `quinn`, `rustls`, Tokio |
| `janus-capnp-rpc` | Optional Cap'n Proto RPC edge with one Janus capability per TCP connection | `janus-protocol`, `janus-kernel`, `capnp`, `capnp-rpc`, Tokio |
| `janus` (root) | Composition root (`src/lib.rs`) + CLI binary (`src/main.rs`) | `janus-transport`, `janus-http3`, optional `janus-capnp-rpc`, `janus-protocol`, `janus-kernel`, `janus-storage` |

`janus-transport`, `janus-http3`, and the root package depend on Tokio. TCP end-to-end tests live in `tests/e2e.rs`; HTTP/3 loopback coverage lives in `crates/janus-http3/tests/`.

The HTTP/3 endpoint is experimental and opt-in for Rust library callers. `janus-http3::bind` configures a Quinn endpoint with ALPN `h3`, TLS 1.3, and 0-RTT disabled. Pass it with a bound TCP listener to `janus::serve_with_http3` to share one Kernel between RESP and HTTP/3. The supported HTTP routes are `GET`, `PUT`, and `DELETE /keys/{key}`; values are opaque bytes and request bodies are limited to 1 MiB. The command-line server, Docker image, and Compose configuration continue to expose TCP + RESP only.

The optional Cap'n Proto edge uses the Rust `capnp-rpc` Level 1 two-party runtime. Its schema is in `crates/janus-capnp-rpc/schema/janus.capnp`; clients bootstrap a `Janus` capability and invoke `execute` with a supported command. Values and keys remain opaque bytes and all calls reach the same domain Kernel as RESP. The Cap'n Proto listener uses a separate TCP address (default `0.0.0.0:6381`) and does not add TLS.

## Run the server

```bash
cargo run --release
# listens on 0.0.0.0:6380

cargo run --release -- --bind 127.0.0.1:6380
# or
JANUS_BIND=127.0.0.1:6380 cargo run --release

# optional snapshot path (SAVE writes here; process loads it on boot)
cargo run --release -- --dbfile /tmp/janus.snap
# or
JANUS_DBFILE=/tmp/janus.snap cargo run --release

# optional WAL path (mutations append here; process replays on boot)
cargo run --release -- --wal /tmp/janus.wal
# or
JANUS_WAL=/tmp/janus.wal cargo run --release

# optional Cap'n Proto RPC listener; RESP remains the default unless selected
cargo run --release --features capnp -- --protocol capnp --capnp-bind 127.0.0.1:6381

# RESP and Cap'n Proto RPC on independent TCP listeners with one Kernel
cargo run --release --features capnp -- --protocol both --bind 127.0.0.1:6380 --capnp-bind 127.0.0.1:6381
```

`JANUS_PROTOCOL=capnp` / `JANUS_PROTOCOL=both` select the same modes through the environment. `JANUS_CAPNP_BIND` configures the Cap'n Proto RPC address. Asking for either mode without compiling the feature exits with an error. To build a Docker image with Cap'n Proto enabled, use `docker build --build-arg JANUS_FEATURES=capnp -t janus:capnp .`.

Quick check with any RESP client (optional), for example:

```bash
printf '*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n' | nc 127.0.0.1 6380
printf '*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n' | nc 127.0.0.1 6380
printf '*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n' | nc 127.0.0.1 6380
printf '*1\r\n$4\r\nSAVE\r\n' | nc 127.0.0.1 6380
printf '*1\r\n$5\r\nMULTI\r\n' | nc 127.0.0.1 6380
printf '*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n' | nc 127.0.0.1 6380
printf '*1\r\n$4\r\nEXEC\r\n' | nc 127.0.0.1 6380
printf '*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n' | nc 127.0.0.1 6380
```

Without `--dbfile` / `JANUS_DBFILE`, `SAVE` returns an error (`ERR save disabled`).
`--dbfile` and `--wal` cannot both be set (`ERR conflicting persistence`).

Automated coverage lives in `cargo test --workspace` (Tokio TCP e2e harness in `tests/e2e.rs` on an ephemeral port, including EXPIRE/TTL, SAVE/restore, WAL recovery, multi-client shared-store concurrency, MULTI/EXEC/DISCARD transactions, the AUTH / requirepass gate, read-only roles with `NOPERM`, and the in-process primary→replica replication harness). HTTP/3 coverage uses a local test certificate and verifies multiple requests on one QUIC connection, the request body limit, and shared state across HTTP/3 and RESP.

## Docker

Multi-stage image (Rust builder → Debian slim runtime, non-root user). `cargo run` / the image entrypoint use the same Tokio server binary.

Default listen address (when the server binary binds): `0.0.0.0:6380` via `JANUS_BIND` or `--bind` (avoids clashing with Redis on `6379`).

```bash
docker build -t janus:local .
docker run --rm -p 6380:6380 janus:local
```

Override the listen address when the binary supports it:

```bash
docker run --rm -p 6380:6380 -e JANUS_BIND=0.0.0.0:6380 janus:local
# or
docker run --rm -p 7000:7000 janus:local --bind 0.0.0.0:7000
```

Compose:

```bash
docker compose up --build
```

## Website

Public landing page (English): source in [`site/`](site/), published via GitHub Pages.

```bash
cd site
npm install
npm run build
```

## Documentation

- [Architecture](docs/architecture.md)
- [Vision](docs/vision.md)
- [Roadmap](docs/roadmap.md)
- [Glossary](docs/glossary.md)
- [Engineering principles](docs/engineering.md)
- [ADRs](docs/adr/)

## Development

```bash
cargo test --workspace
cargo check --workspace --all-targets
```

Optional local git hooks (rejects tool co-author trailers in commit messages):

```bash
./scripts/install-git-hooks.sh
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

Please report vulnerabilities privately — see [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
