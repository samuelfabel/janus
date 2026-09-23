# Janus

![CI](https://img.shields.io/github/actions/workflow/status/samuelfabel/janus/ci.yml?branch=main&label=CI)
![License](https://img.shields.io/github/license/samuelfabel/janus)

A modular **data kernel** written in Rust.

Janus explores protocols, storage engines, and cache building blocks behind a small execution core — not as a drop-in replacement for existing databases.

## Overview

- Layered design: transport, protocol, serializer, kernel, storage
- First milestone targets TCP + RESP + in-memory key/value (`SET` / `GET` / `DELETE` / `EXPIRE` / `TTL` / `SAVE`)
- Modular storage: `StorageEngine` trait + `Box<dyn StorageEngine>` in the Kernel (dependency inversion)
- Default plugin: `MemoryStorageEngine` (`HashMap`); pedagogical second plugin: `BTreeStorageEngine` (`BTreeMap`)
- Optional snapshot persistence via `--dbfile` / `JANUS_DBFILE`
- Optional append-only WAL via `--wal` / `JANUS_WAL` (mutually exclusive with `--dbfile`)
- Concurrent TCP clients share one in-memory store (`Arc<Mutex<Kernel>>`) over Tokio async TCP

This project does **not**:

- Replace Redis or PostgreSQL
- Aim to be production-ready in early versions
- Bundle every Redis command or clustered topology on day one

## Status

TCP listen with RESP `SET` / `GET` / `DEL` / `EXPIRE` / `TTL` / `SAVE` over a pluggable in-memory store (lazy key expiry, optional snapshot file or WAL). Networking uses **Tokio** (`#[tokio::main]`, task per connection); all connections share one Kernel under a `Mutex`. The composition root injects storage via `build_storage()` — **Memory** by default; `BTreeStorageEngine` proves the plugin seam in tests. Default bind `0.0.0.0:6380`.

## Install / build

Requirements: [Rust](https://www.rust-lang.org/tools/install) (stable, edition 2021). Tokio is pulled in via `Cargo.toml` for the async TCP edge.

```bash
git clone https://github.com/samuelfabel/janus.git
cd janus
cargo test
cargo build --release
```

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
```

Quick check with any RESP client (optional), for example:

```bash
printf '*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n' | nc 127.0.0.1 6380
printf '*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n' | nc 127.0.0.1 6380
printf '*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n' | nc 127.0.0.1 6380
printf '*1\r\n$4\r\nSAVE\r\n' | nc 127.0.0.1 6380
printf '*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n' | nc 127.0.0.1 6380
```

Without `--dbfile` / `JANUS_DBFILE`, `SAVE` returns an error (`ERR save disabled`).
`--dbfile` and `--wal` cannot both be set (`ERR conflicting persistence`).

Automated coverage lives in `cargo test` (Tokio TCP e2e harness on an ephemeral port, including EXPIRE/TTL, SAVE/restore, WAL recovery, and multi-client shared-store concurrency).

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
cargo test
cargo check
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
