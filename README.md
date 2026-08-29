# Janus

![CI](https://img.shields.io/github/actions/workflow/status/samuelfabel/janus/ci.yml?branch=main&label=CI)
![License](https://img.shields.io/github/license/samuelfabel/janus)

A modular **data kernel** written in Rust.

Janus explores protocols, storage engines, and cache building blocks behind a small execution core — not as a drop-in replacement for existing databases.

## Overview

- Layered design: transport, protocol, serializer, kernel, storage
- First milestone targets TCP + RESP + in-memory key/value (`SET` / `GET` / `DELETE`)
- Storage behind a trait so engines can be swapped later

This project does **not**:

- Replace Redis or PostgreSQL
- Aim to be production-ready in early versions
- Bundle every Redis command or clustered topology on day one

## Status

Early development. The kernel and in-memory storage have unit tests. The binary currently ships a **TCP listen stub** (accepts connections; RESP command handling comes next). Default listen address: `0.0.0.0:6380` (avoids clashing with Redis on `6379`).

## Install / build

Requirements: [Rust](https://www.rust-lang.org/tools/install) (stable, edition 2021).

```bash
git clone https://github.com/samuelfabel/janus.git
cd janus
cargo test
cargo build --release
./target/release/janus --bind 127.0.0.1:6380
```

### Configuration

| Source | Variable / flag | Default |
|--------|-----------------|--------|
| CLI | `--bind <ADDR>` | — |
| Environment | `JANUS_BIND` | `0.0.0.0:6380` |

## Docker

Multi-stage image (Rust builder → Debian slim runtime, non-root user).

```bash
docker build -t janus:local .
docker run --rm -p 6380:6380 janus:local
```

Override the listen address:

```bash
docker run --rm -p 6380:6380 -e JANUS_BIND=0.0.0.0:6380 janus:local
# or
docker run --rm -p 7000:7000 janus:local --bind 0.0.0.0:7000
```

Compose:

```bash
docker compose up --build
```

Verify the port is open (listen stub accepts and closes):

```bash
nc -zv 127.0.0.1 6380
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
