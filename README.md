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

Early development. TCP listen on `0.0.0.0:6380` with RESP `SET` / `GET` / `DEL` over an in-memory store.

## Install / build

Requirements: [Rust](https://www.rust-lang.org/tools/install) (stable, edition 2021).

```bash
git clone https://github.com/samuelfabel/janus.git
cd janus
cargo test
cargo build --release
```

## Docker

Multi-stage image (Rust builder → Debian slim runtime, non-root user).

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
