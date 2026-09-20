# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `StorageEngine` trait and `MemoryStorageEngine` with unit tests for get/set/delete
- Public landing page under `site/` with GitHub Pages workflow
- Multi-stage `Dockerfile`, `.dockerignore`, and `compose.yaml`
- TCP listener with `--bind` / `JANUS_BIND` (default `0.0.0.0:6380`)
- RESP codec path for SET / GET / DEL wired through kernel + memory storage
- Open-source community health files (Contributing, Code of Conduct, Security, Support)
- Conventional Commits / branch guidelines
- GitHub Actions CI (`cargo test`)
- Git hooks to reject tool co-author trailers in commit messages

### Changed

- TCP transport: growable per-connection buffer, compact-by-offset, write failure closes connection; e2e SET/GET/DEL, multi-message, and fragmented-frame tests
- In-memory storage supports per-key TTL with lazy expire (`expire_at` / `ttl`); `get` takes `&mut self`
- Kernel `Expire` / `Ttl` commands and `Response::Integer` (Redis-style -2/-1/seconds)
- RESP decode for `EXPIRE` / `TTL` fixtures; encode `Response::Integer`
- TCP e2e for `EXPIRE` / `TTL` (before/after deadline) and README command surface
- In-memory snapshot codec (`JANUSNAP`) with `export_snapshot` / `import_snapshot` (TTL as remaining seconds)
- Kernel `Save` with file snapshot store (`--dbfile` / `JANUS_DBFILE`), boot load, and `Response::Error`
- RESP decode for `SAVE` (arity 1, case-insensitive)
- TCP e2e for `SAVE` / restore via boot load and README `--dbfile` surface
- Append-only `JANUSWAL` codec with `WalWriter` (fsync per record) and replay into `StorageEngine`
- Kernel WAL append after successful Set/Delete/Expire, `--wal` / `JANUS_WAL` boot replay, and conflict with `--dbfile`
- TCP e2e for WAL recovery (no SAVE) and README `--wal` / `JANUS_WAL` surface
- Shared `Arc<Mutex<Kernel>>` via `lock_kernel` (poison → `into_inner`) with multi-thread Kernel tests
- Protocol `RespProtocol::shared` uses `lock_kernel` (decode/encode outside the mutex) with multi-thread fixtures
- TCP e2e for concurrent clients on a shared store and README concurrency notes
- Async `TcpInstance` on Tokio (`tokio::spawn`, no `std::thread`) with fragment/EOF unit tests
- Async accept loop + `#[tokio::main]` entry (production path without `std::net` / `block_on`)
- Tokio-native TCP e2e harness and README async runtime notes
- Protocol Instance `execute` returns `Result<usize, ProtocolError>` with STREAM-PROCESSING S1–S7 tests
- Kernel domain types: `Response::Deleted` and Command/Response docs; tests use `MemoryStorageEngine`
- Clarified `StorageEngine` / `MemoryStorageEngine` contract and storage unit tests
- Cargo edition set to `2021` for stable toolchain compatibility
- README expanded with build, docs, and contribution links

### Fixed

- Empty `LICENSE` replaced with full MIT text
