# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Multi-stage `Dockerfile`, `.dockerignore`, and `compose.yaml`
- TCP listener with `--bind` / `JANUS_BIND` (default `0.0.0.0:6380`)
- RESP codec path for SET / GET / DEL wired through kernel + memory storage
- Open-source community health files (Contributing, Code of Conduct, Security, Support)
- Conventional Commits / branch guidelines
- GitHub Actions CI (`cargo test`)
- Git hooks to reject tool co-author trailers in commit messages

### Changed

- Cargo edition set to `2021` for stable toolchain compatibility
- README expanded with build, docs, and contribution links

### Fixed

- Empty `LICENSE` replaced with full MIT text
