# Contributing to Janus

Thank you for your interest in contributing.

## 1. Code of Conduct

Please read [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before participating.

## 2. How to contribute

- Bugs, features, and questions: use the GitHub issue forms
- Security issues: follow [SECURITY.md](SECURITY.md) — never open a public issue for vulnerabilities

## 3. Development setup

Requirements:

- Rust toolchain (stable) with Cargo
- Git

```bash
git clone https://github.com/samuelfabel/janus.git
cd janus
cargo test
cargo check
```

Optional: enable repository git hooks:

```bash
./scripts/install-git-hooks.sh
```

## 4. Repository structure

```text
src/
  command/    # domain commands
  response/   # domain responses
  kernel/     # command execution
  storage/    # StorageEngine trait + memory engine
  protocol/   # protocol layer (in progress)
docs/         # architecture, vision, roadmap, ADRs
```

## 5. Coding guidelines

- Code, comments, and public docs are **English**
- Prefer small, testable modules and explicit interfaces
- Follow existing formatting; run `cargo test` before opening a PR

## 6. Branch naming

Use Conventional Branch names:

```text
{type}/{id}-{short-description}
```

Examples:

- `chore/f0-01-oss-bootstrap`
- `feat/f1-01-memory-storage`
- `fix/tcp-buffer-compact`

Allowed `type` values: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `ci`.

Use kebab-case for the description. Keep it short.

## 7. Commit conventions

We follow [Conventional Commits](https://www.conventionalcommits.org/). See [COMMIT_CONVENTION.md](COMMIT_CONVENTION.md).

Examples:

```text
feat(storage): add memory storage engine
fix(kernel): return deleted flag correctly
docs: clarify architecture layers
```

Commit messages must not include tool co-author trailers. Authorship is the human who reviews and accepts the change. Local hooks and CI enforce this.

## 8. Pull request process

1. Fork (or branch from `main`)
2. Create a branch following the naming rules above
3. Keep the change focused; update docs when behavior changes
4. Ensure `cargo test` passes
5. Fill out `.github/pull_request_template.md`

## 9. Testing

```bash
cargo test
```

Add or update unit tests when changing kernel or storage behavior.

## 10. Documentation

Update files under `docs/` and the root `README.md` when user-facing behavior or architecture changes.

## 11. Security

Private reports only — [SECURITY.md](SECURITY.md).
