# Commit Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/).

## Format

```text
<type>(<optional scope>): <description>

[optional body]

[optional footer(s)]
```

## Types

| Type | Use when |
|------|----------|
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation only |
| `style` | Formatting; no code change |
| `refactor` | Neither fix nor feature |
| `perf` | Performance improvement |
| `test` | Adding or fixing tests |
| `build` | Build system or dependencies |
| `ci` | CI configuration |
| `chore` | Maintenance |
| `revert` | Revert a previous commit |

## Subject rules

- Imperative mood (`add`, not `added`)
- No trailing period
- Prefer lowercase subject
- Keep around 72 characters or less

## Scopes (examples)

`storage`, `kernel`, `protocol`, `transport`, `docs`, `ci`

## Footers

- `Closes #123`
- `BREAKING CHANGE: <description>`

## Authorship

Do not add automated tool co-author trailers to commits. The author is the person who reviewed and accepted the change.
