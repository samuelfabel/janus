#!/usr/bin/env bash
# Fail if any commit in the range has tool co-author / generator trailers.
set -euo pipefail

range="${1:-}"

if [[ -z "$range" ]]; then
  if [[ -n "${GITHUB_BASE_REF:-}" ]]; then
    git fetch origin "${GITHUB_BASE_REF}" --depth=50 >/dev/null 2>&1 || true
    range="origin/${GITHUB_BASE_REF}...HEAD"
  else
    range="HEAD"
  fi
fi

fail=0
while IFS= read -r sha; do
  [[ -z "$sha" ]] && continue
  body="$(git log -1 --format='%B' "$sha")"
  if printf '%s\n' "$body" | grep -Eiq \
    -e '^[[:space:]]*Co-authored-by:[[:space:]].*Cursor' \
    -e 'cursoragent@cursor\.com' \
    -e 'noreply@cursor\.' \
    -e '^[[:space:]]*Made-with:[[:space:]]*Cursor' \
    -e '^[[:space:]]*Generated-by:[[:space:]]*Cursor' \
    -e '^[[:space:]]*Assisted-by:[[:space:]]*Cursor' \
    -e '^[[:space:]]*Co-authored-by:[[:space:]].*copilot' \
    -e '^[[:space:]]*Made-with:[[:space:]]*ChatGPT'
  then
    echo "FAIL $sha"
    printf '%s\n' "$body"
    echo "-----"
    fail=1
  fi
done < <(git rev-list "$range")

if [[ "$fail" -ne 0 ]]; then
  echo "History contains a commit with a tool co-author trailer." >&2
  exit 1
fi

echo "OK: no tool co-author trailers in range."
