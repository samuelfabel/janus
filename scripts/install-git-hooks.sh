#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
hooks_src="$root/.githooks"

if [[ ! -d "$hooks_src" ]]; then
  echo "versioned hooks not found at $hooks_src" >&2
  exit 1
fi

git -C "$root" config core.hooksPath .githooks
chmod +x "$hooks_src"/* "$root/scripts/"*.sh

echo "core.hooksPath=.githooks"
echo "Git hooks installed (tool co-author trailers are rejected)."
