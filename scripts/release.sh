#!/usr/bin/env bash
#
# release.sh — one-shot version bump + tag + push for the DLSlime monorepo.
#
# Usage:
#   scripts/release.sh 0.1.2                # bump to 0.1.2
#   scripts/release.sh 0.1.2 --no-push      # commit + tag locally, skip push
#   scripts/release.sh --dry-run 0.1.2      # show diff, don't touch git
#
# What it does (in order):
#   1. Validates the new version (semver MAJOR.MINOR.PATCH).
#   2. Reads the current version from dlslime/pyproject.toml (the data plane).
#   3. Rewrites the version in every authoritative manifest:
#        dlslime/pyproject.toml, dlslime-ctrl/pyproject.toml,
#        docs/pyproject.toml, dlslime-ctrl/Cargo.toml
#   4. Refreshes dlslime-ctrl/Cargo.lock via `cargo update -p dlslime-ctrl`.
#   5. Replaces old-version references in user-facing docs:
#        docker/README.md, docker/.env.example, docker/docker-compose.yml,
#        .github/workflows/docker-publish.yml (header comment)
#   6. Shows `git diff --stat`, asks for confirmation, then commits + tags + pushes.
#
# After the push the existing GH Actions workflow takes over:
#   - tag v<NEW> -> docker-publish.yml -> ghcr.io/deeplink-org/dlslime-ctrl:<NEW>

set -euo pipefail

# --- args ---------------------------------------------------------------------

DRY_RUN=0
PUSH=1
NEW=""

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --no-push) PUSH=0 ;;
    -h|--help)
      sed -n '2,30p' "$0"
      exit 0
      ;;
    *)
      if [[ -n "$NEW" ]]; then
        echo "ERROR: unexpected extra argument: $arg" >&2
        exit 2
      fi
      NEW="$arg"
      ;;
  esac
done

if [[ -z "$NEW" ]]; then
  echo "usage: $0 <new-version e.g. 0.1.2> [--dry-run] [--no-push]" >&2
  exit 2
fi

if ! [[ "$NEW" =~ ^[0-9]+\.[0-9]+\.[0-9]+(\.post[0-9]+)?$ ]]; then
  echo "ERROR: '$NEW' is not a valid version (expected MAJOR.MINOR.PATCH)" >&2
  exit 2
fi

# --- locate repo root ---------------------------------------------------------

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# --- discover current version -------------------------------------------------

OLD="$(grep -E '^version = "' dlslime/pyproject.toml | head -1 | sed -E 's/version = "([^"]+)"/\1/')"
if [[ -z "$OLD" ]]; then
  echo "ERROR: could not parse current version from dlslime/pyproject.toml" >&2
  exit 1
fi

if [[ "$OLD" == "$NEW" ]]; then
  echo "ERROR: new version $NEW is the same as current $OLD" >&2
  exit 2
fi

echo "Bumping: $OLD  ->  $NEW"

# --- working tree must be clean ----------------------------------------------

if [[ "$DRY_RUN" -eq 0 ]] && ! git diff --quiet; then
  echo "ERROR: working tree has unstaged changes. Commit or stash first." >&2
  exit 1
fi
if [[ "$DRY_RUN" -eq 0 ]] && ! git diff --cached --quiet; then
  echo "ERROR: working tree has staged but uncommitted changes." >&2
  exit 1
fi

# --- 1. authoritative manifests ----------------------------------------------

MANIFESTS=(
  "dlslime/pyproject.toml"
  "dlslime-ctrl/pyproject.toml"
  "docs/pyproject.toml"
  "dlslime-ctrl/Cargo.toml"
)

for f in "${MANIFESTS[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "WARN: skipping missing manifest $f" >&2
    continue
  fi
  # Anchor on `^version = "..."` (column 0) — safe for pyproject [project] tables
  # and Cargo [package] tables alike. Won't touch dep specs which are quoted differently.
  sed -i -E "0,/^version = \"[^\"]+\"$/s//version = \"$NEW\"/" "$f"
done

# --- 2. refresh Cargo.lock ----------------------------------------------------

if command -v cargo >/dev/null 2>&1; then
  (cd dlslime-ctrl && cargo update -p dlslime-ctrl >/dev/null)
else
  echo "WARN: cargo not in PATH — Cargo.lock NOT refreshed; CI will be inconsistent" >&2
fi

# --- 3. doc references --------------------------------------------------------

# These files reference the version as text (image tags, examples). Replace the
# *exact* old version. Using literal match to avoid touching e.g. clap "0.1.1"
# in Cargo.lock (which is already handled by cargo update above).

DOCS=(
  "docker/README.md"
  "docker/.env.example"
  "docker/docker-compose.yml"
  ".github/workflows/docker-publish.yml"
)

# Old major.minor (e.g. 0.1) — used in some doc examples
OLD_MM="${OLD%.*}"
NEW_MM="${NEW%.*}"

for f in "${DOCS[@]}"; do
  [[ -f "$f" ]] || continue
  # Escape dots in OLD for sed
  esc_old="${OLD//./\\.}"
  sed -i "s/${esc_old}/${NEW}/g" "$f"
  # Also bump "v<OLD>" mentions just in case
  sed -i "s/v${esc_old}/v${NEW}/g" "$f"
done

# --- 4. preview ---------------------------------------------------------------

echo
echo "=== diff stat ==="
git --no-pager diff --stat
echo
echo "=== full diff ==="
git --no-pager diff

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo
  echo "[dry-run] No git operations performed. Run 'git checkout .' to discard."
  exit 0
fi

# --- 5. confirm ---------------------------------------------------------------

echo
read -r -p "Commit, tag v$NEW, and push? [y/N] " ans
case "$ans" in
  y|Y|yes|YES) ;;
  *) echo "Aborted. Changes left in working tree."; exit 1 ;;
esac

# --- 6. commit + tag + push ---------------------------------------------------

git add -A
git commit -m "release: v$NEW"
git tag -a "v$NEW" -m "Release v$NEW"

if [[ "$PUSH" -eq 1 ]]; then
  git push origin HEAD
  git push origin "v$NEW"
  echo
  echo "✓ Pushed commit + tag v$NEW"
  echo
  echo "Track the docker build:"
  echo "  https://github.com/DeepLink-org/DLSlime/actions/workflows/docker-publish.yml"
  echo
  echo "After it goes green, the image will be available at:"
  echo "  ghcr.io/deeplink-org/dlslime-ctrl:$NEW"
else
  echo
  echo "✓ Committed and tagged v$NEW locally. Push when ready:"
  echo "  git push origin HEAD && git push origin v$NEW"
fi
