#!/bin/bash
set -e

# Build & twine-upload the `dlslime-ctrl` Rust bin wheel.
#
# Run from anywhere — anchors to dlslime-ctrl/ automatically.
#
# `dlslime-ctrl` is a `bindings = "bin"` maturin project: it ships a single
# `py3-none-manylinux_*.whl` containing the compiled Rust binary, so we only
# need ONE build (not per-Python-version) and no `--interpreter` flag.
#
# Set NO_UPLOAD=1 to build the wheel but skip `twine upload`.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"   # dlslime-ctrl/
cd "$PROJECT_DIR"

PYBIN="${PYBIN:-/opt/python/cp312-cp312/bin}"   # any working python+twine will do

rm -rf dist build

echo "Building wheel with maturin (bin crate, py3-none-*)..."
"$PYBIN/pip" install --quiet "maturin>=1.0,<2.0"
"$PYBIN/maturin" build --release --out dist

echo
echo "Built wheels:"
ls -la dist/

if [[ "${NO_UPLOAD:-0}" == "1" ]]; then
  echo
  echo "NO_UPLOAD=1 — skipping twine upload."
  exit 0
fi

echo
echo "Uploading to PyPI..."
"$PYBIN/twine" upload dist/*
