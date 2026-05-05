#!/bin/bash
set -e

PYTHON_VERSIONS=("cp38-cp38")

rm -rf dist build

for py_version in "${PYTHON_VERSIONS[@]}"; do
    echo "====================================="
    echo "Processing Python version: $py_version"
    echo "====================================="

    export PYTHON_PATH="/opt/python/$py_version/bin/"
    export PYTHON_EXE="$PYTHON_PATH/python"
    export PIP_EXE="$PYTHON_PATH/pip"

    echo "Building wheel with maturin..."
    $PIP_EXE install "maturin>=1.0,<2.0"
    $PYTHON_PATH/maturin build --release --interpreter "$PYTHON_EXE" --out dist

    $PIP_EXE uninstall -y nanoctrl

    echo "Completed processing $py_version"
    echo ""
done

echo "Ready for upload..."

/opt/python/cp38-cp38/bin/twine upload dist/*
