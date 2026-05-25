#!/bin/bash
set -e

# Generate .pyi stubs for the dlslime C++ extension, into dlslime/dlslime/.
# Run from anywhere — anchors to the dlslime/ project root automatically.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"   # dlslime/
cd "$PROJECT_DIR"

${PYTHON_PATH}/pybind11-stubgen dlslime._slime_c --output-dir . \
    --ignore-unresolved-names json \
    --ignore-unresolved-names _abc._abc_data

find dlslime -name "*.pyi" -print0 | xargs -0 sed -i 's/: json/: dict/g; s/-> json/-> dict/g; s/typing_extensions.CapsuleType/typing.Any/g'

echo "✅ Stubs generated and patched! (Fixed json and CapsuleType)"
