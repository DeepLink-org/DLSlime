#!/bin/bash
set -e

pybind11-stubgen dlslime._slime_c --output-dir . \
    --ignore-unresolved-names json \
    --ignore-unresolved-names _abc._abc_data

find dlslime -name "*.pyi" -print0 | xargs -0 sed -i 's/: json/: dict/g; s/-> json/-> dict/g; s/typing_extensions.CapsuleType/typing.Any/g'

echo "✅ Stubs generated and patched! (Fixed json and CapsuleType)"
