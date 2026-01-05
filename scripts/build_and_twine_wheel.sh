#!/bin/bash
set -e

PYTHON_VERSIONS=("cp38-cp38" "cp39-cp39" "cp310-cp310" "cp311-cp311" "cp312-cp312" "cp313-cp313")

rm -rf dist build

for py_version in "${PYTHON_VERSIONS[@]}"; do
    echo "====================================="
    echo "Processing Python version: $py_version"
    echo "====================================="

    ORIGINAL_LD_LIBRARY_PATH=$LD_LIBRARY_PATH
    LIB_SLIME_PATH=/opt/python/${py_version}/lib/python3.*/site-packages/dlslime/
    export LD_LIBRARY_PATH=$LIB_SLIME_PATH:$LD_LIBRARY_PATH

    export PYTHON_PATH="/opt/python/$py_version/bin/"
    export PYTHON_EXE="$PYTHON_PATH/python"
    export PIP_EXE="$PYTHON_PATH/pip"

    $PIP_EXE install cmake pip build twine pybind11-stubgen scikit-build-core pybind11 ninja

    rm -rf build dlslime.egg-info
    # rm -f dlslime/*.pyi

    echo "Compiling and installing temporarily to generate stubs..."
    $PIP_EXE install -v -e . --no-build-isolation

    echo "Generating pyi stubs for $py_version..."

    bash scripts/gen_stubs.sh

    find dlslime -name "*.pyi" -print0 | xargs -0 sed -i 's/: json/: dict/g; s/-> json/-> dict/g; s/typing_extensions.CapsuleType/typing.Any/g'

    echo "✅ Stubs generated and patched! (Fixed json and CapsuleType)"

    echo "Building wheel..."
    $PYTHON_EXE -m build --wheel --no-isolation .

    $PIP_EXE uninstall -y dlslime

    LD_LIBRARY_PATH=$ORIGINAL_LD_LIBRARY_PATH

    echo "Completed processing $py_version"
    echo ""
done

echo "-------------------------------------"
echo "Renaming wheels to manylinux2014 tag..."
echo "-------------------------------------"

# 遍历 dist 目录下所有的 linux_x86_64 包
for whl in dist/*-linux_x86_64.whl; do
    # 检查文件是否存在，防止没有文件时报错
    [ -e "$whl" ] || continue

    # 使用 Bash 字符串替换功能，把 linux_x86_64 替换为 manylinux2014_x86_64
    new_name="${whl//linux_x86_64/manylinux2014_x86_64}"

    echo "Renaming: $(basename "$whl") -> $(basename "$new_name")"
    mv "$whl" "$new_name"
done

echo "Ready for upload..."

/opt/python/cp312-cp312/bin/twine upload dist/*
