#!/bin/bash
set -e  # 遇到错误立即退出

PYTHON_VERSIONS=("cp38-cp38" "cp39-cp39" "cp310-cp310" "cp311-cp311" "cp312-cp312")

rm -rf dist

for py_version in "${PYTHON_VERSIONS[@]}"; do
    echo "====================================="
    echo "Processing Python version: $py_version"
    echo "====================================="

    ORIGINAL_LD_LIBRARY_PATH=$LD_LIBRARY_PATH
    LIB_SLIME_PATH=/opt/python/${py_version}/lib/python3.8/site-packages/dlslime/
    export LD_LIBRARY_PATH=$LIB_SLIME_PATH:$LD_LIBRARY_PATH

    PYTHON_PATH="/opt/python/$py_version/bin"
    PYTHON_EXE="$PYTHON_PATH/python"
    PIP_EXE="$PYTHON_PATH/pip"

    $PIP_EXE install --upgrade pip build twine

    rm -rf build dlslime.egg-info

    $PYTHON_EXE -m build --wheel .

    auditwheel repair dist/*${py_version}*.whl --plat manylinux2014_x86_64 --exclude lib_slime_rdma --exclude libibverbs --exclude libnuma -w dist/

    # $PYTHON_PATH/twine upload dist/*
    LD_LIBRARY_PATH=$ORIGINAL_LD_LIBRARY_PATH

    echo "Completed processing $py_version"
    echo ""
done

rm -f dist/*-linux_*.whl

# 所有版本处理完成后统一上传（推荐）
echo "All versions processed, uploading to PyPI..."
/opt/python/cp312-cp312/bin/twine upload dist/*

echo "All tasks completed successfully!"
