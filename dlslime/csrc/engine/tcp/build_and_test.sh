#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
BUILD_DIR="$REPO_ROOT/build_tcp"
MODE="${1:-all}"

header() { echo; echo -e "\033[1;36m==>\033[m \033[1m$*\033[m"; }
ok()     { echo -e "  \033[1;32mOK\033[m   $*"; }

do_build() {
    header "Configuring (BUILD_TCP=ON, BUILD_RDMA=OFF)"
    cmake -S "$REPO_ROOT" -B "$BUILD_DIR" -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DDLSLIME_INSTALL_PATH=dlslime \
        -DBUILD_PYTHON=ON \
        -DBUILD_RDMA=OFF \
        -DBUILD_TCP=ON \
        -DBUILD_NVLINK=OFF \
        -DBUILD_ASCEND_DIRECT=OFF \
        -DSKBUILD_PROJECT_NAME=dlslime 2>&1 | tail -3
    ok "CMake configure"

    header "Building _slime_c"
    cmake --build "$BUILD_DIR" --target _slime_c -j"$(nproc)" 2>&1 | tail -8
    ok "Build complete"

    cp "$BUILD_DIR/lib/"*.so "$REPO_ROOT/dlslime/"
    ok "Copied .so files to dlslime/"
}

do_test() {
    header "Running TcpEndpoint v3 tests"
    export DLSLIME_LOG_LEVEL=0
    export LD_LIBRARY_PATH="$REPO_ROOT/dlslime"
    export PYTHONPATH="$REPO_ROOT"
    python3 "$SCRIPT_DIR/test_tcp_endpoint.py" 2>&1 | while IFS= read -r line; do
        if   [[ "$line" == *"PASSED"* ]]; then echo -e "  \033[1;32m✓\033[m $line"
        elif [[ "$line" == *"FAIL"*   ]]; then echo -e "  \033[1;91m✗\033[m $line"
        else echo "  $line"
        fi
    done
    ok "All tests passed"
}

case "$MODE" in
    all)    do_build; do_test ;;
    build)  do_build ;;
    test)   do_test ;;
    clean)  rm -rf "$BUILD_DIR" "$REPO_ROOT/dlslime/_slime_c"*.so "$REPO_ROOT/dlslime/lib_slime_"*.so
            ok "Cleaned" ;;
    *)      echo "Usage: $0 {all|build|test|clean}" >&2; exit 1 ;;
esac
