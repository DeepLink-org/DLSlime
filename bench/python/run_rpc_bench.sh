#!/usr/bin/env bash
# run_rpc_bench.sh — run SlimeRPC + Ray benchmarks and print comparison.
#
# Usage:
#   bash run_rpc_bench.sh [--ctrl http://127.0.0.1:3000] [--buf-mb 256] [--max-size-mb 16]
#
# Environment overrides:
#   CTRL=http://host:3000 MAX_SIZE_MB=16 bash run_rpc_bench.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/../results"
CTRL="${CTRL:-http://127.0.0.1:3000}"
BUF_MB="${BUF_MB:-256}"
MAX_SIZE_MB="${MAX_SIZE_MB:-16}"

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --ctrl)   CTRL="$2";   shift 2 ;;
        --buf-mb) BUF_MB="$2"; shift 2 ;;
        --max-size-mb) MAX_SIZE_MB="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

mkdir -p "$RESULTS_DIR"

echo "╔══════════════════════════════════════════════════╗"
echo "║        SlimeRPC vs Ray — RPC Benchmark           ║"
echo "╚══════════════════════════════════════════════════╝"
echo "  NanoCtrl : $CTRL"
echo "  Buffer   : ${BUF_MB} MB"
echo "  Max Size : ${MAX_SIZE_MB} MB"
echo "  Results  : $RESULTS_DIR"
echo ""

# ── [1/3] SlimeRPC ──────────────────────────────────────────────────────────
echo "▶ [1/3] SlimeRPC — starting worker..."
python "$SCRIPT_DIR/rpc_bench_slime_worker.py" \
    --ctrl "$CTRL" --scope rpc-bench --buf-mb "$BUF_MB" &
WORKER_PID=$!

# Give the worker time to register with NanoCtrl
sleep 2

echo "▶ [1/3] SlimeRPC — running driver..."
python "$SCRIPT_DIR/rpc_bench_slime_driver.py" \
    --ctrl "$CTRL" --scope rpc-bench --buf-mb "$BUF_MB" \
    --max-size-mb "$MAX_SIZE_MB" \
    --out "$RESULTS_DIR/slime_rpc.csv"

kill "$WORKER_PID" 2>/dev/null || true
wait "$WORKER_PID" 2>/dev/null || true
echo ""

# ── [2/3] Ray ───────────────────────────────────────────────────────────────
echo "▶ [2/3] Ray — running benchmark..."
python "$SCRIPT_DIR/rpc_bench_ray.py" --out "$RESULTS_DIR/ray_rpc.csv"
echo ""

# ── [3/3] Compare ───────────────────────────────────────────────────────────
echo "▶ [3/3] Comparison"
python "$SCRIPT_DIR/rpc_bench_compare.py" \
    --slime "$RESULTS_DIR/slime_rpc.csv" \
    --ray   "$RESULTS_DIR/ray_rpc.csv"
