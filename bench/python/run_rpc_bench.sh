#!/usr/bin/env bash
# run_rpc_bench.sh — run SlimeRPC + Ray benchmarks (+ optional Pulsing) and print comparison.
#
# Usage:
#   bash run_rpc_bench.sh [--ctrl http://127.0.0.1:3000] [--buf-mb 256] [--max-size-mb 16] \
#                         [--scope rpc-bench-...] [--with-pulsing]
#
# Environment overrides:
#   CTRL=http://host:3000 MAX_SIZE_MB=16 WITH_PULSING=1 bash run_rpc_bench.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/../results"
CTRL="${CTRL:-http://127.0.0.1:3000}"
BUF_MB="${BUF_MB:-256}"
MAX_SIZE_MB="${MAX_SIZE_MB:-16}"
SCOPE="${SCOPE:-rpc-bench-$(date +%s)-$$}"
WITH_PULSING="${WITH_PULSING:-0}"

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --ctrl)   CTRL="$2";   shift 2 ;;
        --buf-mb) BUF_MB="$2"; shift 2 ;;
        --max-size-mb) MAX_SIZE_MB="$2"; shift 2 ;;
        --scope) SCOPE="$2"; shift 2 ;;
        --with-pulsing) WITH_PULSING=1; shift ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ "$WITH_PULSING" == "1" ]]; then
    TOTAL=4
else
    TOTAL=3
fi

mkdir -p "$RESULTS_DIR"
WORKER_LOG="$RESULTS_DIR/slime_worker_${SCOPE}.log"

echo "╔══════════════════════════════════════════════════╗"
if [[ "$WITH_PULSING" == "1" ]]; then
    echo "║   SlimeRPC vs Pulsing vs Ray — RPC Benchmark     ║"
else
    echo "║        SlimeRPC vs Ray — RPC Benchmark           ║"
fi
echo "╚══════════════════════════════════════════════════╝"
echo "  NanoCtrl : $CTRL"
echo "  Scope    : $SCOPE"
echo "  Buffer   : ${BUF_MB} MB"
echo "  Max Size : ${MAX_SIZE_MB} MB"
echo "  Pulsing  : $([[ "$WITH_PULSING" == "1" ]] && echo on || echo off)"
echo "  Results  : $RESULTS_DIR"
echo "  Worker Log: $WORKER_LOG"
echo ""

# ── [1/N] SlimeRPC ──────────────────────────────────────────────────────────
echo "▶ [1/$TOTAL] SlimeRPC — starting worker..."
PYTHONUNBUFFERED=1 python "$SCRIPT_DIR/rpc_bench_slime_worker.py" \
    --ctrl "$CTRL" --scope "$SCOPE" --buf-mb "$BUF_MB" \
    >"$WORKER_LOG" 2>&1 &
WORKER_PID=$!

# Give the worker time to register with NanoCtrl
sleep 2

echo "▶ [1/$TOTAL] SlimeRPC — running driver..."
if ! python "$SCRIPT_DIR/rpc_bench_slime_driver.py" \
    --ctrl "$CTRL" --scope "$SCOPE" --buf-mb "$BUF_MB" \
    --max-size-mb "$MAX_SIZE_MB" \
    --out "$RESULTS_DIR/slime_rpc.csv"; then
    echo ""
    echo "SlimeRPC benchmark failed. Worker log tail:"
    echo "────────────────────────────────────────────────────────────"
    tail -n 200 "$WORKER_LOG" || true
    echo "────────────────────────────────────────────────────────────"
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
    exit 1
fi

kill "$WORKER_PID" 2>/dev/null || true
wait "$WORKER_PID" 2>/dev/null || true
echo ""

STAGE=2

# ── [STAGE/N] Pulsing (optional) ────────────────────────────────────────────
if [[ "$WITH_PULSING" == "1" ]]; then
    echo "▶ [$STAGE/$TOTAL] Pulsing — running benchmark..."
    python "$SCRIPT_DIR/rpc_bench_pulsing.py" --out "$RESULTS_DIR/pulsing_rpc.csv"
    echo ""
    STAGE=$((STAGE + 1))
fi

# ── [STAGE/N] Ray ───────────────────────────────────────────────────────────
echo "▶ [$STAGE/$TOTAL] Ray — running benchmark..."
python "$SCRIPT_DIR/rpc_bench_ray.py" --out "$RESULTS_DIR/ray_rpc.csv"
echo ""
STAGE=$((STAGE + 1))

# ── [STAGE/N] Compare ───────────────────────────────────────────────────────
echo "▶ [$STAGE/$TOTAL] Comparison"
COMPARE_ARGS=(--slime "$RESULTS_DIR/slime_rpc.csv" --ray "$RESULTS_DIR/ray_rpc.csv")
if [[ "$WITH_PULSING" == "1" ]]; then
    COMPARE_ARGS+=(--pulsing "$RESULTS_DIR/pulsing_rpc.csv")
fi
python "$SCRIPT_DIR/rpc_bench_compare.py" "${COMPARE_ARGS[@]}"
