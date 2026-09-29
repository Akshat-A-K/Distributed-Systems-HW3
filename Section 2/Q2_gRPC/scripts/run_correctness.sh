#!/bin/bash
# run_correctness.sh -- full local correctness matrix for HW3 Section 2 Q2.
#
# Starts ONE server process, then for every dataset x worker-count x
# partition x batch-size combination: Reset -> stream -> query --once ->
# diff against the C++ oracle (computed once per dataset and cached). On
# any mismatch, runs diagnose_mismatch.py instead of just failing silently.
#
# Usage: scripts/run_correctness.sh [--skip-gen] [--skip-large] [--large-only]
#   --skip-gen    Don't regenerate data/generated/*.txt (reuse existing).
#   --skip-large  Skip the 100000/1000000-record datasets (faster).
#   --large-only  Skip the small-dataset matrix entirely, run ONLY the
#                 100000/1000000-record reduced matrix. Useful when the
#                 small matrix has already been verified (e.g. on a
#                 different/faster machine) and you specifically want the
#                 large-N cases -- the only ones where a double-vs-long-
#                 double precision difference (see README) could surface --
#                 without waiting through the whole small matrix again.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    PROJECT_ROOT="$SLURM_SUBMIT_DIR"
else
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi
cd "$PROJECT_ROOT"

SKIP_GEN=0
SKIP_LARGE=0
LARGE_ONLY=0
for arg in "$@"; do
    case "$arg" in
        --skip-gen) SKIP_GEN=1 ;;
        --skip-large) SKIP_LARGE=1 ;;
        --large-only) LARGE_ONLY=1; SKIP_LARGE=0 ;;
    esac
done

if [[ -x "$PROJECT_ROOT/.venv/bin/python3" ]]; then
    PYTHON="$PROJECT_ROOT/.venv/bin/python3"
else
    PYTHON="python3"
fi
echo "Using python: $PYTHON ($($PYTHON --version 2>&1))"

echo "=== Build reference oracle + generator ==="
bash scripts/build_reference.sh

if [[ "$SKIP_GEN" -eq 0 ]]; then
    echo "=== Generate datasets ==="
    bash scripts/make_datasets.sh
fi

echo "=== Generate proto stubs ==="
"$PYTHON" -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/weather_stream.proto

# Derived from the SLURM job id (or this script's own PID outside SLURM) so
# two users' jobs on the same shared node don't collide on a hardcoded port.
PORT=$(( 51000 + ${SLURM_JOB_ID:-$$} % 5000 ))
HOST=127.0.0.1
# NOTE: BSD/macOS mktemp only substitutes a trailing XXXXXX -- a literal
# suffix after it (e.g. ".log") is NOT replaced, so a template like
# "hw3q2_server.XXXXXX.log" silently creates that literal, unsubstituted
# filename on first use and then fails with "File exists" on every rerun.
# Keep XXXXXX at the very end and skip the extension.
SERVER_LOG="$(mktemp "${TMPDIR:-/tmp}/hw3q2_server.XXXXXX")"

echo "=== Starting server on $HOST:$PORT ==="
(cd src && exec "$PYTHON" server.py --host "$HOST" --port "$PORT" --workers 4) > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 50); do
    if "$PYTHON" - "$HOST:$PORT" <<'EOF' 2>/dev/null
import sys, grpc
grpc.channel_ready_future(grpc.insecure_channel(sys.argv[1])).result(timeout=0.2)
EOF
    then
        break
    fi
    sleep 0.2
done

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hw3q2_correctness.XXXXXX")"
trap 'kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; rm -rf "$TMP_DIR"' EXIT

TOTAL=0
FAILED=0
declare -a FAIL_NAMES

run_case() {
    local dataset="$1" workers="$2" partition="$3" batch_size="$4"
    local name="$(basename "$dataset" .txt)_W${workers}_${partition}_B${batch_size}"
    TOTAL=$((TOTAL + 1))

    "$PYTHON" - "$HOST:$PORT" "$workers" "$partition" <<'EOF' > /dev/null 2>&1
import sys, grpc
sys.path.insert(0, "src")
import weather_stream_pb2 as pb2
import weather_stream_pb2_grpc as pb2_grpc
ch = grpc.insecure_channel(sys.argv[1])
pb2_grpc.WeatherAnalyticsStub(ch).Reset(pb2.ResetRequest(num_workers=int(sys.argv[2]), partition=sys.argv[3]))
EOF

    if ! (cd src && "$PYTHON" stream_client.py "$HOST:$PORT" "../$dataset" \
            --batch-size "$batch_size" --quiet) > "$TMP_DIR/${name}.stream.log" 2>&1; then
        echo "FAIL  $name  (stream_client error, see $TMP_DIR/${name}.stream.log)"
        FAILED=$((FAILED + 1))
        FAIL_NAMES+=("$name")
        return
    fi

    if ! (cd src && "$PYTHON" dashboard.py "$HOST:$PORT" --once) > "$TMP_DIR/${name}.actual.txt" 2>"$TMP_DIR/${name}.err"; then
        echo "FAIL  $name  (dashboard --once error, see $TMP_DIR/${name}.err)"
        FAILED=$((FAILED + 1))
        FAIL_NAMES+=("$name")
        return
    fi

    local oracle="$TMP_DIR/$(basename "$dataset").oracle.txt"
    if [[ ! -f "$oracle" ]]; then
        build/weather_seq "$dataset" > "$oracle"
    fi

    if diff -q "$oracle" "$TMP_DIR/${name}.actual.txt" > /dev/null; then
        echo "PASS  $name"
    else
        echo "FAIL  $name"
        "$PYTHON" scripts/diagnose_mismatch.py "$oracle" "$TMP_DIR/${name}.actual.txt" | sed 's/^/      /'
        FAILED=$((FAILED + 1))
        FAIL_NAMES+=("$name")
    fi
}

SMALL_DATASETS=(
    data/tie_test_with_header.txt
    data/tie_test_k3.txt
    data/edge_single_station.txt
    data/edge_cross_worker_ties.txt
)
[[ -f data/generated/gen_n5_k5_s100_seed42.txt ]] && SMALL_DATASETS+=(data/generated/gen_n5_k5_s100_seed42.txt)
[[ -f data/generated/gen_n100_k5_s100_seed42.txt ]] && SMALL_DATASETS+=(data/generated/gen_n100_k5_s100_seed42.txt)
[[ -f data/generated/gen_n10000_k5_s100_seed42.txt ]] && SMALL_DATASETS+=(data/generated/gen_n10000_k5_s100_seed42.txt)
[[ -f data/generated/gen_n123457_k10_s37_seed7.txt ]] && SMALL_DATASETS+=(data/generated/gen_n123457_k10_s37_seed7.txt)

if [[ "$LARGE_ONLY" -eq 0 ]]; then
    echo ""
    echo "=== Small-dataset matrix: W x {station,batch_rr} x B ==="
    for dataset in "${SMALL_DATASETS[@]}"; do
        for workers in 1 2 3 4 8; do
            for partition in station batch_rr; do
                for batch_size in 1 3 1000; do
                    run_case "$dataset" "$workers" "$partition" "$batch_size"
                done
            done
        done
    done
fi

if [[ "$SKIP_LARGE" -eq 0 ]]; then
    echo ""
    echo "=== Large-dataset reduced matrix: W in {1,4} x {station,batch_rr} x B=1000 ==="
    for size_spec in "gen_n100000_k5_s100_seed42" "gen_n1000000_k5_s100_seed42"; do
        dataset="data/generated/${size_spec}.txt"
        [[ -f "$dataset" ]] || continue
        for workers in 1 4; do
            for partition in station batch_rr; do
                run_case "$dataset" "$workers" "$partition" 1000
            done
        done
    done
fi

echo ""
echo "================================================================"
echo "Correctness matrix: $((TOTAL - FAILED))/$TOTAL passed."
if [[ "$FAILED" -gt 0 ]]; then
    echo "FAILED cases:"
    printf '  %s\n' "${FAIL_NAMES[@]}"
    echo "Server log: $SERVER_LOG"
    exit 1
fi
echo "ALL PASSED."
exit 0
