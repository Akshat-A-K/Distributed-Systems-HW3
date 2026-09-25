#!/usr/bin/env bash
#SBATCH --job-name=hw3-matrix-mapreduce
#SBATCH --output=hw3_matrix_%j.out
#SBATCH --error=hw3_matrix_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=16
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [[ "${1:-}" == "--submit-grid" ]]; then
    shift
    A_INPUT="${1:-$SCRIPT_DIR/matrix_a.txt}"
    B_INPUT="${2:-$SCRIPT_DIR/matrix_b.txt}"
    A_INPUT="$(realpath "$A_INPUT")"
    B_INPUT="$(realpath "$B_INPUT")"
    echo "Matrix A: $A_INPUT"
    echo "Matrix B: $B_INPUT"
    for NODE_COUNT in 1 2 3; do
        for PROCESS_COUNT in 1 2 4 8 16; do
            if (( PROCESS_COUNT < NODE_COUNT )); then
                continue
            fi
            echo "Submitting: nodes=$NODE_COUNT processes=$PROCESS_COUNT"
            sbatch \
                --nodes="$NODE_COUNT" \
                --ntasks="$PROCESS_COUNT" \
                --ntasks-per-node=16 \
                --cpus-per-task=1 \
                --export=ALL,A_INPUT="$A_INPUT",B_INPUT="$B_INPUT" \
                "$0" --grid-job
        done
    done
    exit 0
fi

if [[ "${1:-}" == "--grid-job" ]]; then
    A_FILE="${A_INPUT:-$SCRIPT_DIR/matrix_a.txt}"
    B_FILE="${B_INPUT:-$SCRIPT_DIR/matrix_b.txt}"
else
    A_FILE="${1:-$SCRIPT_DIR/matrix_a.txt}"
    B_FILE="${2:-$SCRIPT_DIR/matrix_b.txt}"
fi
A_FILE="$(realpath "$A_FILE")"
B_FILE="$(realpath "$B_FILE")"

TASKS="${SLURM_NTASKS:-1}"
JOB_ID="${SLURM_JOB_ID:-manual}"
WORK_DIR="$SCRIPT_DIR/rce_work_${JOB_ID}"
FINAL_OUTPUT="$SCRIPT_DIR/result_rce_${JOB_ID}.txt"
BENCHMARK_FILE="${RCE_BENCHMARK_FILE:-$SCRIPT_DIR/rce_benchmark_${JOB_ID}.csv}"