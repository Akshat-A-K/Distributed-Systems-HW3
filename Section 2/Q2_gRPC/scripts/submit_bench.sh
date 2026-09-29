#!/bin/bash
#SBATCH --job-name=hw3-q2-bench
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=2G
#SBATCH --time=00:45:00
#SBATCH --output=results/bench_%j.log
#SBATCH --error=results/bench_%j.err
#SBATCH --partition=debug
#
# SLURM wrapper for the HW3 Section 2 Q2 benchmark sweep (E1 worker count,
# E2 streaming granularity, E3 concurrent queries) -- single node, same
# reasoning as submit_correctness.sh: HW2 Q8's MPI numbers and Q1's Hadoop
# numbers both came from one node, so Q2's numbers stay on one node too, to
# keep the MPI-vs-MapReduce-vs-gRPC comparison apples-to-apples rather than
# confounding it with a different node count.
#
# Run submit_correctness.sh first and confirm it passes before trusting any
# number this produces -- a fast, wrong system is not a result worth
# reporting.
#
# Submit from the project root (Q2_gRPC/):
#   sbatch scripts/submit_bench.sh
#   squeue -u $USER
#   cat results/bench_<jobid>.log
#   ls results/e1_workers.csv results/e2_granularity.csv results/e3_concurrent_queries.csv

set -uo pipefail

if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    cd "$SLURM_SUBMIT_DIR"
else
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
fi

echo "nproc: $(nproc)"
echo "=== Environment setup ==="
source scripts/setup_python_env.sh

bash scripts/build_reference.sh

if [[ ! -f data/generated/gen_n1000000_k5_s100_seed42.txt ]]; then
    bash scripts/make_datasets.sh
fi
if [[ ! -f src/weather_stream_pb2.py ]]; then
    .venv/bin/python3 -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/weather_stream.proto
fi

# Derived from the job id so concurrent jobs on this shared cluster don't
# collide on a hardcoded port (same reasoning as run_correctness.sh).
BENCH_PORT=$(( 56000 + ${SLURM_JOB_ID:-$$} % 5000 ))

echo "=== E1: worker count (N=1,000,000) ==="
.venv/bin/python3 scripts/bench.py --experiment E1 --port "$BENCH_PORT" \
    --dataset data/generated/gen_n1000000_k5_s100_seed42.txt --out results/e1_workers.csv --reps 3

echo "=== E2: streaming granularity (N=100,000) ==="
.venv/bin/python3 scripts/bench.py --experiment E2 --port "$BENCH_PORT" \
    --dataset data/generated/gen_n100000_k5_s100_seed42.txt --out results/e2_granularity.csv --workers 4 --reps 3

echo "=== E3: concurrent queries (N=1,000,000) ==="
.venv/bin/python3 scripts/bench.py --experiment E3 --port "$BENCH_PORT" \
    --dataset data/generated/gen_n1000000_k5_s100_seed42.txt --out results/e3_concurrent_queries.csv --workers 4 --reps 3

echo "Done. See results/e1_workers.csv, e2_granularity.csv, e3_concurrent_queries.csv"
