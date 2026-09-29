#!/bin/bash
#SBATCH --job-name=hw3-q1-benchmark
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=2G
#SBATCH --time=00:30:00
#SBATCH --output=results/benchmark_%j.log
#SBATCH --error=results/benchmark_%j.err
#SBATCH --partition=debug
#
# SLURM wrapper for Q1's benchmark.sh -- single node, same reasoning as
# Q2's submit_bench.sh: HW2 Q8's MPI numbers were measured on one node
# (--nodes=1 --ntasks-per-node=8), so Q1's numbers stay on one node too,
# for a fair comparison later.
#
# Submit from the project root (Q1_MapReduce/):
#   sbatch scripts/submit_benchmark.sh
#   squeue -u $USER
#   cat results/benchmark_<jobid>.log
#   ls results/benchmark_*.csv

set -uo pipefail

if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    cd "$SLURM_SUBMIT_DIR"
else
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
fi

echo "host: $(hostname)   arch: $(uname -m)   nproc: $(nproc)"
bash scripts/benchmark.sh -n "1000 10000 100000 1000000" -m "1 2 4 8" \
    -o "results/rce_benchmark_${SLURM_JOB_ID:-manual}.csv"
exit $?
