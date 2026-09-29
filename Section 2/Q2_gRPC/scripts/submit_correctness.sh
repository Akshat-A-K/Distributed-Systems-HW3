#!/bin/bash
#SBATCH --job-name=hw3-q2-correctness
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1G
#SBATCH --time=01:30:00
#SBATCH --output=results/correctness_%j.log
#SBATCH --error=results/correctness_%j.err
#SBATCH --partition=debug
#
# SLURM wrapper for the HW3 Section 2 Q2 correctness matrix -- same pattern
# as Q1's submit_local_test.sh: a thin SBATCH header allocating ONE node,
# delegating to the already-locally-verified scripts/run_correctness.sh.
#
# Deliberately single-node: HW2 Q8's MPI benchmark and Q1's Hadoop-substitute
# benchmark both ran on one node (--nodes=1), so Q2 stays on one node too --
# comparing paradigms across a different node count would confound the
# comparison with network-hop latency that has nothing to do with the
# paradigm itself. --cpus-per-task=8 gives real cores for up to W=8 worker
# threads plus gRPC's own RPC-handling threads on that one node.
#
# Needs python3 + a working `pip install -r requirements.txt` and g++.
# scripts/setup_python_env.sh loads the newest available `module load
# python/3.x` before building/reusing .venv (rebuilding it if the existing
# one is on Python <3.8) -- our first RCE run silently built .venv on this
# cluster's ancient default python3 (3.6.8), which is the likely cause of
# it running ~10-15x slower than expected and hitting the time limit.
#
# Submit from the project root (Q2_gRPC/). Any arguments after the
# script path are passed straight through to run_correctness.sh --
# e.g. `--skip-gen` (datasets/proto stubs already built, skip regenerating)
# or `--large-only` (skip the small-dataset matrix, run just the
# 100000/1000000-record cases -- the only ones sensitive to this
# architecture's long-double precision, see README):
#   cd ~/Q2_gRPC
#   sbatch scripts/submit_correctness.sh --skip-gen
#   sbatch scripts/submit_correctness.sh --skip-gen --large-only
#   squeue -u $USER                              # watch it
#   cat results/correctness_<jobid>.log          # check PASS/FAIL after
#
# Bumped to 90 minutes after the first RCE run (job 98701) came in ~10-15x
# slower per-case than the dev machine and got cut off by the original
# 30-minute limit at 234/240 small-matrix cases, never even reaching the
# large-dataset matrix. If this cluster's "debug" partition caps time below
# 90 minutes, SLURM will say so at submission -- check with
# `sinfo -p debug -o "%P %l"` and lower this if needed.

set -uo pipefail

if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    cd "$SLURM_SUBMIT_DIR"
else
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
fi

echo "=== Environment setup ==="
source scripts/setup_python_env.sh

bash scripts/run_correctness.sh "$@"
exit $?
