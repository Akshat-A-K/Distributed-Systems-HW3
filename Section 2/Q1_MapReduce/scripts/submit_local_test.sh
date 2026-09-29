#!/bin/bash
#SBATCH --job-name=hw3-q1-local-test
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=2G
#SBATCH --time=00:15:00
#SBATCH --output=results/local_test_%j.log
#SBATCH --error=results/local_test_%j.err
#SBATCH --partition=debug
#
# SLURM wrapper for the HW3 Section 2 Q1 local pipeline test -- same
# pattern as HW2 Q8's submit_weather.sh: a thin SBATCH header that
# allocates one node, then delegates to an already-existing, already
# locally-verified driving script (scripts/run_local_pipe_test.sh).
#
# Needs NO Hadoop/YARN -- only g++ and python3, both standard on any Linux
# node -- so this should run cleanly with no cluster-specific setup.
#
# Runs two tests: the hand-built tie-break dataset (exercises known
# hottest/coldest/busiest-interval/top-K ties) and a freshly generated
# larger dataset, both split across multiple simulated mapper chunks --
# i.e. the exact same two tests already verified locally on a Mac.
#
# Submit from the project root (Q1_MapReduce/), so the relative
# results/ log paths above resolve correctly:
#   cd ~/Q1_MapReduce
#   sbatch scripts/submit_local_test.sh
#   squeue -u $USER                          # watch it
#   cat results/local_test_<jobid>.log       # check PASS/FAIL after
#
# -partition=debug is a guess carried over from HW2 Q8's submit_weather.sh
# on this same cluster; adjust with `sinfo` if this account's partitions
# differ.

set -uo pipefail

# sbatch copies the submitted script into a spool file and runs THAT copy,
# so ${BASH_SOURCE[0]} resolves to something like
# /var/spool/slurmd/jobNNN/slurm_script under sbatch -- not this script's
# real location. SLURM sets SLURM_SUBMIT_DIR to wherever `sbatch` was
# invoked from specifically to work around this; fall back to
# BASH_SOURCE-based detection only when run directly (no sbatch involved),
# e.g. `bash scripts/submit_local_test.sh` for a local sanity check.
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    cd "$SLURM_SUBMIT_DIR"
else
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
fi

overall_status=0

echo "===== Test 1: hand-built tie-break dataset, 3 mapper chunks ====="
./scripts/run_local_pipe_test.sh -d data/tie_test_with_header.txt -m 3
test1_status=$?
overall_status=$(( overall_status || test1_status ))

echo ""
echo "===== Test 2: freshly generated dataset (default), 7 mapper chunks ====="
./scripts/run_local_pipe_test.sh -m 7
test2_status=$?
overall_status=$(( overall_status || test2_status ))

echo ""
if [[ "$overall_status" -eq 0 ]]; then
    echo "ALL LOCAL TESTS PASSED."
else
    echo "AT LEAST ONE LOCAL TEST FAILED -- see PASS/FAIL messages above." >&2
fi
exit "$overall_status"
