#!/bin/bash
#SBATCH --job-name=hw3-q1-hadoop
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=2G
#SBATCH --time=00:30:00
#SBATCH --output=results/hadoop_run_%j.log
#SBATCH --error=results/hadoop_run_%j.err
#SBATCH --partition=debug
#
# SLURM wrapper for the HW3 Section 2 Q1 REAL Hadoop Streaming run.
#
# STATUS: like scripts/run_hadoop.sh itself, this is written but UNTESTED.
# No course-provided material documents a Hadoop/YARN setup on this
# cluster, unlike the RCE guide's coverage of the Section 3 gRPC problems.
#
# This script does NOT start, format, or otherwise modify any Hadoop
# daemons -- formatting an existing HDFS NameNode is destructive and wipes
# data, so that is never done automatically here. It only checks whether
# Hadoop appears to already be reachable and exits with a clear diagnosis
# if not. If this cluster turns out to require each user to bootstrap their
# own pseudo-distributed HDFS/YARN, that is a separate, deliberate, manual
# setup step -- confirm with course staff or documentation first, don't
# guess at it inside an automated batch job.
#
# Submit from the project root, once the pre-flight checks below have been
# sanity-checked interactively at least once (e.g. by running the same
# `hadoop version` / `jps` commands over a plain `ssh` session first):
#   cd ~/Q1_MapReduce
#   sbatch scripts/submit_hadoop.sh
#   squeue -u $USER
#   cat results/hadoop_run_<jobid>.log
#
# Adjust DATASET / NUM_REDUCERS below, and -partition=debug if this
# account's SLURM partitions differ (check with `sinfo`).

set -uo pipefail

# Confirmed on the actual RCE account (cs3401.08): `hadoop` needs BOTH of
# these modules, not just one -- hdfs/hdfs alone still fails with
# "ERROR: JAVA_HOME is not set" until java/11.0.13 is also loaded.
module load hdfs/hdfs
module load java/11.0.13

# sbatch copies the submitted script into a spool file and runs THAT copy,
# so ${BASH_SOURCE[0]} resolves to something like
# /var/spool/slurmd/jobNNN/slurm_script under sbatch -- not this script's
# real location. SLURM sets SLURM_SUBMIT_DIR to wherever `sbatch` was
# invoked from specifically to work around this; fall back to
# BASH_SOURCE-based detection only when run directly (no sbatch involved).
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    cd "$SLURM_SUBMIT_DIR"
else
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
fi

DATASET="data/tie_test_with_header.txt"
NUM_REDUCERS=2

echo "=== Pre-flight: checking for the 'hadoop' command ==="
if ! command -v hadoop >/dev/null 2>&1; then
    echo "ERROR: 'hadoop' not found on PATH in this job's environment." >&2
    echo "Check 'module avail' for a Hadoop module and add a 'module load ...'" >&2
    echo "line to this script, or confirm with course staff whether Hadoop is" >&2
    echo "actually provisioned on this cluster." >&2
    exit 1
fi
hadoop version || true

echo ""
echo "=== Pre-flight: checking whether HDFS/YARN daemons are already running ==="
if command -v jps >/dev/null 2>&1; then
    jps
    if ! jps | grep -qE 'NameNode|ResourceManager'; then
        echo "" >&2
        echo "WARNING: no NameNode/ResourceManager process seen via 'jps'." >&2
        echo "This script deliberately does not start or format HDFS/YARN --" >&2
        echo "see the header comment above for why. If a pseudo-distributed" >&2
        echo "Hadoop instance needs to be bootstrapped per-user on this" >&2
        echo "cluster, that is a manual step to work out first." >&2
        exit 1
    fi
else
    echo "('jps' not found -- cannot verify daemon status this way; proceeding" >&2
    echo " on the assumption 'hadoop' being on PATH means a working cluster.)" >&2
fi

echo ""
echo "=== Running scripts/run_hadoop.sh ==="
./scripts/run_hadoop.sh -d "$DATASET" -r "$NUM_REDUCERS"
status=$?

echo ""
if [[ "$status" -eq 0 ]]; then
    echo "run_hadoop.sh completed. This only means the JOB ran -- it does NOT"
    echo "confirm correctness. Compare results/hadoop_final_output.txt against"
    echo "weather_seq_reference before trusting it:"
    echo "  ./build/weather_seq $DATASET > results/sequential_output.txt"
    echo "  ./scripts/compare_correctness.sh results/hadoop_final_output.txt results/sequential_output.txt"
else
    echo "run_hadoop.sh FAILED (exit $status) -- see output above." >&2
fi
exit "$status"
