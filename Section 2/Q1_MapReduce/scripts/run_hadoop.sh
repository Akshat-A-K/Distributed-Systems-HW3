#!/bin/bash
# run_hadoop.sh -- real Hadoop Streaming execution for HW3 Section 2 Q1.
#
# STATUS: WRITTEN, REVIEWED AGAINST THE REAL RCE ENVIRONMENT, BUT NOT YET
# RUN -- blocked on a course-ACKNOWLEDGED outage, not an unknown. Diagnosed
# on the actual account (cs3401.08): Hadoop 3.3.0 is installed, HDFS is
# reachable and genuinely shared, but YARN's ResourceManager resolves to
# 0.0.0.0:8032 (a placeholder, not a real host). The course's own
# instructions page (General Instructions, item 6) independently confirms
# this is a known RCE-wide Hadoop environment issue and explicitly
# authorizes using a Slurm-based script instead until it's resolved --
# see scripts/run_local_pipe_test.sh / submit_local_test.sh, which is what
# this project's actual submitted results use in the meantime. This script
# is written to standard Hadoop Streaming conventions and follows the
# already-verified local pipeline (mapper.cpp / reducer.cpp / finalize.py)
# exactly, ready to use once the course posts the issue is resolved. Do not
# treat a clean-looking run of this script as proof of correctness on its
# own -- compare its output against weather_seq_reference the same way
# run_local_pipe_test.sh does.
#
# Assumptions (verify against the actual cluster before running):
#   - A `hadoop` command is already on PATH (e.g. after a `module load ...`
#     this script does not know the name of and does not attempt to run).
#   - HADOOP_STREAMING_JAR (below) points at a real hadoop-streaming jar.
#     The default is the standard Hadoop 3.3.6 layout; override it with
#     -j / --streaming-jar if the cluster's install differs.
#   - HDFS is reachable and writable at the paths this script uses.
#
# Pipeline:
#   local dataset (N K S header + records)
#     -> strip header locally, upload records-only file to HDFS
#     -> hadoop jar hadoop-streaming.jar -mapper mapper -reducer reducer
#        (configurable -D mapreduce.job.reduces, NOT assumed to be 1)
#     -> hadoop fs -cat the job's output part-files locally
#     -> finalize.py --k K (K read from the ORIGINAL local header, same as
#        run_local_pipe_test.sh -- never inferred from Hadoop's output)
#
# Usage:
#   scripts/run_hadoop.sh -d dataset_file [-r num_reducers] [-j streaming_jar]
#                          [-i hdfs_input_dir] [-o hdfs_output_dir]
#     -d dataset_file     Local "N K S" + records file (required).
#     -r num_reducers     Number of reduce tasks (default: 2). Must be
#                          possible to set >1 -- the architecture depends on
#                          this being a real, tunable parameter (see
#                          plan section 5 / README "Why GLOBAL is one key").
#     -j streaming_jar    Path to hadoop-streaming-*.jar. Default guesses
#                          the standard Hadoop 3.3.6 layout under
#                          $HADOOP_HOME; override if that's wrong here.
#     -i hdfs_input_dir   HDFS directory to upload records into
#                          (default: /user/$USER/hw3_q8/input).
#     -o hdfs_output_dir  HDFS directory for the job's output (default:
#                          /user/$USER/hw3_q8/output -- removed first if it
#                          already exists, since Hadoop refuses to write to
#                          an existing output directory).
#
# Exit status: 0 on success, nonzero on any failed step.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"

DATASET_FILE=""
NUM_REDUCERS=2
# Default confirmed on the actual RCE account (cs3401.08) via `find / -iname
# "hadoop-streaming*.jar"` after `module load hdfs/hdfs`: Hadoop 3.3.0 lives
# at /usr/local/apps/hadoop-3.3.0, and $HADOOP_HOME is empty there, so the
# old HADOOP_HOME-based guess (and its hardcoded "3.3.6") was wrong on both
# counts. Still overridable with -j if this differs on another account/module.
HADOOP_STREAMING_JAR="${HADOOP_STREAMING_JAR:-/usr/local/apps/hadoop-3.3.0/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar}"
HDFS_INPUT_DIR="/user/${USER:-hadoop}/hw3_q8/input"
HDFS_OUTPUT_DIR="/user/${USER:-hadoop}/hw3_q8/output"

usage() {
    grep '^#' "${BASH_SOURCE[0]}" | sed '1d' | sed 's/^# \{0,1\}//'
    exit 1
}

while getopts "d:r:j:i:o:h" opt; do
    case "$opt" in
        d) DATASET_FILE="$OPTARG" ;;
        r) NUM_REDUCERS="$OPTARG" ;;
        j) HADOOP_STREAMING_JAR="$OPTARG" ;;
        i) HDFS_INPUT_DIR="$OPTARG" ;;
        o) HDFS_OUTPUT_DIR="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

if [[ -z "$DATASET_FILE" ]]; then
    echo "ERROR: -d dataset_file is required." >&2
    usage
fi
if [[ ! -f "$DATASET_FILE" ]]; then
    echo "ERROR: dataset file not found: $DATASET_FILE" >&2
    exit 2
fi
if ! command -v hadoop >/dev/null 2>&1; then
    echo "ERROR: 'hadoop' command not found on PATH." >&2
    echo "  On the RCE cluster: module load hdfs/hdfs" >&2
    exit 2
fi
if [[ -z "${JAVA_HOME:-}" ]]; then
    echo "ERROR: JAVA_HOME is not set (Hadoop hard-requires it; this is the" >&2
    echo "  exact error 'hadoop version' gives without it)." >&2
    echo "  On the RCE cluster: module load java/11.0.13" >&2
    exit 2
fi
if [[ ! -f "$HADOOP_STREAMING_JAR" ]]; then
    echo "ERROR: Hadoop Streaming jar not found at: $HADOOP_STREAMING_JAR" >&2
    echo "  Pass the real path with -j on this cluster." >&2
    exit 2
fi

echo "=== Step 1/6: Building mapper.cpp and reducer.cpp ==="
mkdir -p "$BUILD_DIR"
CXXFLAGS="-O2 -std=c++17 -Wall -Wextra -pedantic"
g++ $CXXFLAGS -I"$PROJECT_ROOT/src" "$PROJECT_ROOT/src/mapper.cpp" -o "$BUILD_DIR/mapper"
g++ $CXXFLAGS -I"$PROJECT_ROOT/src" "$PROJECT_ROOT/src/reducer.cpp" -o "$BUILD_DIR/reducer"
echo "Build OK: mapper, reducer"

echo ""
echo "=== Step 2/6: Preparing local input (stripping N K S header) ==="
read -r N K S < "$DATASET_FILE"
echo "Parsed header: N=$N K=$K S=$S"
LOCAL_RECORDS_ONLY="$(mktemp "${TMPDIR:-/tmp}/hw3_q8_records.XXXXXX")"
tail -n +2 "$DATASET_FILE" > "$LOCAL_RECORDS_ONLY"

echo ""
echo "=== Step 3/6: Staging input on HDFS ==="
hadoop fs -mkdir -p "$HDFS_INPUT_DIR"
hadoop fs -put -f "$LOCAL_RECORDS_ONLY" "$HDFS_INPUT_DIR/records.txt"
# Hadoop refuses to write to an output dir that already exists.
hadoop fs -rm -r -f "$HDFS_OUTPUT_DIR" || true

echo ""
echo "=== Step 4/6: Submitting Hadoop Streaming job ($NUM_REDUCERS reducer(s)) ==="
hadoop jar "$HADOOP_STREAMING_JAR" \
    -D mapreduce.job.reduces="$NUM_REDUCERS" \
    -input "$HDFS_INPUT_DIR/records.txt" \
    -output "$HDFS_OUTPUT_DIR" \
    -mapper "mapper" \
    -reducer "reducer" \
    -file "$BUILD_DIR/mapper" \
    -file "$BUILD_DIR/reducer"

echo ""
echo "=== Step 5/6: Collecting reducer output from HDFS ==="
LOCAL_REDUCER_OUT="$(mktemp "${TMPDIR:-/tmp}/hw3_q8_reducer_out.XXXXXX")"
hadoop fs -cat "$HDFS_OUTPUT_DIR/part-*" > "$LOCAL_REDUCER_OUT"
echo "Reducer output collected: $(wc -l < "$LOCAL_REDUCER_OUT" | tr -d ' ') lines"

echo ""
echo "=== Step 6/6: Running finalize.py --k $K ==="
FINAL_OUTPUT="$PROJECT_ROOT/results/hadoop_final_output.txt"
mkdir -p "$PROJECT_ROOT/results"
python3 "$PROJECT_ROOT/finalize.py" --k "$K" < "$LOCAL_REDUCER_OUT" > "$FINAL_OUTPUT"

echo ""
echo "================================================================"
echo "Hadoop job complete. Final output: $FINAL_OUTPUT"
echo "This has NOT been compared against weather_seq_reference by this"
echo "script -- run scripts/compare_correctness.sh against a"
echo "weather_seq_reference run on the same dataset before trusting it:"
echo "  build/weather_seq $DATASET_FILE > results/sequential_output.txt"
echo "  scripts/compare_correctness.sh $FINAL_OUTPUT results/sequential_output.txt"
echo "================================================================"
