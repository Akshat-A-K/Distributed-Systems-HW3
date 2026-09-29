#!/bin/bash
# run_local_pipe_test.sh -- local, single-machine correctness test for the
# HW3 Section 2 Q1 Hadoop pipeline. Deliberately splits the dataset into
# MULTIPLE chunks and runs mapper.cpp independently on each one (simulating
# multiple mapper tasks), so this actually exercises reducer.cpp's
# cross-mapper combine logic -- not just a single mapper's pass-through.
#
# Pipeline exercised:
#   dataset (N K S header + records)
#     -> strip header, split records into M chunks
#     -> mapper.cpp x M (independently, one process per chunk)
#     -> concatenate + sort by key (tab-separated field 1)   [shuffle/sort]
#     -> reducer.cpp                                          [combine]
#     -> finalize.py --k K                                    [final assembly]
#     -> compared against weather_seq_reference on the SAME dataset
#
# Usage:
#   scripts/run_local_pipe_test.sh [-d dataset_file] [-m num_mapper_chunks] [-p]
#     -d dataset_file        Path to an existing "N K S" + records file.
#                             If omitted, a small dataset is generated.
#     -m num_mapper_chunks   How many mapper chunks to split records into
#                             (default: 3).
#     -p                     Preserve the temporary working directory
#                             instead of deleting it on exit (for inspection).
#
# Exit status: 0 on PASS, nonzero on FAIL or error.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"

DATASET_FILE=""
NUM_CHUNKS=3
PRESERVE_TMP=0

usage() {
    grep '^#' "${BASH_SOURCE[0]}" | sed '1d' | sed 's/^# \{0,1\}//'
    exit 1
}

while getopts "d:m:ph" opt; do
    case "$opt" in
        d) DATASET_FILE="$OPTARG" ;;
        m) NUM_CHUNKS="$OPTARG" ;;
        p) PRESERVE_TMP=1 ;;
        h) usage ;;
        *) usage ;;
    esac
done

if [[ "$NUM_CHUNKS" -lt 1 ]]; then
    echo "ERROR: -m num_mapper_chunks must be >= 1 (got: $NUM_CHUNKS)" >&2
    exit 2
fi

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hw3_q8_local_test.XXXXXX")"
cleanup() {
    if [[ "$PRESERVE_TMP" -eq 1 ]]; then
        echo "Temporary directory preserved at: $TMP_DIR"
    else
        rm -rf "$TMP_DIR"
    fi
}
trap cleanup EXIT

echo "=== Step 1/7: Building mapper, reducer, weather_seq_reference, generate ==="
mkdir -p "$BUILD_DIR"
CXXFLAGS="-O2 -std=c++17 -Wall -Wextra -pedantic"
g++ $CXXFLAGS -I"$PROJECT_ROOT/src" "$PROJECT_ROOT/src/mapper.cpp" -o "$BUILD_DIR/mapper"
g++ $CXXFLAGS -I"$PROJECT_ROOT/src" "$PROJECT_ROOT/src/reducer.cpp" -o "$BUILD_DIR/reducer"
g++ $CXXFLAGS "$PROJECT_ROOT/weather_seq_reference/weather_seq.cpp" -o "$BUILD_DIR/weather_seq"
g++ $CXXFLAGS "$PROJECT_ROOT/generate.cpp" -o "$BUILD_DIR/generate"
echo "Build OK: mapper, reducer, weather_seq, generate"

echo ""
echo "=== Step 2/7: Preparing dataset ==="
if [[ -z "$DATASET_FILE" ]]; then
    DATASET_FILE="$TMP_DIR/generated_dataset.txt"
    "$BUILD_DIR/generate" 200 5 12 42 > "$DATASET_FILE"
    echo "No dataset given -- generated $DATASET_FILE (N=200, K=5, S=12, seed=42)"
else
    if [[ ! -f "$DATASET_FILE" ]]; then
        echo "ERROR: dataset file not found: $DATASET_FILE" >&2
        exit 2
    fi
    echo "Using provided dataset: $DATASET_FILE"
fi

read -r N K S < "$DATASET_FILE"
echo "Parsed header: N=$N K=$K S=$S"

RECORDS_ONLY="$TMP_DIR/records_only.txt"
tail -n +2 "$DATASET_FILE" > "$RECORDS_ONLY"

ACTUAL_RECORD_COUNT="$(wc -l < "$RECORDS_ONLY" | tr -d ' ')"
if [[ "$ACTUAL_RECORD_COUNT" -ne "$N" ]]; then
    echo "ERROR: header claims N=$N records but file has $ACTUAL_RECORD_COUNT" >&2
    exit 2
fi

echo ""
echo "=== Step 3/7: Splitting records into $NUM_CHUNKS mapper chunks ==="
LINES_PER_CHUNK=$(( (N + NUM_CHUNKS - 1) / NUM_CHUNKS ))
if [[ "$LINES_PER_CHUNK" -lt 1 ]]; then LINES_PER_CHUNK=1; fi
split -l "$LINES_PER_CHUNK" "$RECORDS_ONLY" "$TMP_DIR/chunk_"
CHUNK_COUNT="$(find "$TMP_DIR" -name 'chunk_*' | wc -l | tr -d ' ')"
echo "Split into $CHUNK_COUNT chunk file(s) (requested $NUM_CHUNKS; fewer is expected if N is small)"

echo ""
echo "=== Step 4/7: Running mapper.cpp independently on each chunk ==="
MAPPER_OUT_DIR="$TMP_DIR/mapper_out"
mkdir -p "$MAPPER_OUT_DIR"
i=0
for chunk in "$TMP_DIR"/chunk_*; do
    i=$((i + 1))
    "$BUILD_DIR/mapper" < "$chunk" > "$MAPPER_OUT_DIR/mapper_$i.out"
    lines_in=$(wc -l < "$chunk" | tr -d ' ')
    lines_out=$(wc -l < "$MAPPER_OUT_DIR/mapper_$i.out" | tr -d ' ')
    echo "  mapper $i: $lines_in input records -> $lines_out summary lines"
done

echo ""
echo "=== Step 5/7: Concatenating + sorting by key (shuffle/sort simulation) ==="
SHUFFLED="$TMP_DIR/shuffled.txt"
cat "$MAPPER_OUT_DIR"/mapper_*.out | sort -t $'\t' -k1,1 > "$SHUFFLED"
echo "Shuffled input: $(wc -l < "$SHUFFLED" | tr -d ' ') total lines"

echo ""
echo "=== Step 6/7: Running reducer.cpp, then finalize.py --k $K ==="
REDUCER_OUT="$TMP_DIR/reducer_out.txt"
"$BUILD_DIR/reducer" < "$SHUFFLED" > "$REDUCER_OUT"

HADOOP_FINAL="$TMP_DIR/hadoop_final_output.txt"
python3 "$PROJECT_ROOT/finalize.py" --k "$K" < "$REDUCER_OUT" > "$HADOOP_FINAL"
echo "Final pipeline output written."

echo ""
echo "=== Step 7/7: Comparing against weather_seq_reference ==="
SEQ_OUT="$TMP_DIR/sequential_output.txt"
"$BUILD_DIR/weather_seq" "$DATASET_FILE" > "$SEQ_OUT"

echo ""
if "$SCRIPT_DIR/compare_correctness.sh" "$HADOOP_FINAL" "$SEQ_OUT"; then
    echo ""
    echo "================================================================"
    echo "PASS: local pipeline (dataset -> $CHUNK_COUNT mappers -> shuffle/sort"
    echo "-> reducer -> finalize.py) matches weather_seq_reference exactly."
    echo "Dataset: $DATASET_FILE (N=$N K=$K S=$S)"
    echo "================================================================"
    exit 0
else
    status=$?
    echo ""
    echo "================================================================"
    echo "FAIL: pipeline output does not match weather_seq_reference."
    echo "Dataset: $DATASET_FILE (N=$N K=$K S=$S)"
    echo "Hadoop-pipeline output: $HADOOP_FINAL"
    echo "Reference output:       $SEQ_OUT"
    echo "(rerun with -p to preserve $TMP_DIR for inspection)"
    echo "================================================================"
    exit "$status"
fi
