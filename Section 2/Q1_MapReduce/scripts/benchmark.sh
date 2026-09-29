#!/bin/bash
# benchmark.sh -- benchmark sweep for HW3 Section 2 Q1, using the sanctioned
# Slurm-based substitute pipeline (mapper -> shuffle/sort -> reducer ->
# finalize.py), not run_hadoop.sh (blocked by the course-acknowledged RCE
# Hadoop/YARN outage -- see README's "Hadoop execution" section).
#
# Deliberately does NOT call run_local_pipe_test.sh for the timed runs: that
# script rebuilds all 4 binaries on every invocation (a correctness-testing
# convenience, not a benchmarking one), which would add a ~1.5s fixed
# compile-time cost to every single configuration and swamp the actual
# N/mapper-count-dependent timing at smaller N. This script builds the
# binaries ONCE up front and times only the pipeline execution itself
# (split -> mapper x M -> shuffle/sort -> reducer -> finalize.py), then
# reuses compare_correctness.sh for the same correctness check
# run_local_pipe_test.sh would have done -- every benchmark row is still a
# correctness check, and a failing configuration is recorded as FAILED,
# never a fabricated time.
#
# What's actually a controllable "parallelism" knob under this substitute:
# ONLY the number of mapper chunks (-m). reducer.cpp always runs as a
# single process over the whole shuffled stream here -- there is no
# reducer-COUNT to vary, unlike a real Hadoop job's -numReduceTasks. This
# was an explicitly open question earlier in the project (extend the local
# pipeline to simulate multiple parallel reducers, or report mapper-count
# scaling only) -- resolved here as the latter: report mapper-chunk-count
# scaling, and state this limitation plainly rather than build machinery
# to fake reducer parallelism a real Hadoop job would actually have.
#
# Usage:
#   scripts/benchmark.sh [-n "size1 size2 ..."] [-m "m1 m2 ..."] [-k K] [-s S] [-o csv_file]
#     -n sizes     Space-separated list of N (record counts) to test.
#                  Default: "1000 10000 100000 1000000"
#     -m chunks    Space-separated list of mapper-chunk counts to test.
#                  Default: "1 2 4 8"
#     -k K         Top-K cutoff used for every generated dataset (default: 5).
#     -s S         Station count used for every generated dataset (default: 100).
#     -o csv_file  Output CSV path (default: results/benchmark_<timestamp>.csv).
#
# Exit status: 0 if every configuration passed correctness and was timed,
# nonzero if any configuration failed (failed rows are still recorded in
# the CSV as FAILED, never as a made-up time).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    PROJECT_ROOT="$SLURM_SUBMIT_DIR"
else
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi
BUILD_DIR="$PROJECT_ROOT/build"
RESULTS_DIR="$PROJECT_ROOT/results"

SIZES="1000 10000 100000 1000000"
CHUNK_COUNTS="1 2 4 8"
K=5
S=100
CSV_FILE=""
ANY_FAILED=0

usage() {
    grep '^#' "${BASH_SOURCE[0]}" | sed '1d' | sed 's/^# \{0,1\}//'
    exit 1
}

while getopts "n:m:k:s:o:h" opt; do
    case "$opt" in
        n) SIZES="$OPTARG" ;;
        m) CHUNK_COUNTS="$OPTARG" ;;
        k) K="$OPTARG" ;;
        s) S="$OPTARG" ;;
        o) CSV_FILE="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

cd "$PROJECT_ROOT"
mkdir -p "$RESULTS_DIR" "$BUILD_DIR"
if [[ -z "$CSV_FILE" ]]; then
    CSV_FILE="$RESULTS_DIR/benchmark_$(date +%Y%m%d_%H%M%S).csv"
fi

echo "=== Building mapper, reducer, weather_seq, generate (once) ==="
CXXFLAGS="-O2 -std=c++17 -Wall -Wextra -pedantic"
g++ $CXXFLAGS -I"$PROJECT_ROOT/src" "$PROJECT_ROOT/src/mapper.cpp" -o "$BUILD_DIR/mapper"
g++ $CXXFLAGS -I"$PROJECT_ROOT/src" "$PROJECT_ROOT/src/reducer.cpp" -o "$BUILD_DIR/reducer"
g++ $CXXFLAGS "$PROJECT_ROOT/weather_seq_reference/weather_seq.cpp" -o "$BUILD_DIR/weather_seq"
g++ $CXXFLAGS "$PROJECT_ROOT/generate.cpp" -o "$BUILD_DIR/generate"
echo "Build OK."

echo "n,mapper_chunks,seconds,correctness" > "$CSV_FILE"

run_one() {
    local n="$1" m="$2" dataset="$3"
    local tmp_dir
    tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hw3q1_bench.XXXXXX")"

    read -r hdr_n hdr_k hdr_s < "$dataset"
    local records_only="$tmp_dir/records_only.txt"
    tail -n +2 "$dataset" > "$records_only"

    local lines_per_chunk=$(( (hdr_n + m - 1) / m ))
    [[ "$lines_per_chunk" -lt 1 ]] && lines_per_chunk=1

    local start_ns end_ns
    start_ns=$(date +%s%N)

    split -l "$lines_per_chunk" "$records_only" "$tmp_dir/chunk_"
    mkdir -p "$tmp_dir/mapper_out"
    local i=0
    for chunk in "$tmp_dir"/chunk_*; do
        i=$((i + 1))
        "$BUILD_DIR/mapper" < "$chunk" > "$tmp_dir/mapper_out/mapper_$i.out"
    done
    cat "$tmp_dir"/mapper_out/mapper_*.out | sort -t $'\t' -k1,1 > "$tmp_dir/shuffled.txt"
    "$BUILD_DIR/reducer" < "$tmp_dir/shuffled.txt" > "$tmp_dir/reducer_out.txt"
    python3 "$PROJECT_ROOT/finalize.py" --k "$hdr_k" < "$tmp_dir/reducer_out.txt" > "$tmp_dir/pipeline_final.txt"

    end_ns=$(date +%s%N)
    local seconds
    seconds=$(echo "scale=6; ($end_ns - $start_ns) / 1000000000" | bc)

    "$BUILD_DIR/weather_seq" "$dataset" > "$tmp_dir/sequential_output.txt"
    if "$SCRIPT_DIR/compare_correctness.sh" "$tmp_dir/pipeline_final.txt" "$tmp_dir/sequential_output.txt" > "$tmp_dir/compare.log" 2>&1; then
        echo "N=$n mapper_chunks=$m: ${seconds}s (PASS)"
        echo "$n,$m,$seconds,PASS" >> "$CSV_FILE"
    else
        # Print the diff inline (not just a pointer to a temp path) since
        # tmp_dir is deleted below -- the diagnostic must survive in the
        # persistent SLURM log, not a path that's gone before anyone reads it.
        echo "N=$n mapper_chunks=$m: FAILED correctness (${seconds}s measured -- timing is" >&2
        echo "real, only the correctness verdict is FAIL; see README for the confirmed" >&2
        echo "floating-point precision explanation if this is the known N=100000 case)." >&2
        echo "--- diff (pipeline vs weather_seq_reference) ---" >&2
        cat "$tmp_dir/compare.log" >&2
        echo "--- end diff ---" >&2
        echo "$n,$m,$seconds,FAIL" >> "$CSV_FILE"
        ANY_FAILED=1
        rm -rf "$tmp_dir"
        return
    fi
    rm -rf "$tmp_dir"
}

for n in $SIZES; do
    dataset="$RESULTS_DIR/bench_dataset_n${n}.txt"
    "$BUILD_DIR/generate" "$n" "$K" "$S" 42 > "$dataset"

    for m in $CHUNK_COUNTS; do
        echo ""
        echo "=== N=$n, mapper_chunks=$m ==="
        run_one "$n" "$m" "$dataset"
    done
done

echo ""
echo "================================================================"
echo "Benchmark sweep complete."
echo "Results: $CSV_FILE"
echo "NOTE: mapper_chunks is the only parallelism knob this Slurm-based"
echo "substitute exposes -- reducer.cpp runs single-process here, unlike a"
echo "real Hadoop job's tunable -numReduceTasks. Reducer-count scaling is"
echo "not measurable under this substitute; report mapper-chunk scaling"
echo "only, and state this limitation in the report (see README)."
if [[ "$ANY_FAILED" -eq 1 ]]; then
    echo "Some configurations FAILED (see rows marked FAILED in the CSV)."
fi
echo "================================================================"

exit "$ANY_FAILED"
