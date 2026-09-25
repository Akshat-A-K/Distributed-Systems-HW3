#!/usr/bin/env bash
# ==============================================================================
# run_local.sh - Local MapReduce Matrix Multiplication Runner for bash / Linux
# Runs the Row-Row MapReduce pipeline for EACH matrix category or single input.
# ==============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"
BENCHMARK_FILE="$RESULTS_DIR/local_benchmark.csv"

# Compile C++ mapper and reducer
echo "Compiling mapper and reducer..."
g++ -std=c++17 -O2 mapper.cpp -o "$SCRIPT_DIR/mapper"
g++ -std=c++17 -O2 reducer.cpp -o "$SCRIPT_DIR/reducer"

CATEGORIES=(
    "even-square"
    "odd-rectangular"
    "single-column"
    "single-row"
    "square"
    "tall"
    "wide"
    "large-1000x1000"
)

# Helper function to generate large 1000x1000 identity matrix if missing
ensure_large_matrix() {
    local target_dir="$SCRIPT_DIR/tests/large-1000x1000"
    if [ ! -f "$target_dir/matrix_a.txt" ] || [ ! -f "$target_dir/matrix_b.txt" ]; then
        mkdir -p "$target_dir"
        echo "Generating 1000x1000 identity test matrices..."
        python3 -c "
import os
size = 1000
td = '$target_dir'
with open(os.path.join(td, 'matrix_b.txt'), 'w') as fb, \
     open(os.path.join(td, 'matrix_a.txt'), 'w') as fa, \
     open(os.path.join(td, 'expected.txt'), 'w') as fe:
    fb.write(f'{size} {size}\n')
    for r in range(size):
        row_vals = ['1' if r == c else '0' for c in range(size)]
        fa.write(f'{r} ' + ' '.join(row_vals) + '\n')
        fb.write(' '.join(row_vals) + '\n')
        fe.write(' '.join(row_vals) + '\n')
" 2>/dev/null || true
    fi
}

# Function: Run local benchmark on a single matrix configuration
run_matrix() {
    local a_file="$1"
    local b_file="$2"
    local mapper_tasks="${3:-1}"
    local cat_name="${4:-custom}"
    local exp_file="${5:-}"

    a_file="$(realpath "$a_file")"
    b_file="$(realpath "$b_file")"

    if [ ! -f "$a_file" ] || [ ! -f "$b_file" ]; then
        echo "Error: Files not found: $a_file, $b_file"
        return 1
    fi

    local temp_dir
    temp_dir="$(mktemp -d 2>/dev/null || mktemp -d -t hw3_local_XXXXXX)"
    local final_output="$temp_dir/result.txt"

    local N=$(awk 'NR == 1 { print $1 }' "$b_file")
    local P=$(awk 'NR == 1 { print $2 }' "$b_file")
    local M=$(wc -l < "$a_file" | tr -d ' ')
    local A_BYTES=$(stat -c%s "$a_file" 2>/dev/null || stat -f%z "$a_file")
    local B_BYTES=$(stat -c%s "$b_file" 2>/dev/null || stat -f%z "$b_file")

    echo "--- Category: $cat_name | Tasks: $mapper_tasks | Dimensions: ${M}x${N} * ${N}x${P} ---"

    local TOTAL_START=$(date +%s%N)

    # 1. Split matrix A into chunks
    local SPLIT_START=$(date +%s%N)
    for (( t=0; t<mapper_tasks; t++ )); do
        local tid=$(printf "%03d" "$t")
        : > "$temp_dir/chunk_${tid}.txt"
    done

    awk -v p="$mapper_tasks" -v d="$temp_dir" '{
        tid = sprintf("%03d", (NR - 1) % p)
        print $0 >> (d "/chunk_" tid ".txt")
    }' "$a_file"
    local SPLIT_END=$(date +%s%N)

    # 2. Mapper stage (parallel subshells)
    local MAPPER_START=$(date +%s%N)
    local pids=()
    for (( t=0; t<mapper_tasks; t++ )); do
        local tid=$(printf "%03d" "$t")
        "$SCRIPT_DIR/mapper" "$b_file" < "$temp_dir/chunk_${tid}.txt" > "$temp_dir/map_${tid}.out" 2> "$temp_dir/map_${tid}.err" &
        pids+=($!)
    done

    for pid in "${pids[@]}"; do
        wait "$pid"
    done
    local MAPPER_END=$(date +%s%N)

    # 3. Shuffle / Sort
    local SHUFFLE_START=$(date +%s%N)
    cat "$temp_dir"/map_*.out | sort -k1,1n > "$temp_dir/global_sorted.out"
    local SHUFFLE_END=$(date +%s%N)

    # 4. Reducer stage
    local REDUCER_START=$(date +%s%N)
    "$SCRIPT_DIR/reducer" < "$temp_dir/global_sorted.out" > "$final_output"
    local REDUCER_END=$(date +%s%N)
    local TOTAL_END=$(date +%s%N)

    # Calculate timings
    seconds() { awk "BEGIN { printf \"%.6f\", ($2 - $1) / 1000000000 }"; }
    local SPLIT_SECONDS=$(seconds "$SPLIT_START" "$SPLIT_END")
    local MAPPER_SECONDS=$(seconds "$MAPPER_START" "$MAPPER_END")
    local SHUFFLE_SECONDS=$(seconds "$SHUFFLE_START" "$SHUFFLE_END")
    local REDUCER_SECONDS=$(seconds "$REDUCER_START" "$REDUCER_END")
    local TOTAL_SECONDS=$(seconds "$TOTAL_START" "$TOTAL_END")
    local OUTPUT_BYTES=$(stat -c%s "$final_output" 2>/dev/null || stat -f%z "$final_output")
    local THROUGHPUT=$(awk "BEGIN { if ($TOTAL_SECONDS > 0) printf \"%.6f\", $M / $TOTAL_SECONDS; else print \"0\" }")

    # Correctness check against expected.txt if provided
    local status="PASS"
    if [ -n "$exp_file" ] && [ -f "$exp_file" ]; then
        if ! diff -qwB "$exp_file" "$final_output" >/dev/null 2>&1; then
            echo "  [FAIL] Output did not match expected for $cat_name ($mapper_tasks tasks)!"
            status="FAIL"
        else
            echo "  [PASS] Correctness verified against expected.txt"
        fi
    fi

    # Log to CSV
    local HEADER="n,m,p,mapper_tasks,input_rows,input_bytes,matrix_b_bytes,mapper_time_s,shuffle_time_s,reducer_time_s,total_time_s,output_bytes,throughput_rows_per_s,input_file,category"
    if [ ! -f "$BENCHMARK_FILE" ]; then
        echo "$HEADER" > "$BENCHMARK_FILE"
    fi
    echo "$N,$M,$P,$mapper_tasks,$M,$A_BYTES,$B_BYTES,$MAPPER_SECONDS,$SHUFFLE_SECONDS,$REDUCER_SECONDS,$TOTAL_SECONDS,$OUTPUT_BYTES,$THROUGHPUT,$(basename "$a_file"),$cat_name" >> "$BENCHMARK_FILE"

    echo "  Total: ${TOTAL_SECONDS}s (Split: ${SPLIT_SECONDS}s, Map: ${MAPPER_SECONDS}s, Shuf: ${SHUFFLE_SECONDS}s, Red: ${REDUCER_SECONDS}s) | Throughput: ${THROUGHPUT} rows/s"
    rm -rf "$temp_dir"
}

# -------------------------------------------------------------
# Command-line dispatch
# -------------------------------------------------------------
if [ $# -ge 2 ]; then
    # Custom run: ./run_local.sh <matrix_a> <matrix_b> [tasks] [category_name] [expected]
    run_matrix "$1" "$2" "${3:-1}" "${4:-custom}" "${5:-}"
    exit 0
fi

echo "=========================================================="
echo "Running Local MapReduce Benchmark for EACH Matrix Category"
echo "=========================================================="
ensure_large_matrix

for CAT in "${CATEGORIES[@]}"; do
    CAT_DIR="$SCRIPT_DIR/tests/$CAT"
    A_FILE="$CAT_DIR/matrix_a.txt"
    B_FILE="$CAT_DIR/matrix_b.txt"
    EXP_FILE="$CAT_DIR/expected.txt"

    if [ ! -f "$A_FILE" ] || [ ! -f "$B_FILE" ]; then
        echo "Skipping $CAT: files not found in $CAT_DIR"
        continue
    fi

    # Process counts: for large matrix test 1 process; for others test 1, 2, 3
    if [ "$CAT" == "large-1000x1000" ]; then
        TASK_LIST=(1)
    else
        TASK_LIST=(1 2 3)
    fi

    for TASKS in "${TASK_LIST[@]}"; do
        run_matrix "$A_FILE" "$B_FILE" "$TASKS" "$CAT" "$EXP_FILE"
    done
done

echo ""
echo "=========================================================="
echo "All local matrix category benchmarks completed!"
echo "Results logged to: $BENCHMARK_FILE"
echo "=========================================================="
