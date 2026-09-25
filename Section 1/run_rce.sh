#!/usr/bin/env bash
#SBATCH --job-name=hw3-matrix-mapreduce
#SBATCH --output=hw3_matrix_%j.out
#SBATCH --error=hw3_matrix_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00
set -euo pipefail

# Dynamic project directory detection
if [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -d "$SLURM_SUBMIT_DIR/Section 1" ]; then
    PROJECT_DIR="$SLURM_SUBMIT_DIR/Section 1"
elif [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "$SLURM_SUBMIT_DIR/mapper.cpp" ]; then
    PROJECT_DIR="$SLURM_SUBMIT_DIR"
elif [ -f "$(dirname "${BASH_SOURCE[0]}")/mapper.cpp" ]; then
    PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
else
    PROJECT_DIR="/home/cs3401.08/Distributed-Systems-HW3/Section 1"
fi

cd "$PROJECT_DIR"
RESULTS_DIR="$PROJECT_DIR/results"
mkdir -p "$RESULTS_DIR"

# Matrix categories list in tests directory
CATEGORIES=(
    "square"
    "tall"
    "wide"
    "single-row"
    "single-column"
    "even-square"
    "odd-rectangular"
    "large-1000x1000"
)

# Helper function to generate large 1000x1000 identity matrix if missing
ensure_large_matrix() {
    local target_dir="$PROJECT_DIR/tests/large-1000x1000"
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

# -------------------------------------------------------------
# Mode 1: Submit Grid across all matrix categories or single matrix
# -------------------------------------------------------------
if [[ "${1:-}" == "--submit-grid" ]]; then
    shift
    ensure_large_matrix

    TARGET_CATS=()
    if [ $# -ge 2 ]; then
        # Specific matrix passed as argument: --submit-grid <matrix_a> <matrix_b> [category_name]
        CUSTOM_A="$(realpath "$1")"
        CUSTOM_B="$(realpath "$2")"
        CUSTOM_NAME="${3:-custom}"
        for NODE_COUNT in 1 2 3; do
            for PROCESS_COUNT in 1 2 4 8; do
                if (( PROCESS_COUNT < NODE_COUNT )); then continue; fi
                echo "Submitting SLURM job: category=$CUSTOM_NAME nodes=$NODE_COUNT procs=$PROCESS_COUNT"
                sbatch --nodes="$NODE_COUNT" --ntasks="$PROCESS_COUNT" --cpus-per-task=1 \
                    --export=ALL,A_INPUT="$CUSTOM_A",B_INPUT="$CUSTOM_B",CAT_NAME="$CUSTOM_NAME" \
                    "$PROJECT_DIR/run_rce.sh" --grid-job
            done
        done
        exit 0
    fi

    # Submit grid for EACH matrix category
    echo "=========================================================="
    echo "Submitting SLURM Benchmark Grid for EACH Matrix Category"
    echo "=========================================================="
    for CAT in "${CATEGORIES[@]}"; do
        CAT_DIR="$PROJECT_DIR/tests/$CAT"
        A_PATH="$CAT_DIR/matrix_a.txt"
        B_PATH="$CAT_DIR/matrix_b.txt"

        if [ ! -f "$A_PATH" ] || [ ! -f "$B_PATH" ]; then
            if [ "$CAT" == "square" ] && [ -f "$PROJECT_DIR/matrix_a.txt" ]; then
                A_PATH="$PROJECT_DIR/matrix_a.txt"
                B_PATH="$PROJECT_DIR/matrix_b.txt"
            else
                echo "Skipping $CAT: files not found in $CAT_DIR"
                continue
            fi
        fi

        echo ">>> Scheduling Grid for Category: $CAT"
        for NODE_COUNT in 1 2 3; do
            for PROCESS_COUNT in 1 2 4 8; do
                if (( PROCESS_COUNT < NODE_COUNT )); then continue; fi
                sbatch --nodes="$NODE_COUNT" --ntasks="$PROCESS_COUNT" --cpus-per-task=1 \
                    --export=ALL,A_INPUT="$A_PATH",B_INPUT="$B_PATH",CAT_NAME="$CAT" \
                    "$PROJECT_DIR/run_rce.sh" --grid-job
            done
        done
    done
    echo "All grid jobs submitted successfully."
    exit 0
fi

# -------------------------------------------------------------
# Function: Run Single Matrix Benchmark Pipeline
# -------------------------------------------------------------
run_single_matrix_benchmark() {
    local a_file="$1"
    local b_file="$2"
    local cat_name="$3"
    local tasks="$4"
    local node_count="$5"
    local job_id="$6"

    a_file="$(realpath "$a_file")"
    b_file="$(realpath "$b_file")"

    if [ ! -f "$a_file" ] || [ ! -f "$b_file" ]; then
        echo "ERROR: Input files not found: $a_file, $b_file"
        return 1
    fi

    local tasks_per_node=$(( (tasks + node_count - 1) / node_count ))
    local work_dir="$PROJECT_DIR/rce_work_${job_id}_${cat_name}"
    local final_output="$PROJECT_DIR/result_rce_${job_id}_${cat_name}.txt"
    local bench_file="$RESULTS_DIR/rce_benchmark_${job_id}.csv"
    local cat_bench_file="$RESULTS_DIR/rce_benchmark_${cat_name}_${job_id}.csv"

    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    echo "=========================================="
    echo "Running Matrix: $cat_name"
    echo "Matrix A: $a_file"
    echo "Matrix B: $b_file"
    echo "Tasks: $tasks | Nodes: $node_count | Job ID: $job_id"
    echo "=========================================="

    local N=$(awk 'NR == 1 { print $1 }' "$b_file")
    local P=$(awk 'NR == 1 { print $2 }' "$b_file")
    local M=$(wc -l < "$a_file" | tr -d ' ')
    local A_BYTES=$(stat -c%s "$a_file" 2>/dev/null || stat -f%z "$a_file")
    local B_BYTES=$(stat -c%s "$b_file" 2>/dev/null || stat -f%z "$b_file")

    # Compile mapper and reducer if not compiled in work_dir
    g++ -std=c++17 -O2 "$PROJECT_DIR/mapper.cpp" -o "$work_dir/mapper"
    g++ -std=c++17 -O2 "$PROJECT_DIR/reducer.cpp" -o "$work_dir/reducer"

    local TOTAL_START=$(date +%s%N)

    # 1. Split
    local SPLIT_START=$(date +%s%N)
    split -d -a 3 -n l/"$tasks" "$a_file" "$work_dir/chunk_"
    local SPLIT_END=$(date +%s%N)

    # Ensure all task chunks exist even if empty
    for (( t=0; t<tasks; t++ )); do
        local tid=$(printf "%03d" "$t")
        if [ ! -f "$work_dir/chunk_${tid}" ]; then
            touch "$work_dir/chunk_${tid}"
        fi
    done

    # 2. Map
    local MAPPER_START=$(date +%s%N)
    srun --nodes="$node_count" --ntasks="$tasks" --ntasks-per-node="$tasks_per_node" --cpus-per-task=1 bash -c '
        task_id=$(printf "%03d" "$SLURM_PROCID")
        mapper="$1"
        matrix_b="$2"
        work_dir="$3"
        "$mapper" "$matrix_b" < "$work_dir/chunk_${task_id}" > "$work_dir/map_${task_id}.out"
    ' bash "$work_dir/mapper" "$b_file" "$work_dir"
    local MAPPER_END=$(date +%s%N)

    # 3. Shuffle / Sort
    local SHUFFLE_START=$(date +%s%N)
    cat "$work_dir"/map_*.out | sort -k1,1n > "$work_dir/global_sorted.out"
    local SHUFFLE_END=$(date +%s%N)

    # 4. Reduce
    local REDUCER_START=$(date +%s%N)
    "$work_dir/reducer" < "$work_dir/global_sorted.out" > "$final_output"
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

    local HEADER="n,m,p,mapper_tasks,nodes,input_rows,input_bytes,matrix_b_bytes,split_time_s,mapper_time_s,shuffle_time_s,reducer_time_s,total_time_s,output_bytes,throughput_rows_per_s,input_file,job_id,category"
    local DATA_ROW="$N,$M,$P,$tasks,$node_count,$M,$A_BYTES,$B_BYTES,$SPLIT_SECONDS,$MAPPER_SECONDS,$SHUFFLE_SECONDS,$REDUCER_SECONDS,$TOTAL_SECONDS,$OUTPUT_BYTES,$THROUGHPUT,$a_file,$job_id,$cat_name"

    # Write benchmark file per category and job
    echo "$HEADER" > "$cat_bench_file"
    echo "$DATA_ROW" >> "$cat_bench_file"

    echo "Completed $cat_name: Total=${TOTAL_SECONDS}s, Map=${MAPPER_SECONDS}s, Shuf=${SHUFFLE_SECONDS}s, Red=${REDUCER_SECONDS}s, Throughput=${THROUGHPUT} rows/s"
    rm -rf "$work_dir"
}

# -------------------------------------------------------------
# Mode 2: Grid-job execution (called by SLURM for a single task config)
# -------------------------------------------------------------
TASKS="${SLURM_NTASKS:-1}"
JOB_ID="${SLURM_JOB_ID:-manual}"
NODE_COUNT="${SLURM_JOB_NUM_NODES:-1}"

if [[ "${1:-}" == "--grid-job" ]]; then
    A_FILE="${A_INPUT:-$PROJECT_DIR/matrix_a.txt}"
    B_FILE="${B_INPUT:-$PROJECT_DIR/matrix_b.txt}"
    CAT_NAME="${CAT_NAME:-square}"
    run_single_matrix_benchmark "$A_FILE" "$B_FILE" "$CAT_NAME" "$TASKS" "$NODE_COUNT" "$JOB_ID"
    exit 0
fi

# -------------------------------------------------------------
# Mode 3: Default execution - runs EACH matrix category in current allocation
# -------------------------------------------------------------
echo "=========================================================="
echo "Starting Execution: Running EACH Matrix Category on RCE"
echo "Allocated Nodes: $NODE_COUNT | Tasks: $TASKS | Job: $JOB_ID"
echo "=========================================================="

ensure_large_matrix

for CAT in "${CATEGORIES[@]}"; do
    CAT_DIR="$PROJECT_DIR/tests/$CAT"
    A_PATH="$CAT_DIR/matrix_a.txt"
    B_PATH="$CAT_DIR/matrix_b.txt"

    if [ ! -f "$A_PATH" ] || [ ! -f "$B_PATH" ]; then
        if [ "$CAT" == "square" ] && [ -f "$PROJECT_DIR/matrix_a.txt" ]; then
            A_PATH="$PROJECT_DIR/matrix_a.txt"
            B_PATH="$PROJECT_DIR/matrix_b.txt"
        else
            echo "Skipping $CAT: files not found in $CAT_DIR"
            continue
        fi
    fi

    run_single_matrix_benchmark "$A_PATH" "$B_PATH" "$CAT" "$TASKS" "$NODE_COUNT" "$JOB_ID"
done

echo ""
echo "=========================================================="
echo "All matrix category benchmarks completed on RCE!"
echo "Summary logged to: $RESULTS_DIR/rce_benchmark_${JOB_ID}.csv"
echo "=========================================================="