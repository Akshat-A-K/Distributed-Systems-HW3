#!/usr/bin/env bash

#SBATCH --job-name=hw3-matrix-mapreduce
#SBATCH --output=hw3_matrix_%j.out
#SBATCH --error=hw3_matrix_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=16
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00

set -euo pipefail

# ============================================================
# IMPORTANT:
# SLURM may start the batch job in:
# /var/spool/slurmd/job<JOB_ID>
#
# Therefore always use the directory containing this script.
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


if [[ "${1:-}" == "--submit-grid" ]]; then

    shift

    # Convert input paths to absolute paths BEFORE sbatch.
    if [[ -n "${1:-}" ]]; then
        A_INPUT="$1"
    else
        A_INPUT="$SCRIPT_DIR/matrix_a.txt"
    fi
    if [[ -n "${2:-}" ]]; then
        B_INPUT="$2"
    else
        B_INPUT="$SCRIPT_DIR/matrix_b.txt"
    fi
    if [[ "$A_INPUT" != /* ]]; then
        A_INPUT="$SCRIPT_DIR/$A_INPUT"
    fi

    if [[ "$B_INPUT" != /* ]]; then
        B_INPUT="$SCRIPT_DIR/$B_INPUT"
    fi

    A_INPUT="$(realpath "$A_INPUT")"
    B_INPUT="$(realpath "$B_INPUT")"

    # Check files BEFORE submitting jobs.
    if [[ ! -f "$A_INPUT" ]]; then
        echo "ERROR: Matrix A not found:"
        echo "$A_INPUT"
        exit 1
    fi
    if [[ ! -f "$B_INPUT" ]]; then
        echo "ERROR: Matrix B not found:"
        echo "$B_INPUT"
        exit 1
    fi
    echo "=========================================="
    echo "Submitting benchmark grid"
    echo "=========================================="
    echo "Matrix A: $A_INPUT"
    echo "Matrix B: $B_INPUT"
    echo ""

    for NODE_COUNT in 1 2 3; do
        for PROCESS_COUNT in 1 2 4 8 16; do
            # Cannot allocate more nodes than MPI/SLURM tasks.
            if (( PROCESS_COUNT < NODE_COUNT )); then
                continue
            fi
            echo "Submitting:"
            echo "  Nodes     = $NODE_COUNT"
            echo "  Processes = $PROCESS_COUNT"
            sbatch \
                --nodes="$NODE_COUNT" \
                --ntasks="$PROCESS_COUNT" \
                --ntasks-per-node=16 \
                --cpus-per-task=1 \
                --export=ALL,A_INPUT="$A_INPUT",B_INPUT="$B_INPUT" \
                "$0" --grid-job
        done
    done
    echo ""
    echo "All valid jobs submitted."
    exit 0
fi

if [[ "${1:-}" == "--grid-job" ]]; then

    A_FILE="${A_INPUT:-$SCRIPT_DIR/matrix_a.txt}"
    B_FILE="${B_INPUT:-$SCRIPT_DIR/matrix_b.txt}"
else
    A_FILE="${1:-$SCRIPT_DIR/matrix_a.txt}"
    B_FILE="${2:-$SCRIPT_DIR/matrix_b.txt}"
fi

if [[ "$A_FILE" != /* ]]; then
    A_FILE="$SCRIPT_DIR/$A_FILE"
fi

if [[ "$B_FILE" != /* ]]; then
    B_FILE="$SCRIPT_DIR/$B_FILE"
fi

# Convert to canonical absolute paths.
A_FILE="$(realpath "$A_FILE")"
B_FILE="$(realpath "$B_FILE")"

TASKS="${SLURM_NTASKS:-1}"
JOB_ID="${SLURM_JOB_ID:-manual}"

NODE_COUNT="${SLURM_JOB_NUM_NODES:-1}"
NODE_LIST="${SLURM_NODELIST:-unknown}"

WORK_DIR="$SCRIPT_DIR/rce_work_${JOB_ID}"
FINAL_OUTPUT="$SCRIPT_DIR/result_rce_${JOB_ID}.txt"

BENCHMARK_FILE="$SCRIPT_DIR/rce_benchmark_${JOB_ID}.csv"


echo "=========================================="
echo "SLURM_JOB_ID = $JOB_ID"
echo "SLURM_NODELIST = $NODE_LIST"
echo "SLURM_NTASKS = $TASKS"
echo "SLURM_JOB_NUM_NODES = $NODE_COUNT"
echo "=========================================="

echo "Script directory:"
echo "$SCRIPT_DIR"
echo ""

echo "Current directory:"
pwd
echo ""

echo "Matrix A:"
echo "$A_FILE"

echo "Matrix B:"
echo "$B_FILE"

echo ""
echo "Tasks:"
echo "$TASKS"
echo "Nodes:"
echo "$NODE_COUNT"


if [[ ! -f "$A_FILE" ]]; then

    echo ""
    echo "ERROR: Matrix A does not exist."
    echo "A_FILE = $A_FILE"
    echo ""
    echo "Contents of SCRIPT_DIR:"
    ls -lh "$SCRIPT_DIR"
    exit 1

fi
if [[ ! -f "$B_FILE" ]]; then
    echo ""
    echo "ERROR: Matrix B does not exist."
    echo "B_FILE = $B_FILE"

    echo ""
    echo "Contents of SCRIPT_DIR:"
    ls -lh "$SCRIPT_DIR"

    exit 1

fi

echo ""
echo "Input files found successfully."


N=$(awk 'NR == 1 { print $1 }' "$B_FILE")
P=$(awk 'NR == 1 { print $2 }' "$B_FILE")

M=$(wc -l < "$A_FILE" | tr -d ' ')

A_BYTES=$(stat -c%s "$A_FILE")
B_BYTES=$(stat -c%s "$B_FILE")

echo ""
echo "Matrix dimensions:"
echo "n = $N"
echo "m = $M"
echo "p = $P"

echo ""
echo "Input sizes:"
echo "Matrix A = $A_BYTES bytes"
echo "Matrix B = $B_BYTES bytes"


rm -rf "$WORK_DIR"

mkdir -p "$WORK_DIR"

echo ""
echo "=========================================="
echo "Compiling mapper"
echo "=========================================="

g++ \
    -std=c++17 \
    -O2 \
    "$SCRIPT_DIR/mapper.cpp" \
    -o "$WORK_DIR/mapper"


echo ""
echo "=========================================="
echo "Compiling reducer"
echo "=========================================="

g++ \
    -std=c++17 \
    -O2 \
    "$SCRIPT_DIR/reducer.cpp" \
    -o "$WORK_DIR/reducer"

TOTAL_START=$(date +%s%N)

echo ""
echo "=========================================="
echo "Splitting Matrix A"
echo "=========================================="

SPLIT_START=$(date +%s%N)

split \
    -d \
    -a 3 \
    -n l/"$TASKS" \
    "$A_FILE" \
    "$WORK_DIR/chunk_"
SPLIT_END=$(date +%s%N)


echo "Created chunks:"

ls -lh "$WORK_DIR"/chunk_*


echo ""
echo "=========================================="
echo "Starting Mapper"
echo "=========================================="
echo "Number of mapper processes: $TASKS"

MAPPER_START=$(date +%s%N)

srun \
    --ntasks="$TASKS" \
    bash -c '

        task_id=$(printf "%03d" "$SLURM_PROCID")

        mapper="$1"
        matrix_b="$2"
        work_dir="$3"

        echo "Mapper process $SLURM_PROCID"
        echo "Reading: $work_dir/chunk_${task_id}"

        "$mapper" \
            "$matrix_b" \
            < "$work_dir/chunk_${task_id}" \
            > "$work_dir/map_${task_id}.out"

    ' \
    bash \
    "$WORK_DIR/mapper" \
    "$B_FILE" \
    "$WORK_DIR"

MAPPER_END=$(date +%s%N)

echo ""
echo "Mapper outputs:"
ls -lh "$WORK_DIR"/map_*.out


echo ""
echo "=========================================="
echo "Starting Shuffle / Sort"
echo "=========================================="

SHUFFLE_START=$(date +%s%N)

cat "$WORK_DIR"/map_*.out \
    | sort \
    > "$WORK_DIR/global_sorted.out"

SHUFFLE_END=$(date +%s%N)

echo "Sorted intermediate file:"
ls -lh "$WORK_DIR/global_sorted.out"

echo ""
echo "=========================================="
echo "Starting Reducer"
echo "=========================================="

REDUCER_START=$(date +%s%N)

"$WORK_DIR/reducer" \
    < "$WORK_DIR/global_sorted.out" \
    > "$FINAL_OUTPUT"

REDUCER_END=$(date +%s%N)

TOTAL_END=$(date +%s%N)

seconds() {

    awk "BEGIN {
        printf \"%.6f\", ($2 - $1) / 1000000000
    }"

}

SPLIT_SECONDS=$(seconds "$SPLIT_START" "$SPLIT_END")

MAPPER_SECONDS=$(seconds "$MAPPER_START" "$MAPPER_END")

SHUFFLE_SECONDS=$(seconds "$SHUFFLE_START" "$SHUFFLE_END")

REDUCER_SECONDS=$(seconds "$REDUCER_START" "$REDUCER_END")

TOTAL_SECONDS=$(seconds "$TOTAL_START" "$TOTAL_END")

OUTPUT_BYTES=$(stat -c%s "$FINAL_OUTPUT")

THROUGHPUT=$(awk "BEGIN {
    if ($TOTAL_SECONDS > 0)
        printf \"%.6f\", $M / $TOTAL_SECONDS
    else
        print \"0\"
}")


HEADER="n,m,p,mapper_tasks,nodes,input_rows,input_bytes,matrix_b_bytes,split_time_s,mapper_time_s,shuffle_time_s,reducer_time_s,total_time_s,output_bytes,throughput_rows_per_s,input_file,job_id"

echo "$HEADER" > "$BENCHMARK_FILE"


echo "$N,$M,$P,$TASKS,$NODE_COUNT,$M,$A_BYTES,$B_BYTES,$SPLIT_SECONDS,$MAPPER_SECONDS,$SHUFFLE_SECONDS,$REDUCER_SECONDS,$TOTAL_SECONDS,$OUTPUT_BYTES,$THROUGHPUT,$A_FILE,$JOB_ID" \
    >> "$BENCHMARK_FILE"
echo ""
echo "=========================================="
echo "JOB COMPLETED"
echo "=========================================="
echo "Dimensions:"
echo "n = $N"
echo "m = $M"
echo "p = $P"

echo ""
echo "Mapper tasks:"
echo "$TASKS"
echo ""
echo "Nodes:"
echo "$NODE_COUNT"
echo ""
echo "Split time:"
echo "$SPLIT_SECONDS seconds"
echo ""
echo "Mapper time:"
echo "$MAPPER_SECONDS seconds"
echo ""
echo "Shuffle time:"
echo "$SHUFFLE_SECONDS seconds"
echo ""
echo "Reducer time:"
echo "$REDUCER_SECONDS seconds"
echo ""
echo "Total time:"
echo "$TOTAL_SECONDS seconds"
echo ""
echo "Throughput:"
echo "$THROUGHPUT rows/sec"
echo ""
echo "Final output:"
echo "$FINAL_OUTPUT"
echo ""
echo "Benchmark:"
echo "$BENCHMARK_FILE"
echo "=========================================="
echo ""
echo "=========================================="
echo "MATRIX RESULT"
echo "=========================================="
cat "$FINAL_OUTPUT"
echo ""
echo "Cleaning temporary files..."
rm -rf "$WORK_DIR"
echo "Cleanup complete."
echo ""
echo "Job finished successfully."