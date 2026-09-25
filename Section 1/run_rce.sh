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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [[ "${1:-}" == "--submit-grid" ]]; then

    shift

    A_INPUT="${1:-$SCRIPT_DIR/matrix_a.txt}"
    B_INPUT="${2:-$SCRIPT_DIR/matrix_b.txt}"

    A_INPUT="$(realpath "$A_INPUT")"
    B_INPUT="$(realpath "$B_INPUT")"

    echo "Matrix A: $A_INPUT"
    echo "Matrix B: $B_INPUT"

    for NODE_COUNT in 1 2 3; do

        for PROCESS_COUNT in 1 2 4 8 16; do

            # Cannot use fewer processes than nodes.
            if (( PROCESS_COUNT < NODE_COUNT )); then
                continue
            fi

            echo "Submitting: nodes=$NODE_COUNT processes=$PROCESS_COUNT"

            sbatch \
                --nodes="$NODE_COUNT" \
                --ntasks="$PROCESS_COUNT" \
                --ntasks-per-node=16 \
                --cpus-per-task=1 \
                --export=ALL,A_INPUT="$A_INPUT",B_INPUT="$B_INPUT" \
                "$0" --grid-job

        done

    done

    exit 0
fi

if [[ "${1:-}" == "--grid-job" ]]; then

    A_FILE="${A_INPUT:-$SCRIPT_DIR/matrix_a.txt}"
    B_FILE="${B_INPUT:-$SCRIPT_DIR/matrix_b.txt}"

else

    A_FILE="${1:-$SCRIPT_DIR/matrix_a.txt}"
    B_FILE="${2:-$SCRIPT_DIR/matrix_b.txt}"

fi

# Always use absolute paths.
A_FILE="$(realpath "$A_FILE")"
B_FILE="$(realpath "$B_FILE")"

TASKS="${SLURM_NTASKS:-1}"
JOB_ID="${SLURM_JOB_ID:-manual}"

WORK_DIR="$SCRIPT_DIR/rce_work_${JOB_ID}"
FINAL_OUTPUT="$SCRIPT_DIR/result_rce_${JOB_ID}.txt"

BENCHMARK_FILE="$SCRIPT_DIR/rce_benchmark_${JOB_ID}.csv"


echo "=========================================="
echo "SLURM_JOB_ID = ${SLURM_JOB_ID:-manual}"
echo "SLURM_NODELIST = ${SLURM_NODELIST:-unknown}"
echo "SLURM_NTASKS = ${SLURM_NTASKS:-unknown}"
echo "SLURM_JOB_NUM_NODES = ${SLURM_JOB_NUM_NODES:-unknown}"
echo "=========================================="

echo "Current directory:"
pwd

echo "Matrix A:"
echo "$A_FILE"

echo "Matrix B:"
echo "$B_FILE"


if [[ ! -f "$A_FILE" || ! -f "$B_FILE" ]]; then

    echo "ERROR: Input matrix files not found."

    echo "A_FILE = $A_FILE"
    echo "B_FILE = $B_FILE"

    echo "Current directory:"
    pwd

    echo "Files in current directory:"
    ls -lh

    exit 1

fi

echo "Absolute A_FILE = $A_FILE"
echo "Absolute B_FILE = $B_FILE"


N=$(awk 'NR == 1 { print $1 }' "$B_FILE")
P=$(awk 'NR == 1 { print $2 }' "$B_FILE")

M=$(wc -l < "$A_FILE" | tr -d ' ')

A_BYTES=$(stat -c%s "$A_FILE")
B_BYTES=$(stat -c%s "$B_FILE")
=======================================================

mkdir -p "$WORK_DIR"

echo "Compiling mapper..."

g++ -std=c++17 -O2 \
    "$SCRIPT_DIR/mapper.cpp" \
    -o "$WORK_DIR/mapper"


echo "Compiling reducer..."

g++ -std=c++17 -O2 \
    "$SCRIPT_DIR/reducer.cpp" \
    -o "$WORK_DIR/reducer"


TOTAL_START=$(date +%s%N)

echo "Starting split..."

SPLIT_START=$(date +%s%N)

split \
    -d \
    -a 3 \
    -n l/"$TASKS" \
    "$A_FILE" \
    "$WORK_DIR/chunk_"

SPLIT_END=$(date +%s%N)


echo "Starting mapper with $TASKS tasks..."

MAPPER_START=$(date +%s%N)

srun --ntasks="$TASKS" bash -c '

    task_id=$(printf "%03d" "$SLURM_PROCID")

    mapper="$1"
    matrix_b="$2"
    work_dir="$3"

    "$mapper" "$matrix_b" \
        < "$work_dir/chunk_${task_id}" \
        > "$work_dir/map_${task_id}.out"

' bash "$WORK_DIR/mapper" "$B_FILE" "$WORK_DIR"

MAPPER_END=$(date +%s%N)


echo "Starting shuffle/sort..."

SHUFFLE_START=$(date +%s%N)

cat "$WORK_DIR"/map_*.out | sort \
    > "$WORK_DIR/global_sorted.out"

SHUFFLE_END=$(date +%s%N)



echo "Starting reducer..."

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
    printf \"%.6f\", $M / $TOTAL_SECONDS
}")


HEADER="n,m,p,mapper_tasks,nodes,input_rows,input_bytes,matrix_b_bytes,split_time_s,mapper_time_s,shuffle_time_s,reducer_time_s,total_time_s,output_bytes,throughput_rows_per_s,input_file,job_id"

echo "$HEADER" > "$BENCHMARK_FILE"

echo "$N,$M,$P,$TASKS,${SLURM_JOB_NUM_NODES:-1},$M,$A_BYTES,$B_BYTES,$SPLIT_SECONDS,$MAPPER_SECONDS,$SHUFFLE_SECONDS,$REDUCER_SECONDS,$TOTAL_SECONDS,$OUTPUT_BYTES,$THROUGHPUT,$A_FILE,$JOB_ID" \
    >> "$BENCHMARK_FILE"


echo "=========================================="

echo "Dimensions: n=$N, m=$M, p=$P"

echo "Mapper tasks: $TASKS"

echo "Nodes: ${SLURM_JOB_NUM_NODES:-1}"

echo "Split seconds: $SPLIT_SECONDS"

echo "Mapper seconds: $MAPPER_SECONDS"

echo "Shuffle seconds: $SHUFFLE_SECONDS"

echo "Reducer seconds: $REDUCER_SECONDS"

echo "Total seconds: $TOTAL_SECONDS"

echo "Throughput rows/sec: $THROUGHPUT"

echo "Final output: $FINAL_OUTPUT"

echo "Benchmark: $BENCHMARK_FILE"

echo "=========================================="

cat "$FINAL_OUTPUT"

rm -rf "$WORK_DIR"