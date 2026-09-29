#!/bin/bash
#SBATCH --job-name=hw3-q2-bench-4node
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=2G
#SBATCH --time=00:30:00
#SBATCH --output=results/bench_4node_%j.log
#SBATCH --error=results/bench_4node_%j.err
#SBATCH --partition=debug
#
# Genuine multi-node A/B against submit_bench.sh's single-node E1 numbers.
#
# NOT required by the assignment -- Q2's spec never mentions node count, only
# "number of workers" (a Performance Evaluation requirement already covered
# by E1's single-node sweep). The 3-node demo pattern in
# rce_grpc_execution_guide.pdf is for Section 3's problems, not Section 2.
# This run exists to remove ambiguity by measuring it directly rather than
# assuming: does real inter-node network latency (vs. single-node loopback)
# change the picture for this system?
#
# What this does and does NOT test: our "workers" are threads inside ONE
# coordinator process (a deliberate, documented design choice -- see
# README's Design decisions) -- they cannot be spread across nodes without a
# different architecture (separate worker services), which this project did
# not build. What DOES vary here is genuine physical separation of
# server/client roles: node 0 runs the coordinator (with its usual in-process
# W worker threads), node 1 runs the streaming client connecting to node 0
# over the real cluster network (not loopback) -- reusing server.py and
# stream_client.py completely unchanged, no code differs from the
# single-node run except which physical machine each process lands on.
#
# Submit from the project root (Q2_gRPC/):
#   sbatch scripts/submit_bench_multinode.sh
#   squeue -u $USER
#   cat results/bench_4node_<jobid>.log
#   cat results/e1_workers_4node_<jobid>.csv

set -uo pipefail

if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
    cd "$SLURM_SUBMIT_DIR"
else
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
fi

echo "=== Environment setup ==="
source scripts/setup_python_env.sh

bash scripts/build_reference.sh
if [[ ! -f data/generated/gen_n1000000_k5_s100_seed42.txt ]]; then
    bash scripts/make_datasets.sh
fi
if [[ ! -f src/weather_stream_pb2.py ]]; then
    .venv/bin/python3 -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/weather_stream.proto
fi

NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
SERVER_NODE="${NODES[0]}"
CLIENT_NODE="${NODES[1]}"
echo "Allocated nodes: ${NODES[*]}"
echo "Server node: $SERVER_NODE   Client node: $CLIENT_NODE"

PORT=$(( 57000 + SLURM_JOB_ID % 3000 ))
DATASET="data/generated/gen_n1000000_k5_s100_seed42.txt"
ORACLE="build/weather_seq"
CSV_FILE="results/e1_workers_4node_${SLURM_JOB_ID}.csv"
echo "tag,workers,partition,server_node,client_node,records_received,client_end_to_end_s,server_elapsed_s,throughput_rec_per_s,correctness" > "$CSV_FILE"

for workers in 1 2 4 8; do
    for partition in station batch_rr; do
        tag="4node_W${workers}_${partition}"
        echo ""
        echo "=== $tag: server on $SERVER_NODE, client on $CLIENT_NODE ==="

        SERVER_LOG="results/server_${tag}_${SLURM_JOB_ID}.log"
        srun --nodes=1 --ntasks=1 -w "$SERVER_NODE" \
            .venv/bin/python3 src/server.py --host 0.0.0.0 --port "$PORT" \
            --workers "$workers" --partition "$partition" > "$SERVER_LOG" 2>&1 &
        SERVER_SRUN_PID=$!

        echo "Waiting for server at $SERVER_NODE:$PORT ..."
        READY=0
        for _ in $(seq 1 50); do
            if .venv/bin/python3 - "$SERVER_NODE:$PORT" <<'EOF' 2>/dev/null
import sys, grpc
grpc.channel_ready_future(grpc.insecure_channel(sys.argv[1])).result(timeout=0.3)
EOF
            then
                READY=1
                break
            fi
            sleep 0.3
        done
        if [[ "$READY" -ne 1 ]]; then
            echo "$tag: FAILED -- server never became ready, see $SERVER_LOG" >&2
            echo "$tag,$workers,$partition,$SERVER_NODE,$CLIENT_NODE,,,,,FAIL" >> "$CSV_FILE"
            kill "$SERVER_SRUN_PID" 2>/dev/null
            wait "$SERVER_SRUN_PID" 2>/dev/null
            continue
        fi

        CLIENT_OUT="results/client_${tag}_${SLURM_JOB_ID}.json"
        srun --nodes=1 --ntasks=1 -w "$CLIENT_NODE" \
            .venv/bin/python3 src/stream_client.py "$SERVER_NODE:$PORT" "$DATASET" \
            --batch-size 1000 --preload --json --quiet > "$CLIENT_OUT" 2>"results/client_${tag}_${SLURM_JOB_ID}.err"
        CLIENT_STATUS=$?

        ACTUAL="results/actual_${tag}_${SLURM_JOB_ID}.txt"
        srun --nodes=1 --ntasks=1 -w "$CLIENT_NODE" \
            .venv/bin/python3 src/dashboard.py "$SERVER_NODE:$PORT" --once > "$ACTUAL" 2>/dev/null

        # Stop the server (this srun's own process, running in the background above).
        kill "$SERVER_SRUN_PID" 2>/dev/null
        wait "$SERVER_SRUN_PID" 2>/dev/null

        if [[ "$CLIENT_STATUS" -ne 0 ]]; then
            echo "$tag: FAILED -- stream_client error, see results/client_${tag}_${SLURM_JOB_ID}.err" >&2
            echo "$tag,$workers,$partition,$SERVER_NODE,$CLIENT_NODE,,,,,FAIL" >> "$CSV_FILE"
            continue
        fi

        EXPECTED="results/expected_n1000000.txt"
        [[ -f "$EXPECTED" ]] || "$ORACLE" "$DATASET" > "$EXPECTED"
        if diff -q "$EXPECTED" "$ACTUAL" > /dev/null 2>&1; then
            CORRECTNESS="PASS"
        else
            CORRECTNESS="FAIL"
        fi

        .venv/bin/python3 - "$CLIENT_OUT" "$tag" "$workers" "$partition" "$SERVER_NODE" "$CLIENT_NODE" "$CORRECTNESS" "$CSV_FILE" <<'EOF'
import csv, json, sys
out = json.load(open(sys.argv[1]))
tag, workers, partition, server_node, client_node, correctness, csv_file = sys.argv[2:9]
with open(csv_file, "a", newline="") as f:
    csv.writer(f).writerow([
        tag, workers, partition, server_node, client_node,
        out["records_received"], out["client_end_to_end_s"], out["server_elapsed_s"],
        out["throughput_rec_per_s"], correctness,
    ])
print(f"{tag}: {out['throughput_rec_per_s']:.0f} rec/s, correctness={correctness}")
EOF
    done
done

echo ""
echo "Done. See $CSV_FILE"
