#!/usr/bin/env python3
"""bench.py -- local benchmark runner for HW3 Section 2 Q2.

Covers the spec's explicitly required measurements: effect of worker count
(E1), streaming/message granularity (E2), and concurrent queries (E3).
Starts a fresh server process per run (clean CPU baseline), streams the
dataset via stream_client.py --preload (isolates server-side numbers from
this process's own Python parsing cost), simulates concurrent query
clients as threads issuing GetAnalytics against their own channels, and
runs a correctness check against the C++ oracle every run. A run that
raises is recorded as FAILED in the CSV -- never a fabricated number (same
rule as Q1's benchmark.sh).

Usage:
    python3 scripts/bench.py --experiment E1 --dataset ../data/generated/gen_n1000000_k5_s100_seed42.txt --out ../results/e1.csv
    python3 scripts/bench.py --experiment E2 --dataset ../data/generated/gen_n100000_k5_s100_seed42.txt --out ../results/e2.csv
    python3 scripts/bench.py --experiment E3 --dataset ../data/generated/gen_n1000000_k5_s100_seed42.txt --out ../results/e3.csv --workers 4
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import threading
import time

import grpc

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
sys.path.insert(0, SRC_DIR)

import weather_stream_pb2 as pb2  # noqa: E402
import weather_stream_pb2_grpc as pb2_grpc  # noqa: E402


def venv_python() -> str:
    venv = os.path.join(PROJECT_ROOT, ".venv", "bin", "python3")
    return venv if os.path.exists(venv) else "python3"


def start_server(host, port, workers, partition, log_path):
    proc = subprocess.Popen(
        [venv_python(), "server.py", "--host", host, "--port", str(port),
         "--workers", str(workers), "--partition", partition],
        cwd=SRC_DIR, stdout=open(log_path, "w"), stderr=subprocess.STDOUT,
    )
    channel = grpc.insecure_channel(f"{host}:{port}")
    grpc.channel_ready_future(channel).result(timeout=15)
    channel.close()
    return proc


def stop_server(proc) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def run_client(host, port, dataset, batch_size, rate, preload) -> dict:
    cmd = [venv_python(), "stream_client.py", f"{host}:{port}", dataset,
           "--batch-size", str(batch_size), "--rate", str(rate), "--json", "--quiet"]
    if preload:
        cmd.append("--preload")
    out = subprocess.run(cmd, cwd=SRC_DIR, capture_output=True, text=True, check=True)
    return json.loads(out.stdout.strip().splitlines()[-1])


class QueryLoad:
    """Simulates `num_clients` concurrent query clients as threads, each with
    its own gRPC channel issuing GetAnalytics in a loop (closed loop if
    interval_ms==0, else paced), for the lifetime of the `with` block."""

    def __init__(self, host, port, num_clients, interval_ms):
        self.addr = f"{host}:{port}"
        self.num_clients = num_clients
        self.interval_ms = interval_ms
        self.latencies_ms = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._threads = []

    def _run(self):
        channel = grpc.insecure_channel(self.addr)
        stub = pb2_grpc.WeatherAnalyticsStub(channel)
        local = []
        while not self._stop_event.is_set():
            t0 = time.perf_counter()
            try:
                stub.GetAnalytics(pb2.AnalyticsRequest())
            except grpc.RpcError:
                pass
            else:
                local.append((time.perf_counter() - t0) * 1000.0)
            if self.interval_ms > 0:
                time.sleep(self.interval_ms / 1000.0)
        with self._lock:
            self.latencies_ms.extend(local)
        channel.close()

    def __enter__(self):
        for _ in range(self.num_clients):
            t = threading.Thread(target=self._run, daemon=True)
            t.start()
            self._threads.append(t)
        return self

    def __exit__(self, *exc):
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=5.0)


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * p
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


def check_correctness(host, port, dataset, oracle_binary) -> bool:
    actual = subprocess.run([venv_python(), "dashboard.py", f"{host}:{port}", "--once"],
                             cwd=SRC_DIR, capture_output=True, text=True)
    if actual.returncode != 0:
        return False
    expected = subprocess.run([oracle_binary, dataset], capture_output=True, text=True)
    return actual.stdout == expected.stdout


def run_one(host, port, dataset, workers, partition, batch_size, rate, preload,
            query_clients, query_interval_ms, oracle_binary, results_dir, tag) -> dict:
    log_path = os.path.join(results_dir, f"server_{tag}.log")
    base_row = {
        "tag": tag, "workers": workers, "partition": partition, "batch_size": batch_size,
        "rate": rate, "query_clients": query_clients, "query_interval_ms": query_interval_ms,
    }
    try:
        server = start_server(host, port, workers, partition, log_path)
    except Exception as exc:  # noqa: BLE001
        return dict(base_row, status="FAILED", error=f"server start: {exc}")

    try:
        query_load = QueryLoad(host, port, query_clients, query_interval_ms)
        with query_load:
            time.sleep(0.05)
            client_result = run_client(host, port, dataset, batch_size, rate, preload)
            time.sleep(0.05)
        correct = check_correctness(host, port, dataset, oracle_binary)
    except Exception as exc:  # noqa: BLE001
        stop_server(server)
        return dict(base_row, status="FAILED", error=str(exc))
    stop_server(server)

    row = dict(base_row)
    row.update(client_result)
    row.update({
        "query_p50_ms": percentile(query_load.latencies_ms, 0.50),
        "query_p95_ms": percentile(query_load.latencies_ms, 0.95),
        "query_p99_ms": percentile(query_load.latencies_ms, 0.99),
        "query_count": len(query_load.latencies_ms),
        "correctness": "PASS" if correct else "FAIL",
        "status": "OK",
    })
    return row


FIELDNAMES = [
    "tag", "status", "workers", "partition", "batch_size", "rate",
    "query_clients", "query_interval_ms",
    "n", "records_received", "client_end_to_end_s", "server_elapsed_s",
    "server_cpu_user_s", "server_cpu_sys_s", "throughput_rec_per_s",
    "query_p50_ms", "query_p95_ms", "query_p99_ms", "query_count",
    "correctness", "error",
]


def write_csv(rows, out_path) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True, choices=("E1", "E2", "E3"))
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    default_port = 56000 + int(os.environ.get("SLURM_JOB_ID", os.getpid())) % 5000
    parser.add_argument("--port", type=int, default=default_port,
                         help="Default is derived from $SLURM_JOB_ID (or this process's PID) "
                              "to avoid colliding with another user's job on a shared node.")
    parser.add_argument("--workers", type=int, default=4, help="Fixed worker count for E2/E3")
    parser.add_argument("--reps", type=int, default=1)
    args = parser.parse_args()

    oracle_binary = os.path.join(PROJECT_ROOT, "build", "weather_seq")
    results_dir = os.path.join(PROJECT_ROOT, "results", "raw")
    os.makedirs(results_dir, exist_ok=True)
    dataset = os.path.abspath(args.dataset)

    rows = []
    for rep in range(args.reps):
        if args.experiment == "E1":
            for workers in (1, 2, 4, 8):
                for partition in ("station", "batch_rr"):
                    tag = f"E1_W{workers}_{partition}_rep{rep}"
                    print(f"--- {tag} ---", file=sys.stderr)
                    rows.append(run_one(args.host, args.port, dataset, workers, partition,
                                         1000, 0, True, 0, 0, oracle_binary, results_dir, tag))
        elif args.experiment == "E2":
            for batch_size in (1, 10, 100, 1000, 10000):
                tag = f"E2_B{batch_size}_rep{rep}"
                print(f"--- {tag} ---", file=sys.stderr)
                rows.append(run_one(args.host, args.port, dataset, args.workers, "station",
                                     batch_size, 0, True, 1, 100, oracle_binary, results_dir, tag))
        elif args.experiment == "E3":
            for query_clients in (0, 1, 4, 16):
                tag = f"E3_Q{query_clients}_rep{rep}"
                print(f"--- {tag} ---", file=sys.stderr)
                rows.append(run_one(args.host, args.port, dataset, args.workers, "station",
                                     1000, 0, True, query_clients, 0, oracle_binary,
                                     results_dir, tag))

        for row in rows[-4:]:
            status = row.get("status")
            if status == "FAILED":
                print(f"  {row['tag']}: FAILED ({row.get('error')})", file=sys.stderr)
            else:
                print(f"  {row['tag']}: {row.get('throughput_rec_per_s', 0):.0f} rec/s, "
                      f"correctness={row.get('correctness')}", file=sys.stderr)

    write_csv(rows, args.out)
    print(f"Wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
