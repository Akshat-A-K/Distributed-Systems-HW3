#!/usr/bin/env python3
"""plot_benchmarks.py -- generates E1/E2/E3 plots for HW3 Section 2 Q2 from
already-collected RCE benchmark CSVs. Purely a presentation-format addition
(the spec says "plots/tables" -- tables alone already satisfy it); no new
experiments are run here, and no measured value is altered.

Usage: python3 scripts/plot_benchmarks.py [--results-dir results/rce] [--out-dir results/plots]
"""

import argparse
import csv
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PARTITION_LABELS = {
    "station": "Station-Based Partitioning",
    "batch_rr": "Round-Robin Batch Partitioning",
}
PARTITION_STYLE = {
    "station": dict(marker="o", linestyle="-"),
    "batch_rr": dict(marker="s", linestyle="--"),
}


def load(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def mean(vals):
    vals = [float(v) for v in vals if v not in ("", None)]
    return sum(vals) / len(vals) if vals else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", default="results/rce")
    parser.add_argument("--out-dir", default="results/plots")
    args = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    # --- E1: throughput and CPU utilization vs. worker count ---
    rows = load(os.path.join(args.results_dir, "e1_workers.csv"))
    groups = defaultdict(list)
    for r in rows:
        groups[(int(r["workers"]), r["partition"])].append(r)
    workers = sorted({w for w, _ in groups})

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Experiment E1 -- Effect of Worker Count (N = 1,000,000 records, single node)", fontsize=12)
    for partition in ("station", "batch_rr"):
        style = PARTITION_STYLE[partition]
        label = PARTITION_LABELS[partition]
        thr = [mean([r["throughput_rec_per_s"] for r in groups[(w, partition)]]) for w in workers]
        cpu = [mean([(float(r["server_cpu_user_s"]) + float(r["server_cpu_sys_s"])) / float(r["server_elapsed_s"]) * 100
                     for r in groups[(w, partition)]]) for w in workers]
        ax1.plot(workers, thr, label=label, **style)
        ax2.plot(workers, cpu, label=label, **style)
    ax1.set_xlabel("Worker Count (W)")
    ax1.set_ylabel("Ingestion Throughput (records/s)")
    ax1.set_title("Throughput vs. Worker Count")
    ax1.set_xticks(workers)
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    ax2.axhline(100, color="gray", linestyle=":", label="1 CPU core (100%)")
    ax2.set_xlabel("Worker Count (W)")
    ax2.set_ylabel("Server CPU Utilization (%)")
    ax2.set_title("Server CPU Utilization vs. Worker Count")
    ax2.set_xticks(workers)
    ax2.set_ylim(0, max(150, ax2.get_ylim()[1]))
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(args.out_dir, "e1_workers.png"), dpi=150)
    plt.close(fig)

    # --- E2: throughput and query p95 latency vs. batch size (granularity) ---
    rows = load(os.path.join(args.results_dir, "e2_granularity.csv"))
    groups = defaultdict(list)
    for r in rows:
        groups[int(r["batch_size"])].append(r)
    batch_sizes = sorted(groups)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Experiment E2 -- Effect of Streaming Granularity (N = 100,000 records, W = 4, single node)", fontsize=12)
    thr = [mean([r["throughput_rec_per_s"] for r in groups[b]]) for b in batch_sizes]
    p95 = [mean([r["query_p95_ms"] for r in groups[b]]) for b in batch_sizes]
    ax1.plot(batch_sizes, thr, "o-", color="tab:green")
    ax1.set_xscale("log")
    ax1.set_xlabel("Batch Size (records per message, log scale)")
    ax1.set_ylabel("Ingestion Throughput (records/s)")
    ax1.set_title("Throughput vs. Batch Size")
    ax1.grid(True, which="both", alpha=0.3)
    ax2.plot(batch_sizes, p95, "s-", color="tab:red")
    ax2.set_xscale("log")
    ax2.set_xlabel("Batch Size (records per message, log scale)")
    ax2.set_ylabel("Query Latency, p95 (milliseconds)")
    ax2.set_title("Query Latency (p95) vs. Batch Size")
    ax2.grid(True, which="both", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(args.out_dir, "e2_granularity.png"), dpi=150)
    plt.close(fig)

    # --- E3: throughput and query latency percentiles vs. concurrent query clients ---
    rows = load(os.path.join(args.results_dir, "e3_concurrent_queries.csv"))
    groups = defaultdict(list)
    for r in rows:
        groups[int(r["query_clients"])].append(r)
    qs = sorted(groups)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Experiment E3 -- Effect of Concurrent Query Load (N = 1,000,000 records, W = 4, single node)", fontsize=12)
    thr = [mean([r["throughput_rec_per_s"] for r in groups[q]]) for q in qs]
    ax1.plot(qs, thr, "o-", color="tab:purple")
    ax1.set_xlabel("Concurrent Query Clients (Q)")
    ax1.set_ylabel("Ingestion Throughput (records/s)")
    ax1.set_title("Ingestion Throughput vs. Concurrent Query Clients")
    ax1.set_xticks(qs)
    ax1.grid(True, alpha=0.3)

    percentile_series = (
        ("p50 (median)", "query_p50_ms", dict(marker="o", linestyle="-")),
        ("p95", "query_p95_ms", dict(marker="s", linestyle="--")),
        ("p99", "query_p99_ms", dict(marker="^", linestyle=":")),
    )
    for label, field, style in percentile_series:
        vals = [mean([r[field] for r in groups[q]]) for q in qs if q != 0]
        ax2.plot([q for q in qs if q != 0], vals, label=label, **style)
    ax2.set_xlabel("Concurrent Query Clients (Q)")
    ax2.set_ylabel("Query Latency (milliseconds)")
    ax2.set_title("Query Latency Percentiles vs. Concurrent Query Clients")
    ax2.set_xticks([q for q in qs if q != 0])
    ax2.grid(True, alpha=0.3)
    ax2.legend(title="Percentile")
    fig.tight_layout()
    fig.savefig(os.path.join(args.out_dir, "e3_concurrent_queries.png"), dpi=150)
    plt.close(fig)

    print(f"Wrote {args.out_dir}/e1_workers.png")
    print(f"Wrote {args.out_dir}/e2_granularity.png")
    print(f"Wrote {args.out_dir}/e3_concurrent_queries.png")

    # --- Single-node vs. multi-node throughput comparison, if the
    # cross-node CSV is present (client and server on separate physical nodes) ---
    multinode_path = os.path.join(args.results_dir, "e1_workers_4node_99451.csv")
    if os.path.exists(multinode_path):
        single_rows = load(os.path.join(args.results_dir, "e1_workers.csv"))
        multi_rows = load(multinode_path)
        single_g = defaultdict(list)
        for r in single_rows:
            single_g[(int(r["workers"]), r["partition"])].append(r)
        multi_g = defaultdict(list)
        for r in multi_rows:
            multi_g[(int(r["workers"]), r["partition"])].append(r)
        workers = sorted({w for w, _ in single_g})

        fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
        fig.suptitle("Single-Node vs. Cross-Node Throughput (N = 1,000,000 records)", fontsize=12)
        for ax, partition in zip(axes, ("station", "batch_rr")):
            single_thr = [mean([r["throughput_rec_per_s"] for r in single_g[(w, partition)]]) for w in workers]
            multi_thr = [mean([r["throughput_rec_per_s"] for r in multi_g[(w, partition)]]) for w in workers]
            ax.plot(workers, single_thr, "o-", label="Single node (loopback)", color="tab:blue")
            ax.plot(workers, multi_thr, "s--", label="Cross node (server/client separated)", color="tab:red")
            ax.set_xlabel("Worker Count (W)")
            ax.set_title(PARTITION_LABELS[partition])
            ax.set_xticks(workers)
            ax.grid(True, alpha=0.3)
            ax.legend()
        axes[0].set_ylabel("Ingestion Throughput (records/s)")
        fig.tight_layout()
        fig.savefig(os.path.join(args.out_dir, "e1_single_vs_multinode.png"), dpi=150)
        plt.close(fig)
        print(f"Wrote {args.out_dir}/e1_single_vs_multinode.png")
    else:
        print(f"Skipped single-vs-multi-node plot: {multinode_path} not found")


if __name__ == "__main__":
    main()
