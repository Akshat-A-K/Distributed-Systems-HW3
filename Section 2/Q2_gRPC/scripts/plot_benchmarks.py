#!/usr/bin/env python3
"""plot_benchmarks.py -- generates E1/E2/E3 plots for HW3 Section 2 Q2 from
already-collected RCE benchmark CSVs. Purely a presentation-format addition
(the spec says "plots/tables" -- tables alone already satisfy it); no new
experiments are run here.

Usage: python3 scripts/plot_benchmarks.py [--results-dir results/rce] [--out-dir results/plots]
"""

import argparse
import csv
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


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

    # --- E1: throughput and CPU util vs worker count ---
    rows = load(os.path.join(args.results_dir, "e1_workers.csv"))
    groups = defaultdict(list)
    for r in rows:
        groups[(int(r["workers"]), r["partition"])].append(r)
    workers = sorted({w for w, _ in groups})

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    for partition, marker in (("station", "o-"), ("batch_rr", "s--")):
        thr = [mean([r["throughput_rec_per_s"] for r in groups[(w, partition)]]) for w in workers]
        cpu = [mean([(float(r["server_cpu_user_s"]) + float(r["server_cpu_sys_s"])) / float(r["server_elapsed_s"]) * 100
                     for r in groups[(w, partition)]]) for w in workers]
        ax1.plot(workers, thr, marker, label=partition)
        ax2.plot(workers, cpu, marker, label=partition)
    ax1.set_xlabel("Worker count (W)")
    ax1.set_ylabel("Throughput (rec/s)")
    ax1.set_title("E1: throughput vs. worker count (N=1,000,000)")
    ax1.set_xticks(workers)
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    ax2.axhline(100, color="gray", linestyle=":", label="1 core")
    ax2.set_xlabel("Worker count (W)")
    ax2.set_ylabel("Server CPU utilization (%)")
    ax2.set_title("E1: CPU pinned near 1 core regardless of W (the GIL)")
    ax2.set_xticks(workers)
    ax2.set_ylim(0, max(150, ax2.get_ylim()[1]))
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(args.out_dir, "e1_workers.png"), dpi=150)
    plt.close(fig)

    # --- E2: throughput and query p95 vs batch size ---
    rows = load(os.path.join(args.results_dir, "e2_granularity.csv"))
    groups = defaultdict(list)
    for r in rows:
        groups[int(r["batch_size"])].append(r)
    batch_sizes = sorted(groups)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    thr = [mean([r["throughput_rec_per_s"] for r in groups[b]]) for b in batch_sizes]
    p95 = [mean([r["query_p95_ms"] for r in groups[b]]) for b in batch_sizes]
    ax1.plot(batch_sizes, thr, "o-", color="tab:green")
    ax1.set_xscale("log")
    ax1.set_xlabel("Batch size (B, log scale)")
    ax1.set_ylabel("Throughput (rec/s)")
    ax1.set_title("E2: throughput vs. streaming granularity (N=100,000)")
    ax1.grid(True, alpha=0.3)
    ax2.plot(batch_sizes, p95, "s-", color="tab:red")
    ax2.set_xscale("log")
    ax2.set_xlabel("Batch size (B, log scale)")
    ax2.set_ylabel("Query p95 latency (ms)")
    ax2.set_title("E2: query latency rises sharply at large B")
    ax2.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(args.out_dir, "e2_granularity.png"), dpi=150)
    plt.close(fig)

    # --- E3: throughput and query latency percentiles vs concurrent query clients ---
    rows = load(os.path.join(args.results_dir, "e3_concurrent_queries.csv"))
    groups = defaultdict(list)
    for r in rows:
        groups[int(r["query_clients"])].append(r)
    qs = sorted(groups)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    thr = [mean([r["throughput_rec_per_s"] for r in groups[q]]) for q in qs]
    ax1.plot(qs, thr, "o-", color="tab:purple")
    ax1.set_xlabel("Concurrent query clients (Q)")
    ax1.set_ylabel("Ingestion throughput (rec/s)")
    ax1.set_title("E3: ingestion throughput drops under query load (N=1,000,000)")
    ax1.set_xticks(qs)
    ax1.grid(True, alpha=0.3)

    for label, field, marker in (("p50", "query_p50_ms", "o-"), ("p95", "query_p95_ms", "s--"),
                                  ("p99", "query_p99_ms", "^:")):
        vals = [mean([r[field] for r in groups[q]]) for q in qs if q != 0]
        ax2.plot([q for q in qs if q != 0], vals, marker, label=label)
    ax2.set_xlabel("Concurrent query clients (Q)")
    ax2.set_ylabel("Query latency (ms)")
    ax2.set_title("E3: query latency percentiles rise with contention")
    ax2.set_xticks([q for q in qs if q != 0])
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(args.out_dir, "e3_concurrent_queries.png"), dpi=150)
    plt.close(fig)

    print(f"Wrote {args.out_dir}/e1_workers.png")
    print(f"Wrote {args.out_dir}/e2_granularity.png")
    print(f"Wrote {args.out_dir}/e3_concurrent_queries.png")

    # --- Single-node vs. multi-node (TA-confirmed: client/server on different
    # nodes) comparison, if the 4-node CSV is present ---
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

        fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)
        for ax, partition in zip(axes, ("station", "batch_rr")):
            single_thr = [mean([r["throughput_rec_per_s"] for r in single_g[(w, partition)]]) for w in workers]
            multi_thr = [mean([r["throughput_rec_per_s"] for r in multi_g[(w, partition)]]) for w in workers]
            ax.plot(workers, single_thr, "o-", label="single-node (loopback)", color="tab:blue")
            ax.plot(workers, multi_thr, "s--", label="cross-node (server/client separated)", color="tab:red")
            ax.set_xlabel("Worker count (W)")
            ax.set_title(f"partition={partition}")
            ax.set_xticks(workers)
            ax.grid(True, alpha=0.3)
            ax.legend()
        axes[0].set_ylabel("Throughput (rec/s)")
        fig.suptitle("Single-node vs. cross-node throughput (N=1,000,000) -- TA-confirmed multi-node requirement")
        fig.tight_layout()
        fig.savefig(os.path.join(args.out_dir, "e1_single_vs_multinode.png"), dpi=150)
        plt.close(fig)
        print(f"Wrote {args.out_dir}/e1_single_vs_multinode.png")
    else:
        print(f"Skipped single-vs-multi-node plot: {multinode_path} not found")


if __name__ == "__main__":
    main()
