#!/usr/bin/env python3
"""plot_comparison.py -- generates comparison plots for HW3 Section 2 Q1 from
already-collected benchmark data. Purely a presentation-format addition (the
PDF says "tables and/or plots" -- tables alone already satisfy the
requirement); no new experiments are run, no new numbers are invented.

Data sources:
  - This project's own results/rce_benchmark_98781.csv (job 98781, node06,
    x86_64, single node).
  - HW2's real MPI benchmark (analysis_detail.csv from
    github.com/Akshat-A-K/Distriubuted-Systems-HW2, HW2/Q8/results/),
    hardcoded below verbatim from that file -- not re-derived or estimated.

Usage: python3 scripts/plot_comparison.py [--out-dir results/plots]
"""

import argparse
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Verbatim from HW2/Q8/results/analysis_detail.csv (github.com/Akshat-A-K/Distriubuted-Systems-HW2).
# (records, processes) -> wall_seconds
HW2_MPI = {
    (10000, 1): 0.173228974, (10000, 2): 0.184251491, (10000, 4): 0.251855097, (10000, 8): 0.36520802,
    (100000, 1): 0.295348706, (100000, 2): 0.299558421, (100000, 4): 0.380797395, (100000, 8): 0.532929503,
    (1000000, 1): 1.537481115, (1000000, 2): 1.534062993, (1000000, 4): 1.599460408, (1000000, 8): 1.739035784,
}
HW2_SEQUENTIAL = {10000: 0.016759853, 100000: 0.131702438, 1000000: 1.273702632}


def load_q1(path):
    data = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            n = int(row["n"])
            m = int(row["mapper_chunks"])
            sec = row["seconds"]
            if sec not in ("FAILED", ""):
                data[(n, m)] = float(sec)
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="results/rce_benchmark_98781.csv")
    parser.add_argument("--out-dir", default="results/plots")
    args = parser.parse_args()

    q1 = load_q1(args.csv)
    os.makedirs(args.out_dir, exist_ok=True)
    task_counts = [1, 2, 4, 8]
    sizes = [10000, 100000, 1000000]

    # --- Plot 1: wall time vs task count, one panel per size ---
    fig, axes = plt.subplots(1, len(sizes), figsize=(15, 4.5), sharey=False)
    fig.suptitle("HW2 MPI vs. this project's Slurm-substitute MapReduce (single node, RCE)")
    for ax, n in zip(axes, sizes):
        mpi_y = [HW2_MPI[(n, p)] for p in task_counts]
        q1_y = [q1[(n, m)] for m in task_counts if (n, m) in q1]
        ax.plot(task_counts, mpi_y, "o-", label="HW2 MPI", color="tab:blue")
        if q1_y:
            ax.plot(task_counts, q1_y, "s--", label="Q1 Slurm-substitute", color="tab:orange")
        ax.axhline(HW2_SEQUENTIAL[n], color="gray", linestyle=":", label="HW2 sequential")
        ax.set_title(f"{n:,} records")
        ax.set_xlabel("P (MPI ranks) / M (mapper chunks)")
        ax.set_xticks(task_counts)
        ax.set_yscale("log")
        ax.grid(True, which="both", alpha=0.3)
        if ax is axes[0]:
            ax.set_ylabel("Wall time (s, log scale)")
        ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(args.out_dir, "runtime_comparison.png"), dpi=150)
    plt.close(fig)

    # --- Plot 2: each system's own task-count scaling ratio (M or P vs 1) ---
    fig, ax = plt.subplots(figsize=(7, 5))
    for n in sizes:
        mpi_ratio = [HW2_MPI[(n, p)] / HW2_MPI[(n, 1)] for p in task_counts]
        ax.plot(task_counts, mpi_ratio, "o-", label=f"MPI, {n:,} rec")
    ax.set_prop_cycle(None)
    for n in sizes:
        if all((n, m) in q1 for m in task_counts):
            q1_ratio = [q1[(n, m)] / q1[(n, 1)] for m in task_counts]
            ax.plot(task_counts, q1_ratio, "s--", label=f"Q1 substitute, {n:,} rec")
    ax.axhline(1.0, color="gray", linestyle=":")
    ax.set_xlabel("P / M")
    ax.set_ylabel("Wall time relative to P=1 / M=1")
    ax.set_xticks(task_counts)
    ax.set_title("Task-count overhead: neither system speeds up (ratio > 1 = slower)")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=7, ncol=2)
    fig.tight_layout()
    fig.savefig(os.path.join(args.out_dir, "task_count_overhead.png"), dpi=150)
    plt.close(fig)

    print(f"Wrote {args.out_dir}/runtime_comparison.png")
    print(f"Wrote {args.out_dir}/task_count_overhead.png")


if __name__ == "__main__":
    main()
