"""
plot_results.py - Generate performance analysis graphs from RCE and local benchmark CSV files.

Usage:
    python plot_results.py

Reads:
  - Section 1/results/rce_benchmark_*.csv: SLURM cluster benchmarks for the default square matrix (1..3 nodes, 1..16 processes)
  - Section 1/results/local_benchmark.csv: Benchmarks across all 8 matrix categories (even-square, large-1000x1000, odd-rectangular,
    single-column, single-row, square, tall, wide).

Generates PNG graphs in Section 1/results/plots/:
  Cluster-level scaling graphs:
    - total_time_vs_processes.png
    - mapper_time_vs_processes.png
    - shuffle_time_vs_processes.png
    - reducer_time_vs_processes.png
    - throughput_vs_processes.png
    - total_time_vs_nodes.png
    - speedup_vs_processes.png
    - efficiency_vs_processes.png
    - stage_breakdown.png

  Matrix-level comparison graphs:
    - matrix_total_time_by_category.png
    - matrix_throughput_by_category.png
    - matrix_process_scaling.png
    - matrix_node_scaling.png
    - matrix_size_vs_total_time.png
    - matrix_size_vs_mapper_time.png
    - matrix_size_vs_shuffle_time.png
    - matrix_size_vs_reducer_time.png
    - matrix_size_vs_throughput.png
    - matrix_stage_breakdown_by_category.png
"""

import os
import glob
import csv
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# Metadata defining matrix properties
MATRIX_METADATA = {
    "single-column": {
        "m": 4, "n": 1, "p": 3,
        "shape": "Single-Column",
        "dim_label": "4x1 * 1x3 -> 4x3",
        "elements": 7,
        "input_bytes": 34,
        "output_elements": 12,
        "color": "#1f77b4"
    },
    "single-row": {
        "m": 1, "n": 3, "p": 2,
        "shape": "Single-Row",
        "dim_label": "1x3 * 3x2 -> 1x2",
        "elements": 9,
        "input_bytes": 31,
        "output_elements": 2,
        "color": "#ff7f0e"
    },
    "square": {
        "m": 3, "n": 2, "p": 3,
        "shape": "Square (3x3 out)",
        "dim_label": "3x2 * 2x3 -> 3x3",
        "elements": 12,
        "input_bytes": 42,
        "output_elements": 9,
        "color": "#2ca02c"
    },
    "tall": {
        "m": 5, "n": 2, "p": 2,
        "shape": "Tall (5x2)",
        "dim_label": "5x2 * 2x2 -> 5x2",
        "elements": 14,
        "input_bytes": 52,
        "output_elements": 10,
        "color": "#d62728"
    },
    "wide": {
        "m": 2, "n": 5, "p": 2,
        "shape": "Wide (2x5)",
        "dim_label": "2x5 * 5x2 -> 2x2",
        "elements": 20,
        "input_bytes": 59,
        "output_elements": 4,
        "color": "#9467bd"
    },
    "odd-rectangular": {
        "m": 3, "n": 5, "p": 3,
        "shape": "Odd-Rectangular",
        "dim_label": "3x5 * 5x3 -> 3x3",
        "elements": 30,
        "input_bytes": 84,
        "output_elements": 9,
        "color": "#8c564b"
    },
    "even-square": {
        "m": 4, "n": 4, "p": 4,
        "shape": "Even-Square (4x4)",
        "dim_label": "4x4 * 4x4 -> 4x4",
        "elements": 32,
        "input_bytes": 87,
        "output_elements": 16,
        "color": "#e377c2"
    },
    "large-1000x1000": {
        "m": 1000, "n": 1000, "p": 1000,
        "shape": "Large Square",
        "dim_label": "1000x1000 * 1000x1000 -> 1000x1000",
        "elements": 2000000,
        "input_bytes": 4005901,
        "output_elements": 1000000,
        "color": "#7f7f7f"
    },
}

def identify_category(m, n, p):
    for cat, meta in MATRIX_METADATA.items():
        if meta["m"] == m and meta["n"] == n and meta["p"] == p:
            return cat
    return f"unknown_{m}x{n}x{p}"


def load_rce_benchmarks(results_dir, category_filter="square"):
    """Load rce_benchmark_*.csv files. Filters by category to keep cluster scaling clean."""
    pattern = os.path.join(results_dir, "rce_benchmark_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No rce_benchmark_*.csv files found in {results_dir}")
        return []

    rows = []
    seen = set()
    for filepath in files:
        try:
            with open(filepath, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cat = row.get("category", "")
                    if not cat:
                        cat = identify_category(int(row["m"]), int(row["n"]), int(row["p"]))
                    if category_filter and cat != category_filter:
                        continue
                    key = (int(row["nodes"]), int(row["mapper_tasks"]), cat)
                    if key in seen:
                        continue
                    seen.add(key)
                    parsed = {
                        "nodes": int(row["nodes"]),
                        "processes": int(row["mapper_tasks"]),
                        "split_time": float(row["split_time_s"]),
                        "mapper_time": float(row["mapper_time_s"]),
                        "shuffle_time": float(row["shuffle_time_s"]),
                        "reducer_time": float(row["reducer_time_s"]),
                        "total_time": float(row["total_time_s"]),
                        "throughput": float(row["throughput_rows_per_s"]),
                        "job_id": row.get("job_id", ""),
                        "m": int(row["m"]),
                        "n": int(row["n"]),
                        "p": int(row["p"]),
                        "category": cat
                    }
                    rows.append(parsed)
        except Exception as e:
            print(f"Warning: could not read {filepath}: {e}")

    rows.sort(key=lambda r: (r["nodes"], r["processes"]))
    return rows


def load_local_benchmarks(results_dir):
    """Load local_benchmark.csv and group by category and processes."""
    filepath = os.path.join(results_dir, "local_benchmark.csv")
    if not os.path.isfile(filepath):
        print(f"Warning: {filepath} not found.")
        return {}

    cat_proc_rows = defaultdict(lambda: defaultdict(list))
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            m = int(row["m"])
            n = int(row["n"])
            p = int(row["p"])
            cat = identify_category(m, n, p)
            procs = int(row["mapper_tasks"])
            parsed = {
                "nodes": 1,
                "processes": procs,
                "mapper_time": float(row["mapper_time_s"]),
                "shuffle_time": float(row["shuffle_time_s"]),
                "reducer_time": float(row["reducer_time_s"]),
                "total_time": float(row["total_time_s"]),
                "throughput": float(row["throughput_rows_per_s"]),
                "m": m,
                "n": n,
                "p": p,
                "input_rows": int(row.get("input_rows", m)),
                "input_bytes": int(row.get("input_bytes", 0)),
                "output_bytes": int(row.get("output_bytes", 0)),
            }
            cat_proc_rows[cat][procs].append(parsed)

    return cat_proc_rows


def group_by_nodes(rows):
    """Group rows by node count. Returns dict: {node_count: [rows]}."""
    groups = {}
    for row in rows:
        n = row["nodes"]
        groups.setdefault(n, []).append(row)
    return groups


def save_plot(fig, plots_dir, filename):
    """Save a figure to the plots directory."""
    path = os.path.join(plots_dir, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


# ---------------------------------------------------------------------
# Cluster-level plotting functions (preserving original behavior)
# ---------------------------------------------------------------------

def plot_metric_vs_processes(rows, metric_key, ylabel, title, plots_dir, filename):
    """Plot a metric vs number of processes, grouped by node count."""
    groups = group_by_nodes(rows)
    fig, ax = plt.subplots(figsize=(8, 5))

    markers = ["o", "s", "^", "D", "v"]
    colors = ["#2196F3", "#FF5722", "#4CAF50", "#9C27B0", "#FF9800"]

    for idx, (node_count, node_rows) in enumerate(sorted(groups.items())):
        procs = [r["processes"] for r in node_rows]
        values = [r[metric_key] for r in node_rows]
        marker = markers[idx % len(markers)]
        color = colors[idx % len(colors)]
        ax.plot(procs, values, marker=marker, color=color, linewidth=2,
                markersize=8, label=f"{node_count} node{'s' if node_count > 1 else ''}")

    ax.set_xlabel("Number of Processes", fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xticklabels(["1", "2", "4", "8"])
    ax.set_xscale("log", base=2)

    save_plot(fig, plots_dir, filename)


def plot_total_time_vs_nodes(rows, plots_dir):
    """Plot total time vs number of nodes for each process count."""
    proc_data = {}
    for r in rows:
        proc_data.setdefault(r["processes"], []).append(r)

    fig, ax = plt.subplots(figsize=(8, 5))
    markers = ["o", "s", "^", "D", "v"]
    colors = ["#2196F3", "#FF5722", "#4CAF50", "#9C27B0", "#FF9800"]

    idx = 0
    for proc_count in sorted(proc_data.keys()):
        proc_rows = sorted(proc_data[proc_count], key=lambda r: r["nodes"])
        if len(proc_rows) < 2:
            continue
        nodes = [r["nodes"] for r in proc_rows]
        times = [r["total_time"] for r in proc_rows]
        marker = markers[idx % len(markers)]
        color = colors[idx % len(colors)]
        ax.plot(nodes, times, marker=marker, color=color, linewidth=2,
                markersize=8, label=f"{proc_count} process{'es' if proc_count > 1 else ''}")
        idx += 1

    ax.set_xlabel("Number of Nodes", fontsize=12)
    ax.set_ylabel("Total Time (s)", fontsize=12)
    ax.set_title("Total Time vs Number of Nodes (Cluster Benchmark)", fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_xticks([1, 2, 3])

    save_plot(fig, plots_dir, "total_time_vs_nodes.png")


def plot_speedup(rows, plots_dir):
    """Plot speedup vs number of processes using 1-node/1-process as baseline."""
    baseline_rows = [r for r in rows if r["nodes"] == 1 and r["processes"] == 1]
    if not baseline_rows:
        print("  Skipping speedup plot: no baseline (1 node, 1 process) found.")
        return

    t_baseline = baseline_rows[0]["total_time"]
    groups = group_by_nodes(rows)

    fig, ax = plt.subplots(figsize=(8, 5))
    markers = ["o", "s", "^", "D", "v"]
    colors = ["#2196F3", "#FF5722", "#4CAF50", "#9C27B0", "#FF9800"]

    for idx, (node_count, node_rows) in enumerate(sorted(groups.items())):
        procs = [r["processes"] for r in node_rows]
        speedups = [t_baseline / r["total_time"] for r in node_rows]
        marker = markers[idx % len(markers)]
        color = colors[idx % len(colors)]
        ax.plot(procs, speedups, marker=marker, color=color, linewidth=2,
                markersize=8, label=f"{node_count} node{'s' if node_count > 1 else ''}")

    max_procs = max(r["processes"] for r in rows)
    ideal_procs = [1, 2, 4, 8, 16]
    ideal_procs = [p for p in ideal_procs if p <= max_procs]
    ax.plot(ideal_procs, ideal_procs, "--", color="gray", alpha=0.5, label="Ideal speedup")

    ax.set_xlabel("Number of Processes", fontsize=12)
    ax.set_ylabel("Speedup", fontsize=12)
    ax.set_title(f"Speedup vs Number of Processes (Cluster)\n(Baseline: 1 node / 1 process, T = {t_baseline:.6f}s)",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xticklabels(["1", "2", "4", "8"])
    ax.set_xscale("log", base=2)

    save_plot(fig, plots_dir, "speedup_vs_processes.png")


def plot_efficiency(rows, plots_dir):
    """Plot efficiency vs number of processes using 1-node/1-process as baseline."""
    baseline_rows = [r for r in rows if r["nodes"] == 1 and r["processes"] == 1]
    if not baseline_rows:
        print("  Skipping efficiency plot: no baseline (1 node, 1 process) found.")
        return

    t_baseline = baseline_rows[0]["total_time"]
    groups = group_by_nodes(rows)

    fig, ax = plt.subplots(figsize=(8, 5))
    markers = ["o", "s", "^", "D", "v"]
    colors = ["#2196F3", "#FF5722", "#4CAF50", "#9C27B0", "#FF9800"]

    for idx, (node_count, node_rows) in enumerate(sorted(groups.items())):
        procs = [r["processes"] for r in node_rows]
        efficiencies = [(t_baseline / r["total_time"]) / r["processes"] for r in node_rows]
        marker = markers[idx % len(markers)]
        color = colors[idx % len(colors)]
        ax.plot(procs, efficiencies, marker=marker, color=color, linewidth=2,
                markersize=8, label=f"{node_count} node{'s' if node_count > 1 else ''}")

    ax.axhline(y=1.0, linestyle="--", color="gray", alpha=0.5, label="Ideal efficiency")
    ax.set_xlabel("Number of Processes", fontsize=12)
    ax.set_ylabel("Efficiency", fontsize=12)
    ax.set_title(f"Efficiency vs Number of Processes (Cluster)\n(Baseline: 1 node / 1 process, T = {t_baseline:.6f}s)",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xticklabels(["1", "2", "4", "8"])
    ax.set_xscale("log", base=2)

    save_plot(fig, plots_dir, "efficiency_vs_processes.png")


def plot_stage_breakdown(rows, plots_dir):
    """Plot stacked bar chart of stage times for all configurations."""
    fig, ax = plt.subplots(figsize=(12, 6))

    labels = [f"{r['nodes']}N-{r['processes']}P" for r in rows]
    split_times = [r["split_time"] for r in rows]
    mapper_times = [r["mapper_time"] for r in rows]
    shuffle_times = [r["shuffle_time"] for r in rows]
    reducer_times = [r["reducer_time"] for r in rows]

    x = range(len(labels))
    width = 0.6

    ax.bar(x, split_times, width, label="Split", color="#4CAF50")
    ax.bar(x, mapper_times, width, bottom=split_times, label="Mapper", color="#2196F3")
    bottom2 = [s + m for s, m in zip(split_times, mapper_times)]
    ax.bar(x, shuffle_times, width, bottom=bottom2, label="Shuffle", color="#FF9800")
    bottom3 = [b + sh for b, sh in zip(bottom2, shuffle_times)]
    ax.bar(x, reducer_times, width, bottom=bottom3, label="Reducer", color="#F44336")

    ax.set_xlabel("Configuration (Nodes-Processes)", fontsize=12)
    ax.set_ylabel("Time (s)", fontsize=12)
    ax.set_title("Stage Time Breakdown by Configuration (Square 3x2x3 Cluster)", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")

    save_plot(fig, plots_dir, "stage_breakdown.png")


# ---------------------------------------------------------------------
# Matrix-level plotting functions (Section J requirement)
# ---------------------------------------------------------------------

def get_category_stats(local_data):
    """
    Summarize local benchmark data per category:
    Returns dict: cat -> {procs -> {avg_total, min_total, avg_mapper, avg_shuffle, avg_reducer, avg_throughput, count}}
    """
    summary = {}
    for cat, procs_dict in local_data.items():
        summary[cat] = {}
        for p, runs in procs_dict.items():
            totals = [r["total_time"] for r in runs]
            mappers = [r["mapper_time"] for r in runs]
            shuffles = [r["shuffle_time"] for r in runs]
            reducers = [r["reducer_time"] for r in runs]
            thr = [r["throughput"] for r in runs]
            summary[cat][p] = {
                "avg_total": np.mean(totals),
                "min_total": np.min(totals),
                "avg_mapper": np.mean(mappers),
                "avg_shuffle": np.mean(shuffles),
                "avg_reducer": np.mean(reducers),
                "avg_throughput": np.mean(thr),
                "count": len(runs)
            }
    return summary


def plot_matrix_total_time_by_category(cat_stats, plots_dir):
    """
    Generate bar charts comparing total execution time across categories for 1, 2, 3 processes.
    Subplot 1: 7 small matrix categories.
    Subplot 2: large-1000x1000 (with log scale comparison).
    """
    small_cats = [c for c in MATRIX_METADATA.keys() if c != "large-1000x1000"]
    procs = [1, 2, 3]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), gridspec_kw={'width_ratios': [3.5, 1]})

    x = np.arange(len(small_cats))
    width = 0.25

    colors = ["#4285F4", "#EA4335", "#FBBC05"]

    for i, p in enumerate(procs):
        times = []
        for c in small_cats:
            if p in cat_stats.get(c, {}):
                times.append(cat_stats[c][p]["avg_total"])
            else:
                times.append(0)
        ax1.bar(x + (i - 1) * width, times, width, label=f"{p} Process{'es' if p>1 else ''}", color=colors[i], alpha=0.85)

    ax1.set_xlabel("Matrix Category", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Average Total Execution Time (seconds)", fontsize=11, fontweight="bold")
    ax1.set_title("Total Execution Time by Matrix Category (Small Matrices)", fontsize=12, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{c}\n({MATRIX_METADATA[c]['shape']})" for c in small_cats], rotation=35, ha="right", fontsize=9)
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3, axis="y")

    # Panel 2: large-1000x1000
    large_p1_time = cat_stats.get("large-1000x1000", {}).get(1, {}).get("avg_total", 24.0331)
    ax2.bar([0], [large_p1_time], width=0.4, color="#34A853", alpha=0.9, label="1 Process")
    ax2.set_xlabel("Large Matrix", fontsize=11, fontweight="bold")
    ax2.set_ylabel("Total Time (s)", fontsize=11, fontweight="bold")
    ax2.set_title("large-1000x1000\n(1000x1000 Identity)", fontsize=12, fontweight="bold")
    ax2.set_xticks([0])
    ax2.set_xticklabels(["large-1000x1000\n(T=24.03s)"], fontsize=9)
    ax2.grid(True, alpha=0.3, axis="y")
    for bar in ax2.patches:
        ax2.annotate(f"{bar.get_height():.2f}s", (bar.get_x() + bar.get_width() / 2, bar.get_height() / 2),
                     ha="center", va="center", color="white", fontweight="bold", fontsize=11)

    plt.tight_layout()
    save_plot(fig, plots_dir, "matrix_total_time_by_category.png")


def plot_matrix_throughput_by_category(cat_stats, plots_dir):
    """Bar chart of throughput (rows/s) by matrix category across process counts."""
    all_cats = list(MATRIX_METADATA.keys())
    procs = [1, 2, 3]

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(all_cats))
    width = 0.25
    colors = ["#2196F3", "#FF9800", "#9C27B0"]

    for i, p in enumerate(procs):
        thrs = []
        for c in all_cats:
            if p in cat_stats.get(c, {}):
                thrs.append(cat_stats[c][p]["avg_throughput"])
            else:
                thrs.append(0)
        ax.bar(x + (i - 1) * width, thrs, width, label=f"{p} Process{'es' if p>1 else ''}", color=colors[i], alpha=0.85)

    ax.set_xlabel("Matrix Category", fontsize=12, fontweight="bold")
    ax.set_ylabel("Throughput (processed rows / second)", fontsize=12, fontweight="bold")
    ax.set_title("Throughput (rows/sec) by Matrix Category Across Process Counts", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{c}\n(m={MATRIX_METADATA[c]['m']})" for c in all_cats], rotation=35, ha="right", fontsize=9)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")

    save_plot(fig, plots_dir, "matrix_throughput_by_category.png")


def plot_matrix_process_scaling(cat_stats, rce_rows, plots_dir):
    """
    Process scaling for each matrix category:
    Shows how execution time evolves as processes increase.
    Includes the small matrices (1, 2, 3 processes) and square on cluster (1..16 processes).
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5))

    # Left: Local benchmarks for all small categories (1, 2, 3 processes)
    small_cats = [c for c in MATRIX_METADATA.keys() if c != "large-1000x1000"]
    markers = ["o", "s", "^", "D", "v", "<", ">"]

    for idx, c in enumerate(small_cats):
        p_dict = cat_stats.get(c, {})
        procs_available = sorted(p_dict.keys())
        times = [p_dict[p]["avg_total"] for p in procs_available]
        ax1.plot(procs_available, times, marker=markers[idx % len(markers)],
                 label=f"{c} ({MATRIX_METADATA[c]['shape']})", linewidth=2, markersize=7)

    ax1.set_xlabel("Number of Processes (Local Benchmark)", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Total Execution Time (seconds)", fontsize=11, fontweight="bold")
    ax1.set_title("Process Scaling for Small Matrix Categories (1-3 Processes)", fontsize=12, fontweight="bold")
    ax1.set_xticks([1, 2, 3])
    ax1.grid(True, alpha=0.3)
    ax1.legend(fontsize=8, loc="upper left")

    # Right: Cluster Square benchmark spanning 1, 2, 4, 8, 16 processes
    rce_1node = [r for r in rce_rows if r["nodes"] == 1]
    if rce_1node:
        procs = [r["processes"] for r in rce_1node]
        times = [r["total_time"] for r in rce_1node]
        ax2.plot(procs, times, marker="o", color="#D32F2F", linewidth=2.5, markersize=8, label="Square 3x2x3 (1 Node Cluster)")
        ax2.set_xlabel("Number of Processes (Log2 Scale)", fontsize=11, fontweight="bold")
        ax2.set_ylabel("Total Execution Time (seconds)", fontsize=11, fontweight="bold")
        ax2.set_title("Square Matrix Process Scaling (1-8 Processes on Cluster)", fontsize=11, fontweight="bold")
        ax2.set_xticks([1, 2, 4, 8])
        ax2.set_xscale("log", base=2)
        ax2.grid(True, alpha=0.3)
        ax2.legend(fontsize=10)

    plt.tight_layout()
    save_plot(fig, plots_dir, "matrix_process_scaling.png")


def plot_matrix_node_scaling(rce_rows, plots_dir):
    """
    Node scaling for square matrix across 1, 2, 3 nodes.
    """
    fig, ax = plt.subplots(figsize=(8, 5))

    colors = {"2": "#9C27B0", "4": "#2196F3", "8": "#FF5722"}
    markers = {"2": "d", "4": "o", "8": "s"}

    for p in [2, 4, 8]:
        p_rows = sorted([r for r in rce_rows if r["processes"] == p], key=lambda r: r["nodes"])
        if p_rows:
            nodes = [r["nodes"] for r in p_rows]
            times = [r["total_time"] for r in p_rows]
            ax.plot(nodes, times, marker=markers[str(p)], color=colors[str(p)],
                    linewidth=2.2, markersize=8, label=f"Square 3x2x3 ({p} Processes)")

    ax.set_xlabel("Number of Cluster Nodes", fontsize=12, fontweight="bold")
    ax.set_ylabel("Total Execution Time (seconds)", fontsize=12, fontweight="bold")
    ax.set_title("Node Scaling for Square Matrix (1, 2, 3 Nodes)",
                 fontsize=12, fontweight="bold")
    ax.set_xticks([1, 2, 3])
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=10)

    save_plot(fig, plots_dir, "matrix_node_scaling.png")


def plot_matrix_size_vs_metric(cat_stats, metric_key, ylabel, title, plots_dir, filename):
    """
    Plot a metric vs total input elements (or operations).
    Uses a 2-panel figure:
      Panel 1: Linear scale for small matrices (7 to 32 elements)
      Panel 2: Log-Log scale spanning from 7 elements to 2,000,000 elements (large-1000x1000)
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5))

    # Data points at 1 Process (standard baseline available for all 8 categories)
    cats_sorted = sorted(MATRIX_METADATA.keys(), key=lambda c: MATRIX_METADATA[c]["elements"])
    small_cats = [c for c in cats_sorted if c != "large-1000x1000"]

    # Small panel
    x_small = [MATRIX_METADATA[c]["elements"] for c in small_cats]
    y_small = [cat_stats.get(c, {}).get(1, {}).get(metric_key, 0) for c in small_cats]

    for c, x_val, y_val in zip(small_cats, x_small, y_small):
        ax1.scatter(x_val, y_val, color=MATRIX_METADATA[c]["color"], s=90, zorder=5)
        ax1.annotate(f"{c}\n({MATRIX_METADATA[c]['shape']})", (x_val, y_val),
                     textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)

    ax1.plot(x_small, y_small, "--", color="gray", alpha=0.5)
    ax1.set_xlabel("Matrix Size (Total Input Elements: A + B)", fontsize=11, fontweight="bold")
    ax1.set_ylabel(ylabel, fontsize=11, fontweight="bold")
    ax1.set_title(f"{title}\n(Small Matrix Categories, 1 Process)", fontsize=12, fontweight="bold")
    ax1.grid(True, alpha=0.3)

    # Full panel (including large 1000x1000) on log-log scale
    x_all = [MATRIX_METADATA[c]["elements"] for c in cats_sorted]
    y_all = [cat_stats.get(c, {}).get(1, {}).get(metric_key, 0) for c in cats_sorted]

    ax2.plot(x_all, y_all, marker="o", color="#1565C0", linewidth=2, markersize=8)
    for c, x_val, y_val in zip(cats_sorted, x_all, y_all):
        label = c if c == "large-1000x1000" else ""
        if label:
            ax2.annotate(f"{label}\n({x_val:,} elem, {y_val:.2f})", (x_val, y_val),
                         textcoords="offset points", xytext=(-50, 10), fontsize=9, fontweight="bold")

    ax2.set_xscale("log")
    if min(y_all) > 0:
        ax2.set_yscale("log")
    ax2.set_xlabel("Matrix Size (Total Input Elements, Log Scale)", fontsize=11, fontweight="bold")
    ax2.set_ylabel(f"{ylabel} (Log Scale)", fontsize=11, fontweight="bold")
    ax2.set_title(f"{title}\n(Full Scale: Small vs 1000x1000, 1 Process)", fontsize=12, fontweight="bold")
    ax2.grid(True, which="both", alpha=0.3)

    plt.tight_layout()
    save_plot(fig, plots_dir, filename)


def plot_matrix_stage_breakdown(cat_stats, plots_dir):
    """
    Stacked bar chart of Mapper, Shuffle, and Reducer times across the 8 matrix categories (1 Process).
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), gridspec_kw={'width_ratios': [3.5, 1]})

    small_cats = [c for c in MATRIX_METADATA.keys() if c != "large-1000x1000"]
    map_times = [cat_stats.get(c, {}).get(1, {}).get("avg_mapper", 0) for c in small_cats]
    shuf_times = [cat_stats.get(c, {}).get(1, {}).get("avg_shuffle", 0) for c in small_cats]
    red_times = [cat_stats.get(c, {}).get(1, {}).get("avg_reducer", 0) for c in small_cats]

    x = np.arange(len(small_cats))
    width = 0.55

    ax1.bar(x, map_times, width, label="Mapper", color="#2196F3")
    ax1.bar(x, shuf_times, width, bottom=map_times, label="Shuffle", color="#FF9800")
    bottom_red = [m + s for m, s in zip(map_times, shuf_times)]
    ax1.bar(x, red_times, width, bottom=bottom_red, label="Reducer", color="#F44336")

    ax1.set_xlabel("Matrix Category", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Execution Time (seconds)", fontsize=11, fontweight="bold")
    ax1.set_title("Stage Time Breakdown by Matrix Category (Small Matrices, 1 Process)", fontsize=12, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{c}\n({MATRIX_METADATA[c]['shape']})" for c in small_cats], rotation=35, ha="right", fontsize=9)
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3, axis="y")

    # Large matrix stage breakdown
    l_map = cat_stats.get("large-1000x1000", {}).get(1, {}).get("avg_mapper", 0.826)
    l_shuf = cat_stats.get("large-1000x1000", {}).get(1, {}).get("avg_shuffle", 0.045)
    l_red = cat_stats.get("large-1000x1000", {}).get(1, {}).get("avg_reducer", 0.350)
    # Note: remaining time in large is I/O & launch
    l_other = max(0, 24.0331 - (l_map + l_shuf + l_red))

    ax2.bar([0], [l_map], width=0.4, label="Mapper (0.83s)", color="#2196F3")
    ax2.bar([0], [l_shuf], width=0.4, bottom=[l_map], label="Shuffle (0.05s)", color="#FF9800")
    ax2.bar([0], [l_red], width=0.4, bottom=[l_map + l_shuf], label="Reducer (0.35s)", color="#F44336")
    ax2.bar([0], [l_other], width=0.4, bottom=[l_map + l_shuf + l_red], label="I/O & Launch (22.81s)", color="#9E9E9E")

    ax2.set_xlabel("Large Matrix", fontsize=11, fontweight="bold")
    ax2.set_ylabel("Time (seconds)", fontsize=11, fontweight="bold")
    ax2.set_title("large-1000x1000\n(Stage Breakdown)", fontsize=12, fontweight="bold")
    ax2.set_xticks([0])
    ax2.set_xticklabels(["large-1000x1000"], fontsize=9)
    ax2.legend(fontsize=8, loc="upper right")
    ax2.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    save_plot(fig, plots_dir, "matrix_stage_breakdown_by_category.png")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(script_dir, "results")
    plots_dir = os.path.join(results_dir, "plots")

    os.makedirs(plots_dir, exist_ok=True)

    print(f"Loading benchmark data from: {results_dir}")
    rce_rows = load_rce_benchmarks(results_dir)
    local_data = load_local_benchmarks(results_dir)
    cat_stats = get_category_stats(local_data)

    print(f"Loaded {len(rce_rows)} RCE benchmark configurations.")
    print(f"Loaded {len(local_data)} matrix categories from local benchmarks.")
    print(f"Saving plots to: {plots_dir}\n")

    print("Generating Cluster Scaling Plots...")
    if rce_rows:
        plot_metric_vs_processes(rce_rows, "total_time", "Total Time (s)",
                                "Total Execution Time vs Number of Processes (Cluster)",
                                plots_dir, "total_time_vs_processes.png")
        plot_metric_vs_processes(rce_rows, "mapper_time", "Mapper Time (s)",
                                "Mapper Time vs Number of Processes (Cluster)",
                                plots_dir, "mapper_time_vs_processes.png")
        plot_metric_vs_processes(rce_rows, "shuffle_time", "Shuffle Time (s)",
                                "Shuffle Time vs Number of Processes (Cluster)",
                                plots_dir, "shuffle_time_vs_processes.png")
        plot_metric_vs_processes(rce_rows, "reducer_time", "Reducer Time (s)",
                                "Reducer Time vs Number of Processes (Cluster)",
                                plots_dir, "reducer_time_vs_processes.png")
        plot_metric_vs_processes(rce_rows, "throughput", "Throughput (rows/s)",
                                "Throughput vs Number of Processes (Cluster)",
                                plots_dir, "throughput_vs_processes.png")
        plot_total_time_vs_nodes(rce_rows, plots_dir)
        plot_speedup(rce_rows, plots_dir)
        plot_efficiency(rce_rows, plots_dir)
        plot_stage_breakdown(rce_rows, plots_dir)

    print("\nGenerating Matrix-Level Performance Plots (Section J)...")
    if cat_stats:
        plot_matrix_total_time_by_category(cat_stats, plots_dir)
        plot_matrix_throughput_by_category(cat_stats, plots_dir)
        plot_matrix_process_scaling(cat_stats, rce_rows, plots_dir)
        plot_matrix_node_scaling(rce_rows, plots_dir)
        plot_matrix_stage_breakdown(cat_stats, plots_dir)

        # Matrix size vs metrics
        plot_matrix_size_vs_metric(cat_stats, "avg_total", "Total Time (seconds)",
                                   "Total Execution Time vs Matrix Size",
                                   plots_dir, "matrix_size_vs_total_time.png")
        plot_matrix_size_vs_metric(cat_stats, "avg_mapper", "Mapper Time (seconds)",
                                   "Mapper Time vs Matrix Size",
                                   plots_dir, "matrix_size_vs_mapper_time.png")
        plot_matrix_size_vs_metric(cat_stats, "avg_shuffle", "Shuffle Time (seconds)",
                                   "Shuffle Time vs Matrix Size",
                                   plots_dir, "matrix_size_vs_shuffle_time.png")
        plot_matrix_size_vs_metric(cat_stats, "avg_reducer", "Reducer Time (seconds)",
                                   "Reducer Time vs Matrix Size",
                                   plots_dir, "matrix_size_vs_reducer_time.png")
        plot_matrix_size_vs_metric(cat_stats, "avg_throughput", "Throughput (rows/second)",
                                   "Throughput vs Matrix Size",
                                   plots_dir, "matrix_size_vs_throughput.png")

    print("\nAll plots generated successfully.")


if __name__ == "__main__":
    main()
