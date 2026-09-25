# Section 1: Distributed Matrix Multiplication (Row-Row Method)

This folder contains our implementation for Section 1, Question 1 of Distributed Systems Assignment 3.

## Overview
We compute matrix multiplication C = A x B using the MapReduce Row-Row method.
- Matrix A is partitioned row-wise across mapper processes.
- Matrix B is replicated to each mapper process via file argument.
- Each mapper reads assigned rows of A from standard input and multiplies with full matrix B to produce full rows of C.
- Intermediate outputs are sorted by row index (shuffle phase).
- Reducer takes sorted data and outputs the final matrix rows.

## File Structure
- `mapper.cpp`: C++ source code for mapper. Reads matrix B into memory, reads assigned rows of A from stdin, and outputs `row_index<TAB>row_values`.
- `reducer.cpp`: C++ source code for reducer. Reads sorted lines from stdin and outputs final matrix.
- `matrix_a.txt`: Input matrix A (size 3x2). First number on each line is row index.
- `matrix_b.txt`: Input matrix B (size 2x3). First line has dimensions `n p`.
- `run_local.ps1`: PowerShell script to run a single matrix locally on Windows.
- `run_local.sh`: Bash script to run the full benchmark suite across ALL matrix categories locally (Linux / Git Bash).
- `run_tests.ps1`: Test runner script for local testing with multiple matrix shapes.
- `run_rce.sh`: SLURM batch script for running on RCE cluster with srun.
- `tests/`: Directory containing test cases (square, tall, wide, single row, single col, 1000x1000).
- `results/`: Directory containing benchmark CSV files from RCE runs.
- `report.md`: Detailed analysis report with benchmark table, speedup, and bottleneck analysis.

## How to Run Locally (Windows PowerShell)

1. Compile the C++ code:
```powershell
g++ -O3 -o mapper.exe mapper.cpp
g++ -O3 -o reducer.exe reducer.cpp
```

2. Run with default matrices:
```powershell
.\run_local.ps1
```

3. Run with specific number of mapper processes:
```powershell
.\run_local.ps1 -MapperTasks 1
.\run_local.ps1 -MapperTasks 2
.\run_local.ps1 -MapperTasks 4
```

4. Run all test cases:
```powershell
.\run_tests.ps1 -SkipLarge
```

5. Run the full local benchmark suite across ALL matrix categories:
```bash
bash run_local.sh
```

## How to Run on RCE Cluster (SLURM)

1. Submit a single SLURM job:
```bash
sbatch run_rce.sh
```

2. Submit with specific nodes and processes:
```bash
sbatch --nodes=2 --ntasks=8 --ntasks-per-node=4 run_rce.sh
```

3. Submit the full benchmark grid for ALL matrix categories across 1, 2, 3 nodes:
```bash
bash run_rce.sh --submit-grid
```

Benchmark output will be saved into CSV files inside the `results/` folder.
