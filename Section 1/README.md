# Q1 Matrix Multiplication

This is the local C++ baseline for Section 1 Q1.

## Input format

Each line of `matrix_a.txt` contains:

```text
row_index value_1 value_2 ... value_n
```

The first line of `matrix_b.txt` contains `n p`. The following lines contain the `n x p` matrix values.

## Run on Windows

From PowerShell:

```powershell
.\run_local.ps1
```

Run with a selected number of local mapper processes:

```powershell
.\run_local.ps1 -MapperTasks 1
.\run_local.ps1 -MapperTasks 2
.\run_local.ps1 -MapperTasks 3
```

The runner splits rows of A between independent mapper processes, performs a global local sort, and sends the sorted data to one reducer. It prints the mapper count and elapsed time for performance analysis.

Each run appends one row to `local_benchmark.csv` with this schema:

```text
n,m,p,mapper_tasks,input_rows,input_bytes,matrix_b_bytes,mapper_time_s,shuffle_time_s,reducer_time_s,total_time_s,output_bytes,throughput_rows_per_s,input_file
```

Expected output:

```text
4 3 2
3 0 -3
2 -3 -8
```

## MapReduce behavior

- The mapper loads matrix B once from its local file.
- The mapper reads rows of A from standard input.
- The mapper emits `row_index<TAB>complete_result_row`.
- `Sort-Object` simulates the Hadoop shuffle and sort stage locally.
- The reducer writes result rows in row-index order.

The next step is to adapt the same mapper and reducer to Hadoop Streaming and distribute `matrix_b.txt` using Hadoop's `-files` option.

## Run all test cases

The test suite covers:

- 3 x 3 square matrices.
- Tall matrix: `5 x 2` multiplied by `2 x 2`.
- Wide matrix: `2 x 5` multiplied by `5 x 2`.
- Single row: `1 x 3` multiplied by `3 x 2`.
- Single column: `4 x 1` multiplied by `1 x 3`.
- Negative and zero values.
- Even square matrix: `4 x 4`.
- Odd rectangular matrix: `3 x 5` multiplied by `5 x 3`.
- Large generated matrix: `1000 x 1000`.

Run every case with:

```powershell
.\run_tests.ps1
```

The test runner executes every small case with 1, 2, and 3 mapper processes. This checks odd and even dimensions, square and rectangular matrices, single-row and single-column cases, and divisible/non-divisible row distributions.

The test runner includes the large identity case by default:

```powershell
.\run_tests.ps1
```

The 1000 x 1000 case is run with one mapper process and performs approximately one billion inner-loop operations. To run only the quick small cases, use:

```powershell
.\run_tests.ps1 -SkipLarge
```

## Run on the RCE cluster

The SLURM script uses three mapper tasks by default. It splits matrix A between tasks, gives every mapper the shared matrix B file, globally sorts mapper output, and runs the reducer.

Submit it from an RCE login node:

```bash
sbatch run_rce.sh
```

For a different input pair:

```bash
sbatch run_rce.sh matrix_a.txt matrix_b.txt
```

For all requested multi-node and multi-process measurements, submit the grid from an RCE login node:

```bash
bash run_rce.sh --submit-grid
```

This submits 15 jobs:

- Nodes: `1`, `2`, `3`
- Processes/tasks: `1`, `2`, `4`, `8`, `16`

To submit the grid for another input pair:

```bash
bash run_rce.sh --submit-grid matrix_a.txt matrix_b.txt
```

For one individual configuration, use the normal SLURM override:

```bash
sbatch --nodes=2 --ntasks=8 --ntasks-per-node=16 run_rce.sh
```

The RCE output is written to `result_rce_<job-id>.txt`. Timing data is appended to `rce_benchmark.csv` with dimensions, node/task count, input sizes, split time, mapper time, shuffle time, reducer time, total time, output size, and throughput. The script requires the mapper and reducer source files, `g++`, `srun`, and a shared filesystem visible from the allocated nodes.
