# Section 1 - Question 1 Report
**Topic:** Distributed Matrix Multiplication using MapReduce (Row-Row Method)

## 1. How I Implemented It
I implemented the Row-Row MapReduce matrix multiplication for this problem:
- **Mapper (`mapper.cpp`):** It takes the path of Matrix B as a command-line argument and loads the full Matrix B into memory. Then it reads assigned rows of Matrix A from standard input (stdin). For each row, it computes the dot products across all columns of Matrix B and outputs the row index and the calculated row vector separated by a tab.
- **Reducer (`reducer.cpp`):** It reads the sorted key-value output from standard input, groups the vectors belonging to the same row index, and prints the formatted matrix rows.
- **Execution Pipeline (`run_local.sh` and `run_rce.sh`):**
  - Matrix A is split across mapper tasks.
  - Mapper processes execute independently in parallel with the replicated Matrix B.
  - Intermediate outputs are grouped and sorted using Unix sort (`sort -k1,1n`).
  - Reducer combines the intermediate results into the final matrix.

## 2. Correctness Verification
I tested the implementation with 8 different matrix categories covering all edge cases and requirements given in the assignment:
- **even-square:** 4x4 times 4x4 (m divisible by mapper count) -> Output matched `expected.txt` exactly.
- **odd-rectangular:** 3x5 times 5x3 (m not divisible by mapper count) -> Output matched `expected.txt` exactly.
- **tall:** 5x2 times 2x2 ($m \gg n$) -> Output matched `expected.txt` exactly.
- **wide:** 2x5 times 5x2 ($n \gg m$) -> Output matched `expected.txt` exactly.
- **single-row:** 1x3 times 3x2 ($m=1$ edge case) -> Output matched `expected.txt` exactly.
- **single-column:** 4x1 times 1x3 ($n=1$ edge case) -> Output matched `expected.txt` exactly.
- **square:** 3x2 times 2x3 (gives 3x3 result from problem statement) -> Output matched `expected.txt` exactly.
- **large-1000x1000:** 1000x1000 identity matrices -> Verified output is 1000x1000 identity matrix.

All outputs were verified using `diff` against the expected mathematical results.

## 3. RCE Cluster Benchmark Results

### Default Square Matrix (3x2 and 2x3 -> 3x3 Output)
Here are the actual measured times from the RCE SLURM runs across nodes (1, 2, 3) and processes (1, 2, 4, 8):

| Nodes | Processes | Split Time (s) | Mapper Time (s) | Shuffle Time (s) | Reducer Time (s) | Total Time (s) | Speedup ($S_p$) | Efficiency ($E_p$) |
|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 0.0022 | 0.0649 | 0.0025 | 0.0030 | 0.0768 | 1.000x (Baseline) | 1.000 |
| 1 | 2 | 0.0030 | 0.1682 | 0.0029 | 0.0030 | 0.1815 | 0.423x | 0.212 |
| 1 | 4 | 0.0041 | 0.1562 | 0.0034 | 0.0033 | 0.1724 | 0.445x | 0.111 |
| 1 | 8 | 0.0053 | 0.1810 | 0.0050 | 0.0035 | 0.2019 | 0.380x | 0.048 |
| 2 | 2 | 0.0031 | 0.0530 | 0.0029 | 0.0032 | 0.0668 | 1.150x | 0.575 |
| 2 | 4 | 0.0041 | 0.1601 | 0.0035 | 0.0033 | 0.1764 | 0.435x | 0.109 |
| 2 | 8 | 0.0047 | 0.1626 | 0.0049 | 0.0033 | 0.1824 | 0.421x | 0.053 |
| 3 | 4 | 0.0045 | 0.1709 | 0.0046 | 0.0042 | 0.1917 | 0.401x | 0.100 |
| 3 | 8 | 0.0062 | 0.1673 | 0.0053 | 0.0038 | 0.1917 | 0.401x | 0.050 |

*Formulas used:*
- $\text{Speedup } S_p = \frac{T_1}{T_p}$ where $T_1 = 0.076802\text{s}$ (1 node, 1 process baseline).
- $\text{Efficiency } E_p = \frac{S_p}{p} = \frac{T_1}{p \times T_p}$.

### Large 1000x1000 Matrix on RCE Cluster
For the large 1000x1000 identity matrix, computation time is significant enough to show speedup:

| Nodes | Processes | Mapper Time (s) | Reducer Time (s) | Total Time (s) | Throughput (rows/s) |
|---|---|---|---|---|---|
| 1 | 1 | 0.9196 | 0.1560 | 1.1437 | 874.37 |
| 1 | 2 | 1.0404 | 0.1558 | 1.2662 | 789.78 |
| 1 | 4 | 0.6485 | 0.1518 | 0.8959 | 1116.22 |
| 1 | 8 | 0.4750 | 0.1589 | 0.7454 | 1341.65 |
| 2 | 2 | 0.5884 | 0.1545 | 0.8264 | 1210.08 |
| 2 | 4 | 0.7234 | 0.1535 | 0.9786 | 1021.86 |
| 2 | 8 | 0.6344 | 0.1544 | 0.8962 | 1115.78 |
| 3 | 4 | 0.6420 | 0.1509 | 0.8948 | 1117.62 |
| 3 | 8 | 0.4713 | 0.1636 | 0.7439 | 1344.22 |

As seen above, for the large 1000x1000 matrix, the mapper time drops from **0.9196s** (1 process) down to **0.4713s** (8 processes), giving higher throughput.

## 4. Main Observations and Bottleneck Discussion
1. **Process Launch Overhead for Small Matrices:** For small matrices (3x2, 4x4, etc.), the actual multiplication takes microseconds. The total time of ~0.07s to 0.20s is dominated by `srun`, shell subshell creation, and temporary file I/O. Therefore, increasing process counts from 1 to 8 on small inputs causes speedup to decrease below 1.
2. **True Scaling on Large Workloads:** When testing with 1000x1000 matrices, parallel mappers successfully distribute the arithmetic workload, reducing Mapper execution time by nearly 50% (from 0.92s to 0.47s) and increasing throughput from 874 rows/s to 1344 rows/s.
3. **Stage Breakdown:** In all runs, the Mapper stage is the most computationally intensive phase. Shuffle time using Unix `sort` is very small for compact outputs ($<0.01\text{s}$) and Reducer aggregation scales linearly with the number of generated output rows.
4. **Load Balancing:** When $m$ is divisible by mappers (e.g. 4 rows on 2 or 4 mappers), chunks are completely balanced. When $m$ is not divisible (e.g. 3 rows on 2 mappers), the workload differs by at most 1 row, which has negligible impact on runtime.
