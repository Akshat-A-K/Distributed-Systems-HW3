# Section 1 - Question 1 Report: Distributed Matrix Multiplication

## 1. Implementation Overview
We implemented Distributed Matrix Multiplication (Row-Row method) using MapReduce:
- **`mapper.cpp`**: Replicates Matrix B in memory, processes rows of Matrix A, and outputs dot products.
- **`reducer.cpp`**: Aggregates output by row index and formats the final matrix.
- **Runners**: `run_rce.sh` handles SLURM cluster scheduling (`split`, `srun`, `sort`), while `run_local.sh` and `run_local.ps1` handle local parallel execution.

## 2. Correctness Verification
- Local test suite (`run_tests.ps1`) verified 8 matrix topologies (square, tall, wide, etc.) across 1, 2, and 3 mappers against exact mathematical outputs.
- RCE cluster runs deterministically yield expected byte counts and outputs for all topologies.

## 3. RCE Cluster Benchmark Results (Square 3x2x3 Matrix)
| Nodes | Processes | Mapper (s) | Shuffle (s) | Reducer (s) | Total (s) | Speedup | Efficiency |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | 1 | 0.0474 | 0.0025 | 0.0031 | 0.0676 | 1.000 | 1.000 |
| 1 | 2 | 0.1479 | 0.0027 | 0.0029 | 0.1685 | 0.401 | 0.201 |
| 1 | 4 | 0.1635 | 0.0032 | 0.0032 | 0.1874 | 0.361 | 0.090 |
| 1 | 16 | 0.1973 | 0.0066 | 0.0029 | 0.2338 | 0.289 | 0.018 |
| 2 | 4 | 0.1722 | 0.0034 | 0.0032 | 0.1963 | 0.345 | 0.086 |
| 3 | 16 | 0.1978 | 0.0075 | 0.0043 | 0.2380 | 0.284 | 0.018 |

*(Selected rows shown for brevity. Speedup degrades due to process launch overhead dominating the microsecond compute time for small matrices).*

## 4. Matrix-Level Performance Summary
| Category | Dimensions ($A, B \rightarrow C$) | Best Config | Total Time (s) | Throughput (rows/s) |
|---|---|---|:---:|:---:|
| **even-square** | $4 \times 4 \times 4 \rightarrow 4 \times 4$ | 1P | ~0.078 | 50.75 |
| **large-1000x1000**| $1000 \times 1000 \times 1000 \rightarrow 1000 \times 1000$| 1P | ~24.03 | 41.61 |
| **odd-rectangular**| $3 \times 5 \times 3 \rightarrow 3 \times 3$ | 2P | ~0.097 | 30.65 |
| **single-column** | $4 \times 1 \times 3 \rightarrow 4 \times 3$ | 1P | ~0.081 | 49.06 |
| **single-row** | $1 \times 3 \times 2 \rightarrow 1 \times 2$ | 1P | ~0.092 | 10.78 |
| **square** | $3 \times 2 \times 3 \rightarrow 3 \times 3$ | 1P | ~0.095 | 31.56 |
| **tall** | $5 \times 2 \times 2 \rightarrow 5 \times 2$ | 1P | ~0.079 | 63.05 |
| **wide** | $2 \times 5 \times 2 \rightarrow 2 \times 2$ | 3P | ~0.072 | 27.63 |

## 5. Key Analysis & Observations
1. **Matrix Size**: Compute time scales proportionally to elements (e.g., $1000 \times 1000$ takes 24s vs 0.1s for $4 \times 4$), but small matrices are completely dominated by process launch overhead (~100ms).
2. **Matrix Shape**: "Tall" matrices ($m > n,p$) provide more rows to partition, allowing better parallelism and throughput. "Wide" and "single-row" matrices limit parallelism because workers exceed available rows.
3. **Stage Workloads**: 
   - *Mapper* work scales with $m \times n \times p$.
   - *Shuffle/Reducer* work scales strictly with output size $m \times p$.
4. **Node & Process Scaling**: For small matrices, adding nodes/processes increases execution time because the network and scheduling latency ($150ms+$) dwarfs the computation time ($<1ms$). Parallelism is only beneficial when the chunked computation exceeds scheduling overhead (e.g., $1000 \times 1000$).
5. **Load Balancing**: Odd dimensions cause minor row imbalances (e.g. 2 rows to mapper 1, 1 row to mapper 2), but scheduling variances mask these differences for small matrices.
