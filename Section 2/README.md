# Section 2: Real-World Problem (HW2 Q8, Weather Analytics)

This continues our Homework 2 Q8 problem (Large-Scale Weather and Environmental Data Analytics)
using both distributed-system paradigms Section 2 asks for.

- **`Q1_MapReduce/`** — Hadoop Streaming implementation (C++ mapper/reducer). Hadoop/YARN itself
  is affected by the ongoing RCE environment issue, so execution uses the Slurm-based fallback
  the course allows in the meantime; see that folder's README for the full setup, correctness,
  and benchmark details, including a direct comparison against our own Homework 2 MPI results.
- **`Q2_gRPC/`** — real-time streaming version using gRPC: a replay client, a coordinator with
  multiple analytics workers, and a CLI dashboard that stays live while data is still arriving.
  See that folder's README for setup, architecture, correctness, and benchmark results (both
  single-node and with the client/server split across different physical nodes).

Both implementations reuse the same dataset generator and sequential reference program from
Homework 2 as their correctness oracle.
