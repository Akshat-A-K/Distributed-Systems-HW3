# Distributed Systems - Homework 3

This repository contains our solutions for Distributed Systems Assignment 3.

## Repository Layout
- `Section 1/`: Distributed Matrix Multiplication using MapReduce (Row-Row method in C++ with local and SLURM runners).
- `Section 2/`: Section 2 problems and implementations.
- `Section 3/`: Collaborative Document Editing using gRPC in Python (Server, Client CLI, and Update Streaming).
- `hw3-final.pdf`: Assignment problem statement.
- `rce_grpc_execution_guide.pdf`: Cluster execution guide.
- `Mapreduce_distributed.sh`: Distributed MapReduce runner script.
- `MapreduceForLocalTesting.sh`: Local bash script for testing MapReduce pipeline.

## Section 1 Overview (Matrix Multiplication)
Section 1 implements distributed matrix multiplication C = A x B using the Row-Row MapReduce approach:
- `mapper.cpp`: Loads matrix B into memory, reads assigned rows of A from stdin, and computes output rows.
- `reducer.cpp`: Takes sorted mapper outputs and emits final matrix C rows.
- `run_local.sh`: Local bash script to run and verify all test categories locally.
- `run_rce.sh`: SLURM batch script for multi-node and multi-process execution on the RCE cluster.
- `plot_results.py`: Generates scaling and performance comparison graphs from benchmark CSVs.
- `results/`: Contains benchmark execution CSV files and generated performance plots.
- `report.md`: Complete analysis report containing experimental setup, correctness, timing results, and scaling analysis.

### Quick Start for Section 1

To compile and run locally on Linux / macOS / WSL:
```bash
cd "Section 1"
bash run_local.sh
```

To run on RCE HPC cluster using SLURM:
```bash
cd "Section 1"
bash run_rce.sh --submit-grid
```

To regenerate performance plots:
```bash
python plot_results.py
```

## Section 3 Overview (Collaborative Document Editing using gRPC)
Section 3 implements Problem 1: a multi-user collaborative document editing system using gRPC and Protocol Buffers in Python:
- `document.proto`: Service definition for creating, retrieving, editing documents, and streaming real-time updates.
- `server.py`: Central gRPC server managing in-memory documents, thread-safe synchronization for concurrent edits, and streaming subscriber queues.
- `client.py`: Multi-threaded interactive CLI client with background update listening.
- `bugs_fixes.md`: Record of edge case verification and testing results.
- `README.md`: Detailed instructions and multi-client demonstration walkthrough.

### Quick Start for Section 3

1. Start the server:
```bash
cd "Section 3"
python server.py localhost:50051
```

2. Start one or more clients (in separate terminals):
```bash
cd "Section 3"
python client.py localhost:50051
```
