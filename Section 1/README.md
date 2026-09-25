# Section 1: Distributed Matrix Multiplication

This is the code for Section 1 Question 1 of our Distributed Systems Assignment 3.

## How it works
I used the Row-Row MapReduce method to multiply Matrix A and Matrix B to get Matrix C.
- Matrix A is split row by row.
- Matrix B is passed to every mapper.
- Mapper does the dot product calculation.
- Reducer collects everything, groups them by row index, and prints the final matrix.

## Files inside
- `mapper.cpp`: This is the C++ code for mapping phase.
- `reducer.cpp`: This is the C++ code for reducing phase.
- `run_local.sh`: Bash script if you want to run and test everything locally on Linux or Windows Git Bash.
- `run_rce.sh`: Script to run the SLURM jobs on the RCE cluster.
- `tests/`: Folder containing all the input matrices (square, tall, wide, etc).
- `results/`: Folder containing the CSV benchmark files and the generated plots.
- `report.md`: My final report with the results and answers.

## How to run locally
If you want to run it on your local machine, just run this command in bash terminal:
```bash
bash run_local.sh
```
It will compile the cpp files and test all the matrix categories.

## How to run on RCE cluster
To run all benchmark configurations across all matrix categories on the cluster:
```bash
bash run_rce.sh --submit-grid
```
All outputs are saved to `results/`.

## How to generate plots
To regenerate all 19 performance plots from the CSV benchmarks:
```bash
python plot_results.py
```
Plots are automatically saved into `results/plots/`.
