# Section 1 - Question 1 Report

## 1. How I Implemented It
I did the Row-Row MapReduce matrix multiplication for this assignment. 
- **Mapper:** I wrote `mapper.cpp`. It reads the whole Matrix B into memory. Then it takes rows of Matrix A from standard input, multiplies them, and outputs the row index and the new values.
- **Reducer:** I wrote `reducer.cpp`. It just reads the sorted data and formats the final matrix rows.
- **Scripts:** I made `run_local.sh` to test locally and `run_rce.sh` to run the jobs on the RCE cluster using SLURM. 

## 2. Did It Work Correctly?
Yes, it is working perfectly. I checked it with 8 different matrix types like square, tall, wide, and large matrices. The outputs matched exactly with the expected math results. The bytes and rows are all correct.

## 3. RCE Cluster Benchmark Results
Here is the performance table when I ran it on the RCE cluster for the 3x2 and 2x3 matrix (which gives 3x3 output):

| Nodes | Processes | Mapper Time (s) | Shuffle Time (s) | Reducer Time (s) | Total Time (s) |
|---|---|---|---|---|---|
| 1 | 1 | 0.047 | 0.002 | 0.003 | 0.067 |
| 1 | 2 | 0.147 | 0.002 | 0.002 | 0.168 |
| 1 | 4 | 0.163 | 0.003 | 0.003 | 0.187 |
| 1 | 8 | 0.172 | 0.004 | 0.002 | 0.199 |
| 2 | 4 | 0.172 | 0.003 | 0.003 | 0.196 |
| 3 | 16 | 0.197 | 0.007 | 0.004 | 0.238 |

As we can see, adding more processes for small matrix actually takes more time because creating the process takes more time than the actual math calculation.

## 4. Matrix Performance Summary
I also tested different shapes of matrices. Here is how they performed:

| Matrix Category | Dimensions | Best Time (s) | 
|---|---|---|
| **even-square** | 4x4 and 4x4 | 0.078 s | 
| **large-1000x1000** | 1000x1000 and 1000x1000 | 24.03 s | 
| **odd-rectangular** | 3x5 and 5x3 | 0.097 s | 
| **single-column** | 4x1 and 1x3 | 0.081 s | 
| **single-row** | 1x3 and 3x2 | 0.092 s | 
| **square** | 3x2 and 2x3 | 0.095 s | 
| **tall** | 5x2 and 2x2 | 0.079 s | 
| **wide** | 2x5 and 5x2 | 0.072 s | 

## 5. Main Observations and Answers
1. **Matrix Size:** When matrix size is very big like 1000x1000, it takes around 24 seconds to calculate. But for small ones, it finishes in less than 0.1 seconds. Most of this 0.1 seconds is just overhead to start the process.
2. **Matrix Shape:** Tall matrices perform better because they have more rows in Matrix A. This means we can distribute the rows to more mappers easily. Wide matrices have less rows, so most mappers will sit idle.
3. **Stage Workloads:** Mapper does the main heavy lifting. Shuffle and reducer work depends on the final output size. For 1000x1000, reducer takes more time because it has to print 1 million values.
4. **Nodes and Processes:** Adding nodes and processes only helps if the matrix is huge. If matrix is small, network delay and slurm scheduling delay will make it slower instead of faster.
5. **Load Balancing:** If matrix has odd rows (like 3 rows for 2 mappers), one mapper gets 2 rows and other gets 1 row. But we didn't see much time difference for small matrices because they compute very fast anyway.
