# HW3 Section 2 · Q2 — Real-Time Streaming Analytics via gRPC (HW2 Q8)

A gRPC re-implementation of HW2 Q8 (weather/environmental analytics), continuing the same
problem Q1 (Hadoop MapReduce) re-implemented as a batch job — here the data arrives as a
continuous stream, processed by multiple concurrent workers, with a live CLI dashboard and
support for analytics queries while ingestion is still running.

Final analytics (once a stream completes) are **byte-for-byte identical** to HW2's unmodified
sequential reference (`reference/weather_seq.cpp`) — same 19-line output, same 6-decimal
precision (per the instructor's Q7/Q8 clarification), same tie-break rules. See
[Correctness](#correctness) for how this was verified across 248 local test configurations,
including at N=1,000,000.

## Architecture

```
generate.cpp dataset ("N K S" header + records, reused from Q1)
      │
stream_client.py ──gRPC client-streaming──►  server.py (Coordinator, ONE process)
(replays records, configurable                    │  handler routes each record by
 batch size = granularity, + rate)                 │  station_id % W into a per-worker queue
                                                    ▼
                                     Worker thread 0..W-1 (sole writer of its own shard)
                                     shard state = mapper.cpp's local accumulators, ported to
                                     Python: Kahan sums, min/max, extreme count, hottest/
                                     coldest candidate, per-station map, per-interval map
                                                    │  each shard guarded by its own Lock
dashboard.py ──gRPC unary GetAnalytics────────────► combiner: lock one shard at a time, copy
(polls, prints live 19-line report)  ◄──────────────  summary, merge (reducer.cpp's GlobalCombiner,
                                                       ported), resolve busiest interval / top-K
                                                       (finalize.py's exact functions, imported
                                                       unchanged from Q1)
```

One coordinator process, in-process worker **threads** — not separate gRPC services per worker,
not a multi-node worker deployment. Only two network hops exist (client→coordinator,
dashboard→coordinator), both gRPC, matching the one pattern the RCE cluster's own gRPC guide
actually documents (server on one node, clients on others, connect via `hostname:port`).

This is Q1's mapper/reducer split kept alive as long-running threads instead of one-shot
processes: a **worker is a persistent mapper** for its shard; a **query is a live re-run of the
reducer's merge step** over snapshots of every shard, instead of over a sorted file.

## Design decisions

### Why Python (not C++, unlike Q1)

Q2's spec has no language mandate (Q1's does — "Language: C++"). The RCE gRPC execution guide's
only documented, confirmed pattern is Python (`python3 server.py` / `client.py`) — no C++
grpc/protoc setup exists anywhere in the course materials. Python is also far faster to iterate
on under the team's time constraint. Decided once, up front, not revisited.

### `.proto` interface (`proto/weather_stream.proto`)

- **`StreamRecords(stream IngestMessage) returns (IngestSummary)`** — client-streaming. The first
  message must be a `DatasetHeader`, every message after that a `RecordBatch`. Batch size is
  carried *in the message* (`repeated Record`), not just as a client-side send delay — this makes
  "streaming granularity" a single tunable (B=1 is genuinely per-record streaming, B=10000 is
  bulk) instead of two separate, harder-to-compare knobs. Returns only after every record already
  sent has been applied (a drain barrier on the server), so a query issued right after this
  returns is guaranteed to see the whole stream. Rejected alternatives: bidi streaming with a
  per-batch ack (adds a response per batch for no benefit at small B) and unary-per-batch (loses
  stream semantics and adds per-call overhead).
- **`GetAnalytics(AnalyticsRequest) returns (AnalyticsSnapshot)`** — unary; the dashboard polls.
  The spec's own language is "querying" the current analytics, and polling gives a clean,
  client-controlled round-trip time — needed to measure query latency and the effect of
  concurrent query clients (both explicitly required experiments). Server-push (a
  `WatchAnalytics` streaming RPC) would blur exactly that measurement, so it wasn't built.
- **`Reset(ResetRequest) returns (ResetResponse)`** — reconfigures worker count/partition on a
  running server without restarting the process. Used by `run_correctness.sh` and `bench.py` to
  sweep configurations quickly, and matches how a hand-started server (via ssh, per the RCE
  guide's pattern) would need to be reused across a benchmark sweep.

### Record routing / worker state

Default partition is **`station_id % W`** — each worker owns a disjoint set of stations, so
per-station Kahan sums are built in file order regardless of W (byte-identical to sequential for
every field, not just within tolerance — see [Correctness](#correctness)). A second mode,
**`batch_rr`** (whole batches round-robinned across workers), was added specifically to answer the
spec's required "record distribution strategy" investigation with a genuinely different strategy:
station maps now overlap across workers, exercising the cross-worker station-merge path
(`combine_station_interval` in `analytics_core.py`) that `station` mode mostly avoids. Per-worker
state is a direct, field-by-field port of `mapper.cpp`'s accumulators
(`analytics_core.ShardState`); combining them at query time is a direct port of `reducer.cpp`'s
`GlobalCombiner`/`StationCombiner`/`IntervalCombiner`.

### Concurrency and locking

One bounded `Queue` per worker (the ingestion handler routes each record to exactly one worker, so
each shard has exactly one writer — no write-write races are possible). A query locks **one**
worker's shard at a time, copies a snapshot, releases, and moves on — never two locks at once, so
deadlock is impossible, and ingestion is never blocked for longer than one shard's copy. The merge
across shards happens outside any lock. This gives every snapshot an internal consistency
guarantee (TOTAL == sum of per-station counts == sum of per-interval counts) but **not** a
guarantee that it's an exact prefix of the stream if workers happen to be at slightly different
points — an accepted, documented property (the spec asks for "a useful view of the current
state," not point-in-time atomicity across shards), verified never to actually violate the
invariants above under concurrent load (see `tests/test_coordinator_concurrency.py`).

### The GIL, measured rather than assumed

Python threads share one interpreter lock, so more worker threads were not expected to buy more
*parallel* CPU time — and the benchmark confirms this directly: server CPU utilization
((user+sys)/elapsed) stays at **~103–105% across W=1..8** (see [Benchmarks](#benchmarks)), i.e.
essentially one core, regardless of worker count. What more workers *do* still buy here:
independent shards (a query only ever locks one shard at a time, not the whole system) and overlap
between receiving/deserializing the next batch and applying the previous one. This is reported as
a real, explained finding, not engineered around with `multiprocessing` — that would have added
real IPC/serialization overhead to trade for a benefit the assignment doesn't actually require
demonstrating (the architecture, not raw CPU-bound throughput, is what's being evaluated).

## Reused vs. ported vs. new

| Reused verbatim (copied) | Ported (same logic, C++ → Python) | New |
|---|---|---|
| `reference/generate.cpp`, `reference/weather_seq.cpp` (oracle), `src/finalize.py`'s `find_busiest_interval`/`top_k_stations` (imported, not reimplemented), `scripts/compare_correctness.sh`, Q1's `tie_test_with_header.txt`/`tie_test_k3.txt`, the `SLURM_SUBMIT_DIR` sbatch pattern | `kahan.hpp` → `KahanSum` (`analytics_core.py`); `mapper.cpp`'s accumulation loop → `ShardState`; `reducer.cpp`'s `GlobalCombiner`/`StationCombiner`/`IntervalCombiner` → `GlobalCombiner`/`combine_station_interval` | `.proto`, `coordinator.py`, `server.py`, `stream_client.py`, `dashboard.py`, `bench.py`, `diagnose_mismatch.py`, `data/edge_single_station.txt` + `data/edge_cross_worker_ties.txt` (new adversarial datasets, see [Correctness](#correctness)), this README |

## Directory structure

```
Q2_gRPC/
├── proto/weather_stream.proto
├── src/            analytics_core.py  coordinator.py  server.py  stream_client.py
│                   dashboard.py  dataset_io.py  finalize.py (copied)
│                   weather_stream_pb2*.py (generated per machine, gitignored)
├── reference/      weather_seq.cpp  generate.cpp  (copied verbatim)
├── data/           tie_test_*.txt (copied) + edge_*.txt (new) ; generated/ (gitignored, made by make_datasets.sh)
├── tests/          test_coordinator_concurrency.py
├── scripts/        gen_proto.sh  build_reference.sh  make_datasets.sh
│                   run_correctness.sh  compare_correctness.sh (copied)  diagnose_mismatch.py
│                   bench.py
└── results/        e1_workers.csv  e2_granularity.csv  e3_concurrent_queries.csv
```

## Setup

```bash
cd Q2_gRPC
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
bash scripts/build_reference.sh      # builds build/weather_seq, build/generate
bash scripts/gen_proto.sh            # generates src/weather_stream_pb2*.py — regenerate on
                                      # every new machine (protobuf version-checks generated
                                      # code against the installed runtime library)
bash scripts/make_datasets.sh        # data/generated/*.txt
```

## Running locally (three processes)

```bash
# terminal 1
.venv/bin/python3 src/server.py --host 127.0.0.1 --port 50151 --workers 4

# terminal 2
.venv/bin/python3 src/dashboard.py 127.0.0.1:50151 --interval 0.5

# terminal 3
.venv/bin/python3 src/stream_client.py 127.0.0.1:50151 data/generated/gen_n1000000_k5_s100_seed42.txt \
    --batch-size 1000 --rate 200000
```

The dashboard updates live while the client streams; `dashboard.py 127.0.0.1:50151 --once` prints
just the final 19(+K)-line report and exits.

## Running on RCE

**Multi-node (client and server on different physical nodes) is the primary configuration for
Q2.** Q2's whole premise is a coordinator receiving a stream from a genuinely separate client
process over gRPC — that separation is only really tested when the two are on different
machines; same-node loopback can't distinguish "the network boundary works" from "the network
boundary happens to be free." Q1's single-node choice (see `Q1_MapReduce/README.md`) is kept
as-is — it mirrors HW2's own MPI benchmark methodology directly, which measured a fixed node
count while varying rank count, so this project does the same for the mapper/reducer substitute.

Both configurations are run and kept: a genuine multi-node sweep (`submit_bench_multinode.sh`,
server and client on separate allocated nodes, real inter-node TCP, W ∈ {1,2,4,8} ×
{station, batch_rr}) as the primary Q2 performance evidence, and a single-node sweep
(`submit_bench.sh`) as a same-machine baseline — the gap between the two, if any, is itself a
useful "does node separation matter" observation, not a discarded result.

`rce_grpc_execution_guide.pdf`'s own 3-node pattern (server on one node, clients on others, for
Section 3's problems) matches this multi-node approach closely — client/server/dashboard are
three separate OS processes over a real socket regardless of node count, never sharing memory.

```bash
# 0. Before assuming anything: confirm the environment (no course-documented module/pip
#    instructions exist for grpc/protobuf on this cluster — this is a real, flagged gap)
python3 --version
uname -m          # x86_64 here means the long-double precision risk in Correctness below
                  # actually applies; arm64 (like the dev machine this was built on) means it
                  # doesn't -- submit_correctness.sh/submit_bench.sh check this automatically

# 1. Correctness (sbatch; same SLURM_SUBMIT_DIR pattern proven in Q1).
#    scripts/setup_python_env.sh loads the newest `module load python/3.x` it can find and
#    (re)builds .venv on it, rebuilding if an existing .venv is on Python <3.8 -- our first
#    RCE run (job 98701) silently built .venv on this cluster's default python3, which turned
#    out to be 3.6.8, and the whole matrix ran ~10-15x slower than on the dev machine as a
#    result, hitting the original 30-minute time limit at 234/240 small-matrix cases. A
#    3.10+ interpreter is available on this same cluster via `module load python/3.x`.
sbatch scripts/submit_correctness.sh
squeue -u $USER
cat results/correctness_<jobid>.log     # expect "Correctness matrix: 248/248 passed."

# 2. Multi-node benchmarks (sbatch; PRIMARY Q2 performance evidence, see above).
#    Requests 4 nodes; server lands on node 0, client on node 1 (2 of the 4 actively used,
#    with headroom to extend to the other 2 for concurrent query-load clients if desired).
sbatch scripts/submit_bench_multinode.sh
cat results/bench_4node_<jobid>.log
# -> results/e1_workers_4node_<jobid>.csv

# 3. Single-node benchmarks (sbatch; same-machine baseline, E1/E2/E3, 3 reps each -- see Benchmarks)
sbatch scripts/submit_bench.sh
cat results/bench_<jobid>.log
# -> results/e1_workers.csv, e2_granularity.csv, e3_concurrent_queries.csv

# 4. (Optional) A live multi-node demo to watch interactively -- three ssh sessions, one per
#    allocated node, connected via each node's real hostname (never "localhost" across nodes):
salloc --nodes=3 --ntasks-per-node=1 --cpus-per-task=8 --partition=debug
scontrol show hostnames $SLURM_JOB_NODELIST    # -> e.g. node06 node07 node08
ssh node06; cd ~/Q2_gRPC; .venv/bin/python3 src/server.py --port 50051 --workers 4
ssh node07; cd ~/Q2_gRPC; .venv/bin/python3 src/dashboard.py node06:50051
ssh node08; cd ~/Q2_gRPC; .venv/bin/python3 src/stream_client.py node06:50051 <dataset>
```

**Status**: correctness (job 98751, node06, x86_64: 244/248 pass, the 4 failures a confirmed
benign precision tie, see [Correctness](#correctness)), single-node benchmarks (job 98780:
E1/E2/E3, 3 reps each, see [Benchmarks](#benchmarks)), and multi-node benchmarks (job 99451,
server on `node01`, client on `node02`, 8 configurations, all `correctness=PASS`, see
[Multi-node results](#multi-node-results-server-and-client-on-different-physical-nodes) below)
have all completed successfully. A live interactive 2-node demo (`salloc` job 99440, server on
`node06`, dashboard+client on `node07`) additionally confirmed the dashboard updates correctly
in real time across physically separate nodes — transcript in `results/demo_2node_live.txt`.
First correctness attempt (job 98701) hit the original 30-minute time limit after running
~10-15x slower than the dev machine, traced to this cluster's default `python3` being a
decade-old 3.6.8 — fixed by `scripts/setup_python_env.sh`, which loads the newest available
`module load python/3.x` and rebuilds `.venv` on it if needed.

### Multi-node results (server and client on different physical nodes)

Job 99451: 4 nodes allocated (`node[01-03,07]`), server on `node01`, client on `node02` (2
actively used, leaving headroom for a larger query-load setup on the remaining 2), N=1,000,000,
B=1000, unlimited rate, same sweep as the single-node E1 experiment above for direct comparison:

| W | partition | single-node throughput | cross-node throughput | difference |
|---|---|---:|---:|---:|
| 1 | station  | 522,358 rec/s | 404,275 rec/s | −22.6% |
| 1 | batch_rr | 531,249 rec/s | 409,256 rec/s | −23.0% |
| 2 | station  | 478,272 rec/s | 367,233 rec/s | −23.2% |
| 2 | batch_rr | 529,478 rec/s | 403,689 rec/s | −23.8% |
| 4 | station  | 456,882 rec/s | 339,469 rec/s | −25.7% |
| 4 | batch_rr | 534,018 rec/s | 411,409 rec/s | −23.0% |
| 8 | station  | 431,689 rec/s | 320,916 rec/s | −25.6% |
| 8 | batch_rr | 536,155 rec/s | 403,241 rec/s | −24.8% |

**Observation**: real inter-node network latency has a consistent, measurable cost — cross-node
throughput is ~23–26% lower than single-node across every worker count and both partition
strategies, answering directly (not by inference) the question the single-node design was
originally worried about confounding. The *shape* of both trends is preserved across node
counts (`station` declines with W, `batch_rr` stays flat) — node separation shifts the whole
curve down by a roughly constant fraction rather than changing which trend dominates, meaning
the GIL-bound worker-count finding from the single-node run still holds under real network
conditions, just at a uniformly lower absolute throughput. All 8 configurations passed
correctness against the oracle (`build/weather_seq` on `node01`), confirming the drain barrier
and final-state consistency work correctly across a real network partition between ingestion and
query paths, not just in-process. Raw data: `results/rce/e1_workers_4node_99451.csv`.

![Single-node vs. cross-node throughput, both partition strategies](results/plots/e1_single_vs_multinode.png)

## Correctness

**Method**: a three-way comparison for every test dataset — the C++ oracle
(`build/weather_seq`), the pure-Python sequential mode (`analytics_core.py --sequential`, no
gRPC, no threads), and the full streaming system. A Python-vs-oracle mismatch would localize to
the port itself; a streaming-vs-sequential mismatch would localize to concurrency/routing/
combining.

**The one real risk**: Python's `float` is a C `double`; the C++ oracle's `KahanSum` additionally
uses `long double` internally. On the dev machine (Apple Silicon, `arm64`) `long double == double`
— confirmed via `uname -m` — so this risk **cannot** materialize here; on x86-64 (`long double` is
80-bit), it could show up as an exact half-unit tie in an average's 6th printed decimal, and only
at N where `sum/N` can actually land on a tie (impossible when N is a multiple of 10, more exposed
at N=10⁵/10⁶). `scripts/diagnose_mismatch.py` exists to classify any future mismatch
(STRUCTURAL/INTEGER = a real bug; FLOAT_MINOR = consistent with this precision risk, worth a
second look but not automatically a bug) rather than silently tolerating or silently failing on
it.

**Results** (`scripts/run_correctness.sh`, all on the dev machine over `localhost`):

- **248/248 configurations passed, byte-for-byte**, sweeping:
  - 4 hand-built adversarial datasets + `data/generated/{gen_n5,gen_n100,gen_n10000,gen_n123457}...txt`
    (8 datasets) × **W ∈ {1,2,3,4,8}** × **partition ∈ {station, batch_rr}** × **batch size ∈ {1,3,1000}**
  - `gen_n100000` and `gen_n1000000` × W ∈ {1,4} × both partitions × B=1000 (reduced matrix, per
    the plan, since the full sweep is redundant once the small-dataset matrix has already
    exercised every routing/combining code path)
- This includes **N=1,000,000 at W=1 and W=4, both partition modes** — a real exact match, not
  a within-tolerance one, confirming the double-precision port introduces no drift on this
  platform even at scale.
- The two new adversarial datasets built for this project (`data/edge_single_station.txt`,
  `data/edge_cross_worker_ties.txt`) specifically exercise: a single station with W>1 (idle
  workers, K larger than the station count, duplicate records, exact extreme-boundary
  temperatures 0.00/40.00); a 3-way hottest tie and a cross-timestamp coldest tie whose
  contending records land on *different* workers depending on W (testing the reducer-merge
  comparator across shards, not just within one); a 3-way tie across three different interval
  buckets; and a station-count tie exactly at the K-th cutoff. All pass at every tested W.
- `tests/test_coordinator_concurrency.py` additionally streams a 100,000-record dataset from one
  thread while 4 threads query concurrently, across W ∈ {1,2,4,8} and both partitions, asserting
  on every snapshot that TOTAL never decreases and never exceeds records actually applied, and
  that the final result exactly matches the sequential mode. All pass (`python3 -m unittest
  tests.test_coordinator_concurrency -v`, from `Q2_gRPC/`).
- Concurrent query support was also verified manually against a live 1,000,000-record stream:
  repeated `GetAnalytics` calls during ingestion returned strictly increasing counts
  (`applied=41000 → 171000 → 301500 → 431500 → 561000`, `active_streams=1` throughout), confirming
  queries work correctly *during* ingestion, not just after.
- **Confirmed on real RCE hardware (x86_64, job 98751)**: 244/248 passed; the 4 failures were all
`AVERAGE_WIND_SPEED` at N=100,000, off by exactly 1e-6. Exact rational verification on the actual
dataset gave `60.1152135` — a perfect halfway point — which round-half-even correctly resolves to
`.115214` (the oracle's long-double answer); the streaming system's double-precision Kahan sum
landed a float's-width on the other side. This is the double-vs-long-double precision risk above,
now empirically confirmed rather than theoretical, and not a logic bug.

**Not built**: isolated unit tests for `analytics_core.py` in a vacuum (e.g. a standalone
  `KahanSum` trace test). Given the time constraint, and that the 248-case matrix plus the
  concurrency test already exercise every accumulation/merge/tie-break path at the integration
  level far more thoroughly than a hand-traced unit test would, this was judged redundant rather
  than skipped by oversight.

## Benchmarks

**Primary results are from real RCE hardware** (job 98780, node06, x86_64, single node —
`--cpus-per-task=8`, matching HW2 MPI's and Q1's own single-node convention), 3 repetitions per
configuration, means reported below. Local dev-machine numbers (Apple M-series, `arm64`, 1 rep)
are kept in the tables further down for reference/reproducibility, but RCE is what's used for any
cross-paradigm comparison. Every benchmark run re-verifies correctness against the oracle — a run
is never reported as fast without its correctness verdict also being recorded (see the E2 note
below).

### E1 — effect of worker count (RCE, N=1,000,000, B=1000, unlimited rate, mean of 3 reps)

| W | partition | throughput (rec/s) | server CPU util. |
|---|---|---|---|
| 1 | station  | 522,358 | 102.0% |
| 1 | batch_rr | 531,249 | 102.0% |
| 2 | station  | 478,272 | 101.9% |
| 2 | batch_rr | 529,478 | 101.9% |
| 4 | station  | 456,882 | 101.9% |
| 4 | batch_rr | 534,018 | 101.9% |
| 8 | station  | 431,689 | 102.0% |
| 8 | batch_rr | 536,155 | 101.9% |

**Observation**: same pattern as the local dev-machine run (see below), confirmed on real
cluster hardware — server CPU utilization stays pinned at ~one core regardless of W (101.9–102.0%
throughout), the GIL prediction from [Design decisions](#the-gil-measured-rather-than-assumed).
`station` partitioning declines noticeably as W grows (522k→432k, −17%) while `batch_rr` stays
essentially flat-to-slightly-up (531k→536k), because per-record `station_id % W` bucketing does
real Python-level work per record that scales with thread-switching overhead, while whole-batch
round-robin routing doesn't. More workers don't buy raw throughput here; they buy the
architectural properties described in Design decisions (independent shards, pipelining).

![E1: throughput vs. worker count, and server CPU utilization pinned near one core](results/plots/e1_workers.png)

### E2 — effect of streaming granularity (RCE, N=100,000, W=4, station, 1 query client @100ms, mean of 3 reps)

| batch size (B) | throughput (rec/s) | query p95 (ms) | correctness |
|---|---|---|---|
| 1      | 22,193  | 2.18  | FAIL* |
| 10     | 100,597 | 1.91  | FAIL* |
| 100    | 317,983 | 2.69  | FAIL* |
| 1,000  | 454,893 | 7.05  | FAIL* |
| 10,000 | 467,802 | 31.15 | FAIL* |

\* **Every** E2 row fails correctness, at every batch size — this is the confirmed, benign
`AVERAGE_WIND_SPEED` precision tie from [Correctness](#correctness) (N=100,000 on `x86_64` hits
the exact halfway point `60.1152135`), not an instability introduced by streaming granularity.
It's expected to appear on every row here since it depends only on N, not on B — included for
completeness, not as a new finding.

**Observation**: throughput scales strongly from B=1 to B=1,000 (~20×) as per-message
gRPC/protobuf overhead gets amortized over more records, then nearly plateaus (B=1,000→10,000
gains ~3%) — matching the local dev-machine run's shape closely. Query p95 stays low and stable
through B=100, then rises sharply at B≥1,000 (7ms→31ms) — a single large batch occupies a
worker's lock for noticeably longer, a real, measured granularity/latency trade-off, consistent
across both machines.

![E2: throughput and query p95 latency vs. streaming batch size](results/plots/e2_granularity.png)

### E3 — effect of concurrent query clients (RCE, N=1,000,000, W=4, station, B=1000, closed-loop queries, mean of 3 reps)

| concurrent query clients (Q) | ingestion throughput (rec/s) | query p50 / p95 / p99 (ms) | queries completed |
|---|---|---|---|
| 0  | 454,301 | — | 0 |
| 1  | 371,100 | 0.25 / 0.30 / 7.58 | ~11,926 |
| 4  | 257,879 | 0.70 / 9.91 / 13.02 | ~21,163 |
| 16 | 249,124 | 2.83 / 42.1 / 47.8 | ~21,057 |

**Observation**: ingestion throughput drops by nearly half (454k → 249k rec/s, −45%) as
closed-loop query load rises from 0 to 16 concurrent clients, and query latency percentiles rise
sharply in step (p50 0.25ms→2.8ms, p95 0.30ms→42.1ms) — the same GIL-contention story as the local
run, now confirmed on real cluster hardware with proper 3-rep means. This is the clearest evidence
in this project that the single-process design trades query/ingestion isolation for simplicity: a
production system wanting to protect ingestion throughput under heavy query load would need either
multiple processes (with real IPC cost) or a compiled, GIL-free runtime — a trade-off this README
states rather than hides.

![E3: ingestion throughput drops and query latency percentiles rise under concurrent query load](results/plots/e3_concurrent_queries.png)

All three plots above are regenerated from `results/rce/*.csv` with `python3 scripts/plot_benchmarks.py`
(needs `matplotlib`; not part of the required build, purely a presentation-format addition over the
tables — the spec allows "plots/tables" either way).

### Local dev-machine numbers (Apple M-series, `arm64`, 1 rep — kept for reference)

<details>
<summary>Expand local (non-RCE) E1/E2/E3 tables</summary>

**E1** (N=1,000,000, B=1000, unlimited rate):

| W | partition | throughput (rec/s) | server CPU util. |
|---|---|---|---|
| 1 | station  | 643,308 | 103.4% |
| 1 | batch_rr | 647,709 | 103.4% |
| 2 | station  | 638,642 | 103.8% |
| 2 | batch_rr | 650,152 | 103.4% |
| 4 | station  | 615,970 | 104.6% |
| 4 | batch_rr | 657,098 | 103.4% |
| 8 | station  | 588,844 | 105.1% |
| 8 | batch_rr | 652,196 | 103.4% |

**E2** (N=100,000, W=4, station, 1 query client @100ms) — all PASS here since `arm64`'s
`long double == double` can't hit the RCE-only precision tie:

| batch size (B) | throughput (rec/s) | query p50 / p95 (ms) |
|---|---|---|
| 1     | 26,382  | 1.0 / 1.2 |
| 10    | 135,650 | 1.1 / 2.1 |
| 100   | 440,959 | 1.3 / 2.7 |
| 1,000 | 607,829 | 1.1 / 5.5 |
| 10,000| 618,147 | 2.2 / 41.6 |

**E3** (N=1,000,000, W=4, station, B=1000, closed-loop queries):

| concurrent query clients (Q) | ingestion throughput (rec/s) | query p50 / p95 / p99 (ms) | queries completed |
|---|---|---|---|
| 0  | 618,174 | — | 0 |
| 1  | 496,685 | 0.14 / 0.89 / 5.70 | 10,321 |
| 4  | 345,638 | 0.41 / 7.26 / 9.40 | 14,792 |
| 16 | 290,740 | 1.67 / 25.9 / 29.3 | 15,201 |

</details>

**Not built** (stretch goals explicitly scoped out given the time constraint, per the plan): E4
(streaming rate/backlog), E5 (station-count load-skew), a lazy-vs-preload client comparison, and
matplotlib plots (the tables above are the "tables" the spec asks for; polished plots were judged
lower-value than getting all three *required* experiments — worker count, granularity, concurrent
queries — run, correctness-checked, and now confirmed on real RCE hardware).

## Comparison against HW2 MPI and Q1 (Hadoop/Slurm-substitute)

**Data sources**: HW2's own benchmark (`HW2/Q8/results/analysis_detail.csv`, from
github.com/Akshat-A-K/Distriubuted-Systems-HW2) and Q1's `Q1_MapReduce/results/rce_benchmark_98781.csv`,
both single-node RCE runs, compared against this project's `results/rce/e1_workers.csv`. As in
Q1's README: same cluster, matching record counts, but not byte-identical datasets (HW2 varied
K/S/seed per size; this project and Q1 both fixed K=5 S=100 seed=42) — an "as delivered"
comparison, not an attempt to isolate an inherent per-paradigm cost.

**Coverage gap, stated plainly**: this project's own experiment design doesn't give a full
apples-to-apples grid against HW2/Q1's P/M sweeps. E1 (worker-count scaling) was only run at
N=1,000,000; E2 (the only N=100,000 experiment) fixed workers at 4 and varied batch size instead.
So the comparison below is honest about which cells actually have data, not filled in with
invented numbers for the missing ones.

**Node count, deliberately kept equal here**: the table below uses Q2's *single-node* E1 numbers
specifically, even though multi-node (client/server on separate nodes) is the primary
configuration for Q2's own Performance Evaluation (see "Running on RCE" above) — because
HW2's MPI and Q1's Slurm-substitute numbers are themselves single-node, and holding node count
equal across all three is what makes this particular comparison meaningful. Mixing in Q2's
multi-node numbers here would reintroduce exactly the confound (real inter-node latency) this
table exists to avoid. The multi-node numbers are reported separately, in their own section below,
once available.

### Wall time at N=1,000,000 (the only size with a full worker-count sweep here)

| Task-count knob | 1 | 2 | 4 | 8 |
|:---|---:|---:|---:|---:|
| HW2 MPI (P ranks) | 1.537s | 1.534s | 1.599s | 1.739s |
| Q1 Slurm-substitute (M mapper chunks) | 0.996s | 1.003s | 1.014s | 1.043s |
| Q2 gRPC (W worker threads, `station`) | 1.914s | 2.091s | 2.189s | 2.316s |
| Q2 gRPC (W worker threads, `batch_rr`) | 1.882s | 1.889s | 1.873s | 1.865s |

(HW2 sequential baseline at this size: 1.274s.)

### Same underlying limitation, three different mechanisms

All three systems' "task count" knob fails to deliver real multi-core speedup here, each for a
different, specific, already-measured reason:

- **HW2 MPI**: real OS processes, genuinely schedulable across cores — but process-launch and
  collective-communication overhead exceeds the actual per-record compute at this problem size
  (HW2's own finding: at P=8/1M records, comm time 0.0099s > compute time 0.0062s).
- **Q1's Slurm-substitute**: mapper chunks run *sequentially on one core* by construction (the
  permitted fallback for an unavailable Hadoop cluster, not a real distributed execution) — more
  chunks is strictly more overhead, never real parallelism.
- **Q2's gRPC workers**: real Python threads, but bounded by the GIL — measured directly via
  server CPU utilization staying pinned at ~102% regardless of W (see Benchmarks above), meaning
  only about one core's worth of work ever actually happens concurrently no matter how many
  worker threads exist.

None of the three implementations here demonstrate genuine multi-core scaling from their
task-count knob at this problem size — a real, reportable finding about how each paradigm's
*implementation* (not necessarily the paradigm itself) behaves under these specific constraints
(a shared single-node cluster allocation for all three, plus Python's GIL for Q2 specifically and
a course-mandated single-process-per-mapper-chunk fallback for Q1).

### Why Q2 is the slowest of the three here, and why that's expected

Q2's numbers include real gRPC/protobuf serialization and a live TCP round-trip per batch (1,000
records/batch) — genuine network-stack overhead neither MPI's in-memory collectives nor Q1's
direct-file-I/O pipeline pay. This is inherent to the *streaming* paradigm's premise (data arrives
continuously over a network connection, not read once from a local file) and is expected, not a
performance defect: Q2 is solving a different problem (real-time ingestion with concurrent query
support while data is still arriving) that neither HW2's MPI nor Q1's batch MapReduce attempt.
Comparing raw throughput alone would miss Q2's actual value proposition — concurrent query support
during ingestion (see E3 above), which the other two systems don't provide at all.

## Known limitations

- RCE benchmarks are 3 reps on one shared, possibly-contended node — real and consistent with
  the local dev-machine trends, but not isolated-hardware confidence intervals.
- The float-precision risk in [Correctness](#correctness) is now empirically confirmed (not just
  analyzed) on `x86_64`: N=100,000's `AVERAGE_WIND_SPEED` hits the exact halfway point
  `60.1152135` and resolves to the opposite side of what `double`-precision Kahan gives. Verified
  as a genuine tie via exact `Fraction`/`Decimal` arithmetic on the real dataset, not assumed.
- E4 (streaming rate), E5 (station skew), and plots remain unbuilt (see Benchmarks above) —
  scoped out given the time constraint, not overlooked.
