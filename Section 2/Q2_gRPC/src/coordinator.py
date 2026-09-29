"""coordinator.py -- concurrency layer only (no gRPC). Wraps analytics_core's
ShardState/finalize_report in a pool of persistent worker threads: each
worker is a long-running "mapper" for its shard; a query is a live re-run of
reducer.cpp's merge step over snapshots of every shard.

Concurrency model (see plan section 3 for the full rationale):
  - One bounded Queue per worker; the caller (the gRPC ingestion handler)
    routes each record to exactly one worker, so each shard has exactly one
    writer and no write-write races are possible.
  - Each worker's shard is guarded by its own Lock, taken only by that
    worker's thread (to apply records) and by query() (to copy a snapshot).
    A query takes at most one worker's lock at a time -- never two at once,
    so deadlock is impossible -- and merges the copies outside any lock.
  - A snapshot is internally consistent (TOTAL == sum of station counts ==
    sum of interval counts) but is not guaranteed to be an exact prefix of
    the stream if workers happen to be at different points -- an accepted
    property (the spec asks for a "useful view of the current state," not
    point-in-time atomicity across shards), not a bug.
  - end_stream() uses a FIFO drain marker per worker queue as a barrier: by
    the time all markers have fired, every record submitted before this call
    has been applied, so a query issued after the ack sees all of it.
"""

import queue
import threading
import time
from typing import Dict, List, Optional, Tuple

from analytics_core import Report, ShardState, finalize_report

_STOP = object()

Record = Tuple[int, int, float, float, float, float, float]


class _DrainMarker:
    __slots__ = ("event",)

    def __init__(self):
        self.event = threading.Event()


class Worker:
    def __init__(self, worker_id: int, queue_capacity: int):
        self.worker_id = worker_id
        self.queue: "queue.Queue" = queue.Queue(maxsize=queue_capacity)
        self.lock = threading.Lock()
        self.state = ShardState()
        self.applied = 0
        self.busy_s = 0.0
        self.error: Optional[BaseException] = None
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"worker-{worker_id}")
        self._thread.start()

    def _run(self) -> None:
        while True:
            item = self.queue.get()
            if item is _STOP:
                return
            if isinstance(item, _DrainMarker):
                item.event.set()
                continue
            if self.error is not None:
                # Keep draining so a barrier waiting on this queue can never
                # hang; the handler surfaces the stored error separately.
                continue
            t0 = time.perf_counter()
            try:
                with self.lock:
                    for rec in item:
                        self.state.apply(*rec)
                    self.applied += len(item)
            except BaseException as exc:  # noqa: BLE001 -- must not kill the thread
                self.error = exc
            self.busy_s += time.perf_counter() - t0

    def submit(self, records: List[Record]) -> None:
        if records:
            self.queue.put(records)

    def drain_marker(self) -> _DrainMarker:
        marker = _DrainMarker()
        self.queue.put(marker)
        return marker

    def stop(self) -> None:
        self.queue.put(_STOP)
        self._thread.join(timeout=5.0)

    def snapshot(self):
        with self.lock:
            summary = self.state.summary()
            station_rows = self.state.station_rows()
            interval_rows = self.state.interval_rows()
        return summary, station_rows, interval_rows


class Coordinator:
    def __init__(self, num_workers: int = 4, partition: str = "station", queue_capacity: int = 64):
        if num_workers < 1:
            raise ValueError("num_workers must be >= 1")
        if partition not in ("station", "batch_rr"):
            raise ValueError(f"unknown partition mode: {partition!r}")
        self.num_workers = num_workers
        self.partition = partition
        self.queue_capacity = queue_capacity
        self.workers: List[Worker] = [Worker(i, queue_capacity) for i in range(num_workers)]

        self._admin_lock = threading.Lock()
        self._active_streams = 0
        self._completed_streams = 0
        self._expected_records = 0
        self._records_received = 0
        self._k: Optional[int] = None

    # ---- stream lifecycle ----

    def start_stream(self, n: int, k: int) -> None:
        with self._admin_lock:
            if self._k is not None and self._k != k:
                raise ValueError(
                    f"K mismatch: this stream declares K={k}, coordinator already has K={self._k} "
                    "(call Reset between streams that use a different K)"
                )
            self._k = k
            self._expected_records += n
            self._active_streams += 1

    def route_batch(self, records: List[Record], seq: int) -> None:
        if not records:
            return
        if self.partition == "batch_rr":
            self.workers[seq % self.num_workers].submit(records)
        else:  # "station": station_id % num_workers, disjoint shards per station
            buckets: List[List[Record]] = [[] for _ in range(self.num_workers)]
            for rec in records:
                buckets[rec[1] % self.num_workers].append(rec)
            for worker, bucket in zip(self.workers, buckets):
                worker.submit(bucket)
        with self._admin_lock:
            self._records_received += len(records)

    def end_stream(self) -> None:
        markers = [w.drain_marker() for w in self.workers]
        for marker in markers:
            marker.event.wait()
        errors = [w.error for w in self.workers if w.error is not None]
        with self._admin_lock:
            self._active_streams -= 1
            if not errors:
                self._completed_streams += 1
        if errors:
            raise errors[0]

    # ---- queries ----

    def query(self, top_k: int = 0) -> Report:
        partials = []
        station_rows_list = []
        interval_rows_list = []
        for worker in self.workers:
            summary, station_rows, interval_rows = worker.snapshot()
            partials.append(summary)
            station_rows_list.append(station_rows)
            interval_rows_list.append(interval_rows)
        with self._admin_lock:
            k = top_k if top_k > 0 else (self._k or 0)
        return finalize_report(partials, station_rows_list, interval_rows_list, k)

    def status(self) -> Dict:
        with self._admin_lock:
            records_received = self._records_received
            expected = self._expected_records
            active = self._active_streams
            completed = self._completed_streams
            k = self._k or 0
        applied_total = 0
        worker_status = []
        for worker in self.workers:
            applied = worker.applied
            qsize = worker.queue.qsize()
            applied_total += applied
            worker_status.append(
                {"worker_id": worker.worker_id, "records_applied": applied,
                 "queue_depth": qsize, "busy_s": worker.busy_s}
            )
        return {
            "records_received": records_received,
            "records_applied": applied_total,
            "expected_records": expected,
            "active_streams": active,
            "completed_streams": completed,
            "ingestion_complete": active == 0 and completed > 0,
            "num_workers": self.num_workers,
            "partition": self.partition,
            "k": k,
            "workers": worker_status,
        }

    # ---- reset ----

    def reset(self, num_workers: Optional[int] = None, partition: Optional[str] = None) -> Tuple[int, str]:
        if partition is not None and partition not in ("station", "batch_rr"):
            raise ValueError(f"unknown partition mode: {partition!r}")
        if num_workers is not None and num_workers < 1:
            raise ValueError("num_workers must be >= 1")
        with self._admin_lock:
            if self._active_streams > 0:
                raise RuntimeError("cannot Reset while a stream is active")
            new_num_workers = num_workers if num_workers else self.num_workers
            new_partition = partition if partition else self.partition
            for worker in self.workers:
                worker.stop()
            self.num_workers = new_num_workers
            self.partition = new_partition
            self.workers = [Worker(i, self.queue_capacity) for i in range(new_num_workers)]
            self._completed_streams = 0
            self._expected_records = 0
            self._records_received = 0
            self._k = None
            return new_num_workers, new_partition

    def shutdown(self) -> None:
        for worker in self.workers:
            worker.stop()
