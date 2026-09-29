"""Concurrency smoke test for coordinator.Coordinator: streams a dataset in
while other threads query concurrently, checking invariants on every
snapshot and that the final result matches the pure-Python sequential mode
(analytics_core.run_sequential) -- independent of worker count/partition.

Run with: python3 -m unittest tests.test_coordinator_concurrency -v
(from Q2_gRPC/, with src/ on sys.path -- see the sys.path insert below).
"""

import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import dataset_io  # noqa: E402
from analytics_core import format_report, run_sequential  # noqa: E402
from coordinator import Coordinator  # noqa: E402

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def stream_dataset(coord: Coordinator, path: str, batch_size: int = 137) -> None:
    header, records = dataset_io.read_dataset(path)
    coord.start_stream(header.n, header.k)
    batch: list = []
    seq = 0
    for rec in records:
        batch.append(rec)
        if len(batch) >= batch_size:
            coord.route_batch(batch, seq)
            seq += 1
            batch = []
    if batch:
        coord.route_batch(batch, seq)
    coord.end_stream()


class QueryProber(threading.Thread):
    """Queries in a loop until stopped; records any invariant violation."""

    def __init__(self, coord: Coordinator):
        super().__init__(daemon=True)
        self.coord = coord
        # Named _stop_event, not _stop: threading.Thread has its own private
        # _stop() method that join() relies on internally, and overriding it
        # with an attribute of the same name breaks join().
        self._stop_event = threading.Event()
        self.violations: list = []
        self.last_total = 0
        self.queries = 0

    def run(self) -> None:
        while not self._stop_event.is_set():
            report = self.coord.query()
            self.queries += 1
            if report.has_data:
                status = self.coord.status()
                station_sum = None  # not exposed directly; TOTAL vs applied is the cross-check below
                if report.total_measurements < self.last_total:
                    self.violations.append(
                        f"TOTAL decreased: {self.last_total} -> {report.total_measurements}"
                    )
                self.last_total = report.total_measurements
                if report.total_measurements > status["records_applied"]:
                    self.violations.append(
                        f"report total {report.total_measurements} exceeds applied "
                        f"{status['records_applied']}"
                    )
            time.sleep(0.001)

    def stop(self) -> None:
        self._stop_event.set()


class CoordinatorConcurrencyTest(unittest.TestCase):
    DATASET = os.path.join(DATA_DIR, "generated", "gen_n100000_k5_s100_seed42.txt")

    def setUp(self) -> None:
        if not os.path.exists(self.DATASET):
            self.skipTest(f"{self.DATASET} not generated yet")
        self.expected_output = run_sequential(self.DATASET)

    def _run_one(self, num_workers: int, partition: str) -> None:
        coord = Coordinator(num_workers=num_workers, partition=partition, queue_capacity=8)
        probers = [QueryProber(coord) for _ in range(4)]
        for p in probers:
            p.start()

        stream_dataset(coord, self.DATASET)

        for p in probers:
            p.stop()
        for p in probers:
            p.join(timeout=5.0)

        for p in probers:
            self.assertEqual(p.violations, [], f"W={num_workers} partition={partition}: {p.violations}")
            self.assertGreater(p.queries, 0, "prober never got a chance to query")

        final = coord.query()
        actual_output = format_report(final)
        self.assertEqual(
            actual_output, self.expected_output,
            f"W={num_workers} partition={partition}: final streamed output diverged from sequential",
        )

        status = coord.status()
        self.assertEqual(status["records_applied"], status["records_received"])
        coord.shutdown()

    def test_station_partition_various_worker_counts(self) -> None:
        for w in (1, 2, 4, 8):
            with self.subTest(workers=w):
                self._run_one(w, "station")

    def test_batch_rr_partition_various_worker_counts(self) -> None:
        for w in (1, 2, 4, 8):
            with self.subTest(workers=w):
                self._run_one(w, "batch_rr")

    def test_reset_rejected_while_stream_active(self) -> None:
        coord = Coordinator(num_workers=2)
        header, records = dataset_io.read_dataset(self.DATASET)
        coord.start_stream(header.n, header.k)
        with self.assertRaises(RuntimeError):
            coord.reset()
        coord.route_batch(records, 0)
        coord.end_stream()
        coord.shutdown()

    def test_k_mismatch_rejected(self) -> None:
        coord = Coordinator(num_workers=2)
        coord.start_stream(n=10, k=5)
        with self.assertRaises(ValueError):
            coord.start_stream(n=10, k=3)
        coord.end_stream()
        coord.shutdown()


if __name__ == "__main__":
    unittest.main()
