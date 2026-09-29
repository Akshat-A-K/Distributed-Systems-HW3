#!/usr/bin/env python3
"""server.py -- gRPC coordinator for HW3 Section 2 Q2.

Thin gRPC layer over coordinator.Coordinator: StreamRecords ingests a
client-streamed dataset, GetAnalytics answers live queries, Reset
reconfigures worker count/partition between runs. See coordinator.py for
the concurrency model and analytics_core.py for the ported HW2 Q8 logic.

Usage:
    python3 server.py [--host 0.0.0.0] [--port 50051] [--workers 4]
                       [--partition station|batch_rr] [--queue-capacity 64]
                       [--max-msg-mb 64]

On RCE: bind --host 0.0.0.0 and connect from other nodes via
<this-node-hostname>:<port>, never localhost (see rce_grpc_execution_guide.pdf).
"""

import argparse
import resource
import sys
import time
from concurrent import futures

import grpc

import weather_stream_pb2 as pb2
import weather_stream_pb2_grpc as pb2_grpc
from coordinator import Coordinator


def _cpu_times():
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime, r.ru_stime


class WeatherAnalyticsServicer(pb2_grpc.WeatherAnalyticsServicer):
    def __init__(self, coordinator: Coordinator):
        self.coordinator = coordinator

    def StreamRecords(self, request_iterator, context):
        # NOTE: context.abort() raises internally to terminate the RPC, so
        # none of these calls are wrapped in a broad try/except -- catching
        # that exception here would just make us call abort() a second time.
        # Only the coordinator calls that can raise a *domain* exception
        # (K mismatch, a worker error surfaced at drain time) get a narrow
        # try/except that turns it into a clean abort().
        t0 = time.perf_counter()
        cpu_u0, cpu_s0 = _cpu_times()
        header_seen = False
        records_received = 0

        for msg in request_iterator:
            kind = msg.WhichOneof("payload")
            if kind == "header":
                if header_seen:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                   "a second DatasetHeader was sent on the same stream")
                try:
                    self.coordinator.start_stream(msg.header.n, msg.header.k)
                except ValueError as exc:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
                header_seen = True
            elif kind == "batch":
                if not header_seen:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                   "first message on the stream must be a DatasetHeader")
                records = [
                    (r.timestamp, r.station_id, r.temperature, r.humidity,
                     r.pressure, r.rainfall, r.wind_speed)
                    for r in msg.batch.records
                ]
                self.coordinator.route_batch(records, msg.batch.seq)
                records_received += len(records)
            else:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "empty IngestMessage")

        if not header_seen:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "stream contained no DatasetHeader")

        try:
            self.coordinator.end_stream()
        except Exception as exc:  # noqa: BLE001 -- a worker's stored error, surfaced here
            context.abort(grpc.StatusCode.INTERNAL, f"ingestion failed while draining: {exc}")

        elapsed = time.perf_counter() - t0
        cpu_u1, cpu_s1 = _cpu_times()
        return pb2.IngestSummary(
            records_received=records_received,
            server_elapsed_s=elapsed,
            cpu_user_s=cpu_u1 - cpu_u0,
            cpu_sys_s=cpu_s1 - cpu_s0,
        )

    def GetAnalytics(self, request, context):
        t0 = time.perf_counter()
        report = self.coordinator.query(top_k=request.top_k)
        compute_ms = (time.perf_counter() - t0) * 1000.0

        status = self.coordinator.status()
        status_msg = pb2.SystemStatus(
            records_received=status["records_received"],
            records_applied=status["records_applied"],
            expected_records=status["expected_records"],
            active_streams=status["active_streams"],
            completed_streams=status["completed_streams"],
            ingestion_complete=status["ingestion_complete"],
            num_workers=status["num_workers"],
            partition=status["partition"],
            dataset_k=status["k"],
            workers=[
                pb2.WorkerStatus(
                    worker_id=w["worker_id"], records_applied=w["records_applied"],
                    queue_depth=w["queue_depth"], busy_s=w["busy_s"],
                )
                for w in status["workers"]
            ],
            query_compute_ms=compute_ms,
        )

        if not report.has_data:
            return pb2.AnalyticsSnapshot(has_data=False, status=status_msg)

        return pb2.AnalyticsSnapshot(
            has_data=True,
            total_measurements=report.total_measurements,
            average_temperature=report.average_temperature,
            min_temperature=report.min_temperature,
            max_temperature=report.max_temperature,
            average_humidity=report.average_humidity,
            min_humidity=report.min_humidity,
            max_humidity=report.max_humidity,
            average_pressure=report.average_pressure,
            min_pressure=report.min_pressure,
            max_pressure=report.max_pressure,
            total_rainfall=report.total_rainfall,
            max_rainfall=report.max_rainfall,
            average_wind_speed=report.average_wind_speed,
            max_wind_speed=report.max_wind_speed,
            extreme_temperature_events=report.extreme_temperature_events,
            hottest=pb2.Measurement(temperature=report.hottest_temp,
                                     station_id=report.hottest_station, timestamp=report.hottest_ts),
            coldest=pb2.Measurement(temperature=report.coldest_temp,
                                     station_id=report.coldest_station, timestamp=report.coldest_ts),
            busiest_interval=report.busiest_interval if report.busiest_interval is not None else -1,
            busiest_interval_count=report.busiest_count if report.busiest_count is not None else 0,
            top_stations=[
                pb2.StationStat(station_id=sid, count=count, average_temperature=avg_temp,
                                 total_rainfall=total_rain)
                for sid, count, avg_temp, total_rain in report.top_stations
            ],
            status=status_msg,
        )

    def Reset(self, request, context):
        num_workers = request.num_workers if request.num_workers > 0 else None
        partition = request.partition if request.partition else None
        try:
            new_w, new_p = self.coordinator.reset(num_workers=num_workers, partition=partition)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except RuntimeError as exc:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(exc))
        return pb2.ResetResponse(num_workers=new_w, partition=new_p)


def serve(host: str, port: int, workers: int, partition: str, queue_capacity: int,
          max_msg_mb: int, rpc_threads: int) -> None:
    coordinator = Coordinator(num_workers=workers, partition=partition, queue_capacity=queue_capacity)
    servicer = WeatherAnalyticsServicer(coordinator)

    max_bytes = max_msg_mb * 1024 * 1024
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=rpc_threads),
        options=[
            ("grpc.max_send_message_length", max_bytes),
            ("grpc.max_receive_message_length", max_bytes),
            ("grpc.so_reuseport", 0),
        ],
    )
    pb2_grpc.add_WeatherAnalyticsServicer_to_server(servicer, server)
    address = f"{host}:{port}"
    bound_port = server.add_insecure_port(address)
    if bound_port == 0:
        print(f"ERROR: could not bind {address} (port in use?)", file=sys.stderr)
        sys.exit(1)

    server.start()
    print(f"WeatherAnalytics server listening on {address} "
          f"(workers={workers} partition={partition})", file=sys.stderr)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        coordinator.shutdown()
        server.stop(grace=2.0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--partition", choices=("station", "batch_rr"), default="station")
    parser.add_argument("--queue-capacity", type=int, default=64)
    parser.add_argument("--max-msg-mb", type=int, default=64)
    parser.add_argument("--rpc-threads", type=int, default=32,
                         help="Must exceed concurrent streams + query clients; a streaming "
                              "RPC holds a thread for its whole life.")
    args = parser.parse_args()
    serve(args.host, args.port, args.workers, args.partition, args.queue_capacity,
          args.max_msg_mb, args.rpc_threads)


if __name__ == "__main__":
    main()
