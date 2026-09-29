#!/usr/bin/env python3
"""dashboard.py -- CLI dashboard / query client for HW3 Section 2 Q2.

Usage:
    python3 dashboard.py SERVER [--interval 1.0] [--once] [--top-k 0]
                          [--no-clear] [--timeout 5]

Live mode (default) polls GetAnalytics every --interval seconds and
redraws an ingestion-progress header plus the current 19(+K)-line report.
--once issues a single query and prints ONLY the report lines to stdout
(nothing else -- "do not print debugging information as part of the
required output"), for use by correctness scripts and quick checks.
"""

import argparse
import sys
import time

import grpc

import weather_stream_pb2 as pb2
import weather_stream_pb2_grpc as pb2_grpc

CLEAR = "\033[H\033[2J"


def snapshot_to_lines(snap) -> list:
    if not snap.has_data:
        return ["(no data yet)"]
    lines = [
        f"TOTAL_MEASUREMENTS {snap.total_measurements}",
        f"AVERAGE_TEMPERATURE {snap.average_temperature:.6f}",
        f"MIN_TEMPERATURE {snap.min_temperature:.6f}",
        f"MAX_TEMPERATURE {snap.max_temperature:.6f}",
        f"AVERAGE_HUMIDITY {snap.average_humidity:.6f}",
        f"MIN_HUMIDITY {snap.min_humidity:.6f}",
        f"MAX_HUMIDITY {snap.max_humidity:.6f}",
        f"AVERAGE_PRESSURE {snap.average_pressure:.6f}",
        f"MIN_PRESSURE {snap.min_pressure:.6f}",
        f"MAX_PRESSURE {snap.max_pressure:.6f}",
        f"TOTAL_RAINFALL {snap.total_rainfall:.6f}",
        f"MAX_RAINFALL {snap.max_rainfall:.6f}",
        f"AVERAGE_WIND_SPEED {snap.average_wind_speed:.6f}",
        f"MAX_WIND_SPEED {snap.max_wind_speed:.6f}",
        f"EXTREME_TEMPERATURE_EVENTS {snap.extreme_temperature_events}",
        f"HOTTEST_MEASUREMENT {snap.hottest.temperature:.6f} {snap.hottest.station_id} "
        f"{snap.hottest.timestamp}",
        f"COLDEST_MEASUREMENT {snap.coldest.temperature:.6f} {snap.coldest.station_id} "
        f"{snap.coldest.timestamp}",
        f"BUSIEST_INTERVAL {snap.busiest_interval} {snap.busiest_interval_count}",
        "TOP_STATIONS",
    ]
    for st in snap.top_stations:
        lines.append(f"{st.station_id} {st.count} {st.average_temperature:.6f} {st.total_rainfall:.6f}")
    return lines


def render_header(snap, server: str, query_ms: float) -> list:
    status = snap.status
    pct = (100.0 * status.records_applied / status.expected_records) if status.expected_records else 0.0
    bar_width = 30
    filled = int(bar_width * pct / 100.0)
    bar = "#" * filled + "." * (bar_width - filled)
    lines = [
        f"HW3 Q2 . Streaming Weather Analytics   server={server}   "
        f"{time.strftime('%H:%M:%S')}   query {query_ms:.1f} ms",
        f"Ingestion: {status.records_applied}/{status.expected_records} applied "
        f"({pct:.1f}%) [{bar}]  backlog {status.records_received - status.records_applied}",
        f"Streams: {status.active_streams} active / {status.completed_streams} completed   "
        f"Workers: {status.num_workers} (partition={status.partition})   K={status.dataset_k}",
    ]
    for w in status.workers:
        lines.append(
            f"  w{w.worker_id} {w.records_applied} rec  queue {w.queue_depth}  busy {w.busy_s:.2f}s"
        )
    lines.append("-" * 65)
    return lines


def run_once(stub, top_k: int) -> int:
    snap = stub.GetAnalytics(pb2.AnalyticsRequest(top_k=top_k))
    if not snap.has_data:
        print("(no data ingested yet)", file=sys.stderr)
        return 3
    for line in snapshot_to_lines(snap):
        print(line)
    return 0


def run_live(stub, server: str, interval: float, top_k: int, no_clear: bool) -> int:
    try:
        while True:
            t0 = time.perf_counter()
            snap = stub.GetAnalytics(pb2.AnalyticsRequest(top_k=top_k))
            query_ms = (time.perf_counter() - t0) * 1000.0

            out = []
            if not no_clear and sys.stdout.isatty():
                out.append(CLEAR)
            out.extend(render_header(snap, server, query_ms))
            out.extend(snapshot_to_lines(snap))
            print("\n".join(out))
            sys.stdout.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("server", help="host:port of the coordinator")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--top-k", type=int, default=0)
    parser.add_argument("--no-clear", action="store_true")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()

    channel = grpc.insecure_channel(
        args.server,
        options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)],
    )
    try:
        grpc.channel_ready_future(channel).result(timeout=args.timeout)
    except grpc.FutureTimeoutError:
        print(f"ERROR: could not connect to {args.server} within {args.timeout}s", file=sys.stderr)
        sys.exit(2)
    stub = pb2_grpc.WeatherAnalyticsStub(channel)

    if args.once:
        sys.exit(run_once(stub, args.top_k))
    else:
        sys.exit(run_live(stub, args.server, args.interval, args.top_k, args.no_clear))


if __name__ == "__main__":
    main()
