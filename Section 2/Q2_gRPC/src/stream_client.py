#!/usr/bin/env python3
"""stream_client.py -- replays a pre-generated dataset to the coordinator as
a gRPC client-stream, instead of submitting it as one batch.

Usage:
    python3 stream_client.py SERVER DATASET [--batch-size 1000] [--rate 0]
                              [--preload] [--max-records M] [--json]
                              [--connect-timeout 10]

SERVER is host:port (on RCE: the server's node hostname, never "localhost"
across nodes -- see rce_grpc_execution_guide.pdf). --batch-size is the
streaming "granularity" knob (1 = per-record streaming; large = bulk).
--rate is target records/sec; 0 (default) sends as fast as flow control
allows. --preload parses the whole dataset and builds every IngestMessage
before starting the timer, so benchmark numbers measure the server, not
this (single-threaded) Python client's own parsing/serialization cost.
"""

import argparse
import json
import sys
import time

import grpc

import dataset_io
import weather_stream_pb2 as pb2
import weather_stream_pb2_grpc as pb2_grpc


def _make_batches(records, batch_size, max_records=None):
    batch = []
    seq = 0
    sent = 0
    for rec in records:
        batch.append(rec)
        if len(batch) >= batch_size:
            yield seq, batch
            sent += len(batch)
            seq += 1
            batch = []
            if max_records is not None and sent >= max_records:
                return
    if batch:
        yield seq, batch


def _batch_to_message(seq, batch):
    return pb2.IngestMessage(batch=pb2.RecordBatch(
        seq=seq,
        records=[
            pb2.Record(timestamp=ts, station_id=sid, temperature=temp, humidity=hum,
                       pressure=pres, rainfall=rain, wind_speed=wind)
            for (ts, sid, temp, hum, pres, rain, wind) in batch
        ],
    ))


def _lazy_generator(header, records, batch_size, rate, max_records, progress):
    yield pb2.IngestMessage(header=pb2.DatasetHeader(n=header.n, k=header.k, s=header.s))
    start = time.perf_counter()
    sent = 0
    for seq, batch in _make_batches(records, batch_size, max_records):
        if rate and rate > 0:
            target = start + (sent + len(batch)) / rate
            now = time.perf_counter()
            if target > now:
                time.sleep(target - now)
        yield _batch_to_message(seq, batch)
        sent += len(batch)
        if progress and sent % max(batch_size * 50, 50000) < batch_size:
            print(f"  ... sent {sent} records", file=sys.stderr)


def _preloaded_generator(messages):
    for msg in messages:
        yield msg


def run(server: str, dataset_path: str, batch_size: int, rate: float, preload: bool,
        max_records, connect_timeout: float, as_json: bool, quiet: bool) -> int:
    header, records = dataset_io.read_dataset(dataset_path)
    if max_records is not None:
        records = records[:max_records]

    channel_options = [
        ("grpc.max_send_message_length", 64 * 1024 * 1024),
        ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ("grpc.enable_http_proxy", 0),
    ]
    channel = grpc.insecure_channel(server, options=channel_options)
    try:
        grpc.channel_ready_future(channel).result(timeout=connect_timeout)
    except grpc.FutureTimeoutError:
        print(f"ERROR: could not connect to {server} within {connect_timeout}s", file=sys.stderr)
        return 2
    stub = pb2_grpc.WeatherAnalyticsStub(channel)

    if preload:
        messages = [pb2.IngestMessage(header=pb2.DatasetHeader(n=header.n, k=header.k, s=header.s))]
        for seq, batch in _make_batches(records, batch_size):
            messages.append(_batch_to_message(seq, batch))
        request_iter = _preloaded_generator(messages)
    else:
        request_iter = _lazy_generator(header, records, batch_size, rate, None, not quiet)

    t_start = time.perf_counter()
    try:
        summary = stub.StreamRecords(request_iter)
    except grpc.RpcError as exc:
        print(f"ERROR: StreamRecords failed: {exc.code()} {exc.details()}", file=sys.stderr)
        return 3
    t_ack = time.perf_counter()

    end_to_end_s = t_ack - t_start
    throughput = summary.records_received / end_to_end_s if end_to_end_s > 0 else float("inf")

    result = {
        "server": server, "dataset": dataset_path, "n": header.n,
        "batch_size": batch_size, "rate": rate, "preload": preload,
        "records_received": summary.records_received,
        "client_end_to_end_s": end_to_end_s,
        "server_elapsed_s": summary.server_elapsed_s,
        "server_cpu_user_s": summary.cpu_user_s,
        "server_cpu_sys_s": summary.cpu_sys_s,
        "throughput_rec_per_s": throughput,
    }
    if as_json:
        print(json.dumps(result))
    else:
        print(f"Streamed {summary.records_received} records to {server} "
              f"(batch_size={batch_size} rate={rate or 'unlimited'} preload={preload})")
        print(f"  client end-to-end: {end_to_end_s:.4f}s  throughput: {throughput:.1f} rec/s")
        print(f"  server elapsed:    {summary.server_elapsed_s:.4f}s  "
              f"cpu(user+sys): {summary.cpu_user_s + summary.cpu_sys_s:.4f}s")

    channel.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("server", help="host:port of the coordinator")
    parser.add_argument("dataset", help="path to a generate.cpp-format dataset file")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=0.0, help="target records/sec, 0=unlimited")
    parser.add_argument("--preload", action="store_true")
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--connect-timeout", type=float, default=10.0)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    sys.exit(run(args.server, args.dataset, args.batch_size, args.rate, args.preload,
                 args.max_records, args.connect_timeout, args.json, args.quiet))


if __name__ == "__main__":
    main()
