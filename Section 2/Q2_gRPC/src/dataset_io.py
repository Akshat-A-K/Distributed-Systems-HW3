"""dataset_io.py -- reads generate.cpp's dataset file format ("N K S" header
line, then N records of "timestamp station_id temperature humidity pressure
rainfall wind_speed"), shared by the sequential mode, the streaming client,
and the correctness scripts so the parsing rule lives in exactly one place.
"""

from typing import Iterator, NamedTuple, TextIO, Tuple

Record = Tuple[int, int, float, float, float, float, float]


class Header(NamedTuple):
    n: int
    k: int
    s: int


def read_header(f: TextIO) -> Header:
    n, k, s = f.readline().split()
    return Header(int(n), int(k), int(s))


def iter_records(f: TextIO) -> Iterator[Record]:
    for line in f:
        line = line.strip()
        if not line:
            continue
        ts, sid, temp, hum, pres, rain, wind = line.split()
        yield (int(ts), int(sid), float(temp), float(hum), float(pres), float(rain), float(wind))


def read_dataset(path: str):
    """Loads the whole file into memory as (Header, list[Record]).

    Fine for correctness datasets (small); the streaming client reads
    lazily via iter_records instead so multi-million-record benchmark
    datasets aren't held in memory twice.
    """
    with open(path) as f:
        header = read_header(f)
        records = list(iter_records(f))
    if len(records) != header.n:
        raise ValueError(f"header declared N={header.n} but parsed {len(records)} records in {path}")
    return header, records
