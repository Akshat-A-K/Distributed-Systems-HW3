"""analytics_core.py -- gRPC-independent, thread-independent HW2 Q8 analytics.

Ported line-by-line from the already-verified Q1 Hadoop pipeline
(../Q1_MapReduce/src/mapper.cpp, reducer.cpp, common/kahan.hpp):

  - KahanSum          <- kahan.hpp's reset/add/value trio (Python `float` is
                         a C `double`; the C++ side additionally uses `long
                         double` internally -- see README's precision note).
  - ShardState        <- mapper.cpp's per-record loop body + end-of-split
                         GLOBAL/STATION_*/INTERVAL_* emission. One ShardState
                         is one worker's persistent "mapper" for its shard.
  - GlobalCombiner    <- reducer.cpp's GlobalCombiner::merge_one/print_final.
  - combine_station_interval <- reducer.cpp's StationCombiner/IntervalCombiner.

finalize.py's find_busiest_interval/top_k_stations are imported unchanged
(same file, copied into this project) rather than reimplemented, so the
tie-break rules can never drift between Q1 and Q2.
"""

import sys
from typing import Dict, List, NamedTuple, Optional, Tuple

from finalize import find_busiest_interval, top_k_stations

StationEntry = List  # [count: int, sum_temp: KahanSum, sum_rainfall: KahanSum]


class KahanSum:
    __slots__ = ("sum", "c")

    def __init__(self):
        self.sum = 0.0
        self.c = 0.0

    def add(self, x: float) -> None:
        y = x - self.c
        t = self.sum + y
        self.c = (t - self.sum) - y
        self.sum = t

    def value(self) -> float:
        return self.sum


def _hottest_key(temp: float, ts: int, station: int):
    # Max temp; tie -> smaller ts; tie -> smaller station. Smallest tuple wins.
    return (-temp, ts, station)


def _coldest_key(temp: float, ts: int, station: int):
    # Min temp; tie -> smaller ts; tie -> smaller station. Smallest tuple wins.
    return (temp, ts, station)


class GlobalPartial(NamedTuple):
    """Mirrors mapper.cpp's 21-field GLOBAL line, in the same field order."""
    count: int
    sum_temp: float
    sum_humidity: float
    sum_pressure: float
    sum_rainfall: float
    sum_wind: float
    min_temp: float
    max_temp: float
    min_humidity: float
    max_humidity: float
    min_pressure: float
    max_pressure: float
    max_rainfall: float
    max_wind: float
    extreme_count: int
    hottest_temp: float
    hottest_ts: int
    hottest_station: int
    coldest_temp: float
    coldest_ts: int
    coldest_station: int


class ShardState:
    """One worker's local accumulators -- port of mapper.cpp's loop body."""

    __slots__ = (
        "count", "sum_temp", "sum_humidity", "sum_pressure", "sum_rainfall", "sum_wind",
        "min_temp", "max_temp", "min_humidity", "max_humidity", "min_pressure", "max_pressure",
        "max_rainfall", "max_wind", "extreme_count",
        "hottest_key", "hottest_temp", "hottest_ts", "hottest_station",
        "coldest_key", "coldest_temp", "coldest_ts", "coldest_station",
        "stations", "intervals",
    )

    def __init__(self):
        self.count = 0
        self.sum_temp = KahanSum()
        self.sum_humidity = KahanSum()
        self.sum_pressure = KahanSum()
        self.sum_rainfall = KahanSum()
        self.sum_wind = KahanSum()
        self.min_temp = self.max_temp = 0.0
        self.min_humidity = self.max_humidity = 0.0
        self.min_pressure = self.max_pressure = 0.0
        self.max_rainfall = 0.0
        self.max_wind = 0.0
        self.extreme_count = 0
        self.hottest_key = None
        self.hottest_temp = 0.0
        self.hottest_ts = 0
        self.hottest_station = 0
        self.coldest_key = None
        self.coldest_temp = 0.0
        self.coldest_ts = 0
        self.coldest_station = 0
        self.stations: Dict[int, StationEntry] = {}
        self.intervals: Dict[int, int] = {}

    def apply(self, ts: int, station_id: int, temp: float, hum: float, pres: float,
              rain: float, wind: float) -> None:
        self.count += 1
        self.sum_temp.add(temp)
        self.sum_humidity.add(hum)
        self.sum_pressure.add(pres)
        self.sum_rainfall.add(rain)
        self.sum_wind.add(wind)

        if self.count == 1:
            self.min_temp = self.max_temp = temp
            self.min_humidity = self.max_humidity = hum
            self.min_pressure = self.max_pressure = pres
            self.max_rainfall = rain
            self.max_wind = wind
        else:
            if temp < self.min_temp:
                self.min_temp = temp
            if temp > self.max_temp:
                self.max_temp = temp
            if hum < self.min_humidity:
                self.min_humidity = hum
            if hum > self.max_humidity:
                self.max_humidity = hum
            if pres < self.min_pressure:
                self.min_pressure = pres
            if pres > self.max_pressure:
                self.max_pressure = pres
            if rain > self.max_rainfall:
                self.max_rainfall = rain
            if wind > self.max_wind:
                self.max_wind = wind

        if temp >= 40.0 or temp <= 0.0:
            self.extreme_count += 1

        hk = _hottest_key(temp, ts, station_id)
        if self.hottest_key is None or hk < self.hottest_key:
            self.hottest_key = hk
            self.hottest_temp, self.hottest_ts, self.hottest_station = temp, ts, station_id

        ck = _coldest_key(temp, ts, station_id)
        if self.coldest_key is None or ck < self.coldest_key:
            self.coldest_key = ck
            self.coldest_temp, self.coldest_ts, self.coldest_station = temp, ts, station_id

        entry = self.stations.get(station_id)
        if entry is None:
            entry = [0, KahanSum(), KahanSum()]
            self.stations[station_id] = entry
        entry[0] += 1
        entry[1].add(temp)
        entry[2].add(rain)

        # ts // 60 matches C++'s ts / 60 (integer division) because every
        # timestamp in this dataset is non-negative (generate.cpp draws from
        # [1700000000, 1700086400]); Python floor-division and C++
        # truncation-toward-zero only differ for negative operands.
        interval_id = ts // 60
        self.intervals[interval_id] = self.intervals.get(interval_id, 0) + 1

    def summary(self) -> Optional[GlobalPartial]:
        """Port of mapper.cpp's EOF emission: an empty shard contributes
        nothing (no meaningful min/max/hottest/coldest to report)."""
        if self.count == 0:
            return None
        return GlobalPartial(
            count=self.count,
            sum_temp=self.sum_temp.value(), sum_humidity=self.sum_humidity.value(),
            sum_pressure=self.sum_pressure.value(), sum_rainfall=self.sum_rainfall.value(),
            sum_wind=self.sum_wind.value(),
            min_temp=self.min_temp, max_temp=self.max_temp,
            min_humidity=self.min_humidity, max_humidity=self.max_humidity,
            min_pressure=self.min_pressure, max_pressure=self.max_pressure,
            max_rainfall=self.max_rainfall, max_wind=self.max_wind,
            extreme_count=self.extreme_count,
            hottest_temp=self.hottest_temp, hottest_ts=self.hottest_ts,
            hottest_station=self.hottest_station,
            coldest_temp=self.coldest_temp, coldest_ts=self.coldest_ts,
            coldest_station=self.coldest_station,
        )

    def station_rows(self) -> Dict[int, Tuple[int, float, float]]:
        return {sid: (e[0], e[1].value(), e[2].value()) for sid, e in self.stations.items()}

    def interval_rows(self) -> Dict[int, int]:
        return dict(self.intervals)


class GlobalCombiner:
    """Merges GlobalPartial summaries from multiple shards -- port of
    reducer.cpp's GlobalCombiner::merge_one, generalized from "one partial
    per mapper" to "one partial per worker snapshot"."""

    def __init__(self):
        self.count = 0
        self.sum_temp = KahanSum()
        self.sum_humidity = KahanSum()
        self.sum_pressure = KahanSum()
        self.sum_rainfall = KahanSum()
        self.sum_wind = KahanSum()
        self.have_bounds = False
        self.min_temp = self.max_temp = 0.0
        self.min_humidity = self.max_humidity = 0.0
        self.min_pressure = self.max_pressure = 0.0
        self.max_rainfall = 0.0
        self.max_wind = 0.0
        self.extreme_count = 0
        self.hottest_key = None
        self.hottest_temp = 0.0
        self.hottest_ts = 0
        self.hottest_station = 0
        self.coldest_key = None
        self.coldest_temp = 0.0
        self.coldest_ts = 0
        self.coldest_station = 0

    def merge_one(self, p: GlobalPartial) -> None:
        self.count += p.count
        self.sum_temp.add(p.sum_temp)
        self.sum_humidity.add(p.sum_humidity)
        self.sum_pressure.add(p.sum_pressure)
        self.sum_rainfall.add(p.sum_rainfall)
        self.sum_wind.add(p.sum_wind)

        if not self.have_bounds:
            self.min_temp, self.max_temp = p.min_temp, p.max_temp
            self.min_humidity, self.max_humidity = p.min_humidity, p.max_humidity
            self.min_pressure, self.max_pressure = p.min_pressure, p.max_pressure
            self.max_rainfall, self.max_wind = p.max_rainfall, p.max_wind
            self.have_bounds = True
        else:
            if p.min_temp < self.min_temp:
                self.min_temp = p.min_temp
            if p.max_temp > self.max_temp:
                self.max_temp = p.max_temp
            if p.min_humidity < self.min_humidity:
                self.min_humidity = p.min_humidity
            if p.max_humidity > self.max_humidity:
                self.max_humidity = p.max_humidity
            if p.min_pressure < self.min_pressure:
                self.min_pressure = p.min_pressure
            if p.max_pressure > self.max_pressure:
                self.max_pressure = p.max_pressure
            if p.max_rainfall > self.max_rainfall:
                self.max_rainfall = p.max_rainfall
            if p.max_wind > self.max_wind:
                self.max_wind = p.max_wind

        self.extreme_count += p.extreme_count

        hk = _hottest_key(p.hottest_temp, p.hottest_ts, p.hottest_station)
        if self.hottest_key is None or hk < self.hottest_key:
            self.hottest_key = hk
            self.hottest_temp = p.hottest_temp
            self.hottest_ts = p.hottest_ts
            self.hottest_station = p.hottest_station

        ck = _coldest_key(p.coldest_temp, p.coldest_ts, p.coldest_station)
        if self.coldest_key is None or ck < self.coldest_key:
            self.coldest_key = ck
            self.coldest_temp = p.coldest_temp
            self.coldest_ts = p.coldest_ts
            self.coldest_station = p.coldest_station


def combine_station_interval(
    station_rows_list: List[Dict[int, Tuple[int, float, float]]],
    interval_rows_list: List[Dict[int, int]],
):
    """Port of reducer.cpp's StationCombiner/IntervalCombiner: sums complete
    per-shard station/interval rows into complete global totals. Needed even
    under station-id sharding, because one interval bucket (60-second window)
    is populated by many different stations, and therefore many workers."""
    stations: Dict[int, StationEntry] = {}
    for rows in station_rows_list:
        for sid, (c, st, sr) in rows.items():
            entry = stations.get(sid)
            if entry is None:
                entry = [0, KahanSum(), KahanSum()]
                stations[sid] = entry
            entry[0] += c
            entry[1].add(st)
            entry[2].add(sr)

    intervals: Dict[int, int] = {}
    for rows in interval_rows_list:
        for iid, c in rows.items():
            intervals[iid] = intervals.get(iid, 0) + c

    station_out = {sid: (e[0], e[1].value(), e[2].value()) for sid, e in stations.items()}
    return station_out, intervals


class Report(NamedTuple):
    has_data: bool
    total_measurements: int = 0
    average_temperature: float = 0.0
    min_temperature: float = 0.0
    max_temperature: float = 0.0
    average_humidity: float = 0.0
    min_humidity: float = 0.0
    max_humidity: float = 0.0
    average_pressure: float = 0.0
    min_pressure: float = 0.0
    max_pressure: float = 0.0
    total_rainfall: float = 0.0
    max_rainfall: float = 0.0
    average_wind_speed: float = 0.0
    max_wind_speed: float = 0.0
    extreme_temperature_events: int = 0
    hottest_temp: float = 0.0
    hottest_station: int = 0
    hottest_ts: int = 0
    coldest_temp: float = 0.0
    coldest_station: int = 0
    coldest_ts: int = 0
    busiest_interval: Optional[int] = None
    busiest_count: Optional[int] = None
    top_stations: tuple = ()  # (station_id: int, count: int, avg_temp: float, total_rainfall: float)


def finalize_report(
    partials: List[Optional[GlobalPartial]],
    station_rows_list: List[Dict[int, Tuple[int, float, float]]],
    interval_rows_list: List[Dict[int, int]],
    k: int,
) -> Report:
    """The query-time combine step: merges every shard's snapshot the same
    way reducer.cpp merges mapper partials, then resolves busiest-interval
    and top-K stations via finalize.py's unmodified tie-break functions."""
    gc = GlobalCombiner()
    any_data = False
    for p in partials:
        if p is None:
            continue
        any_data = True
        gc.merge_one(p)

    if not any_data:
        return Report(has_data=False)

    stations, intervals = combine_station_interval(station_rows_list, interval_rows_list)

    interval_rows_str = {str(i): c for i, c in intervals.items()}
    busiest_id_str, busiest_count = find_busiest_interval(interval_rows_str)
    busiest_id = int(busiest_id_str) if busiest_id_str is not None else None

    station_rows_str = {str(sid): v for sid, v in stations.items()}
    top = top_k_stations(station_rows_str, k)
    top_stations = tuple(
        (int(sid), count, avg_temp, total_rain) for sid, count, avg_temp, total_rain in top
    )

    return Report(
        has_data=True,
        total_measurements=gc.count,
        average_temperature=gc.sum_temp.value() / gc.count,
        min_temperature=gc.min_temp,
        max_temperature=gc.max_temp,
        average_humidity=gc.sum_humidity.value() / gc.count,
        min_humidity=gc.min_humidity,
        max_humidity=gc.max_humidity,
        average_pressure=gc.sum_pressure.value() / gc.count,
        min_pressure=gc.min_pressure,
        max_pressure=gc.max_pressure,
        total_rainfall=gc.sum_rainfall.value(),
        max_rainfall=gc.max_rainfall,
        average_wind_speed=gc.sum_wind.value() / gc.count,
        max_wind_speed=gc.max_wind,
        extreme_temperature_events=gc.extreme_count,
        hottest_temp=gc.hottest_temp, hottest_station=gc.hottest_station, hottest_ts=gc.hottest_ts,
        coldest_temp=gc.coldest_temp, coldest_station=gc.coldest_station, coldest_ts=gc.coldest_ts,
        busiest_interval=busiest_id, busiest_count=busiest_count,
        top_stations=top_stations,
    )


def format_report(r: Report) -> str:
    """Same field order/precision as weather_seq.cpp / reducer.cpp+finalize.py
    (6 decimal digits, per the instructor's Q7/Q8 clarification)."""
    if not r.has_data:
        return ""
    lines = [
        f"TOTAL_MEASUREMENTS {r.total_measurements}",
        f"AVERAGE_TEMPERATURE {r.average_temperature:.6f}",
        f"MIN_TEMPERATURE {r.min_temperature:.6f}",
        f"MAX_TEMPERATURE {r.max_temperature:.6f}",
        f"AVERAGE_HUMIDITY {r.average_humidity:.6f}",
        f"MIN_HUMIDITY {r.min_humidity:.6f}",
        f"MAX_HUMIDITY {r.max_humidity:.6f}",
        f"AVERAGE_PRESSURE {r.average_pressure:.6f}",
        f"MIN_PRESSURE {r.min_pressure:.6f}",
        f"MAX_PRESSURE {r.max_pressure:.6f}",
        f"TOTAL_RAINFALL {r.total_rainfall:.6f}",
        f"MAX_RAINFALL {r.max_rainfall:.6f}",
        f"AVERAGE_WIND_SPEED {r.average_wind_speed:.6f}",
        f"MAX_WIND_SPEED {r.max_wind_speed:.6f}",
        f"EXTREME_TEMPERATURE_EVENTS {r.extreme_temperature_events}",
        f"HOTTEST_MEASUREMENT {r.hottest_temp:.6f} {r.hottest_station} {r.hottest_ts}",
        f"COLDEST_MEASUREMENT {r.coldest_temp:.6f} {r.coldest_station} {r.coldest_ts}",
        f"BUSIEST_INTERVAL {r.busiest_interval} {r.busiest_count}",
        "TOP_STATIONS",
    ]
    for sid, count, avg_temp, total_rain in r.top_stations:
        lines.append(f"{sid} {count} {avg_temp:.6f} {total_rain:.6f}")
    return "\n".join(lines) + "\n"


def run_sequential(dataset_path: str) -> str:
    """Pure-Python, no-gRPC, no-thread sequential run -- an intermediate
    correctness checkpoint between the C++ oracle and the full streaming
    system (see README's three-way comparison)."""
    import dataset_io

    header, records = dataset_io.read_dataset(dataset_path)
    shard = ShardState()
    for ts, sid, temp, hum, pres, rain, wind in records:
        shard.apply(ts, sid, temp, hum, pres, rain, wind)
    report = finalize_report(
        [shard.summary()], [shard.station_rows()], [shard.interval_rows()], header.k
    )
    return format_report(report)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Pure-Python sequential HW2 Q8 analytics (no gRPC, no threads)."
    )
    parser.add_argument("--sequential", metavar="DATASET", required=True)
    args = parser.parse_args()
    sys.stdout.write(run_sequential(args.sequential))
