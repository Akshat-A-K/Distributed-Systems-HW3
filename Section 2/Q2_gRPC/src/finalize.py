#!/usr/bin/env python3
"""finalize.py -- HW3 Section 2 Q1 final assembly step.

NOT a MapReduce step (see plan section 5): reads reducer.cpp's already-small
output -- the one already-finalized GLOBAL block, plus complete-but-unranked
STATION_<id> and INTERVAL_<id> rows -- and resolves the two statistics that
need a view across ALL reducer instances: the globally busiest interval and
the top-K stations. Then assembles the exact HW2 Q8 output.

It does not recompute any Shape-A statistic (count, averages, min/max,
extreme count, hottest/coldest): those 15 lines are already final when they
arrive here and are passed through verbatim, in the order reducer.cpp wrote
them (reducer.cpp already puts them in the exact required Q8 field order).

Usage:
    mapper | sort | reducer | finalize.py --k K > final_output.txt

K is the top-K cutoff from the original HW2 dataset's "N K S" header line.
Neither mapper.cpp nor reducer.cpp need K -- only this final top-K
truncation depends on it. It is required on the command line and is never
defaulted or invented by this script.
"""

import argparse
import sys


def parse_reducer_output(lines):
    """Splits reducer.cpp's output into (global_lines, station_rows, interval_rows).

    global_lines: the already-final Shape-A lines, in arrival order, kept as
        raw strings -- never recomputed (see module docstring).
    station_rows: {station_id_str: (count, sum_temp, sum_rainfall)}
    interval_rows: {interval_id_str: count}
    """
    global_lines = []
    station_rows = {}
    interval_rows = {}

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        if not line:
            continue

        if "\t" in line:
            key, value = line.split("\t", 1)
            if key.startswith("STATION_"):
                station_id = key[len("STATION_"):]
                count_str, sum_temp_str, sum_rain_str = value.split()
                station_rows[station_id] = (
                    int(count_str), float(sum_temp_str), float(sum_rain_str)
                )
            elif key.startswith("INTERVAL_"):
                interval_id = key[len("INTERVAL_"):]
                interval_rows[interval_id] = int(value.strip())
            else:
                raise ValueError(f"unrecognized tabbed key from reducer: {key!r}")
        else:
            # No tab -- one of reducer.cpp's already-final GLOBAL lines.
            global_lines.append(line)

    return global_lines, station_rows, interval_rows


def find_busiest_interval(interval_rows):
    """Max count; tie -> smaller interval_id (same rule as weather_seq.cpp).

    Ids are compared numerically, not as strings: Hadoop's shuffle sorts
    keys lexicographically ("INTERVAL_10" before "INTERVAL_2"), so string
    comparison here would pick the wrong winner on a tie.
    """
    busiest_id = None
    busiest_count = None
    for id_str, count in interval_rows.items():
        id_int = int(id_str)
        if (busiest_count is None or count > busiest_count or
                (count == busiest_count and id_int < int(busiest_id))):
            busiest_id = id_str
            busiest_count = count
    return busiest_id, busiest_count


def top_k_stations(station_rows, k):
    """Count descending, station_id ascending on ties -- same rule as
    weather_seq.cpp. Returns up to k (station_id, count, avg_temp, total_rainfall)."""
    rows = []
    for station_id, (count, sum_temp, sum_rain) in station_rows.items():
        avg_temp = sum_temp / count
        rows.append((station_id, count, avg_temp, sum_rain))

    rows.sort(key=lambda r: (-r[1], int(r[0])))
    return rows[:k]


def main():
    parser = argparse.ArgumentParser(
        description="Assemble the final HW2 Q8 output from reducer.cpp's output."
    )
    parser.add_argument(
        "--k", type=int, required=True,
        help="Top-K cutoff, from the original dataset's N K S header line.",
    )
    args = parser.parse_args()

    global_lines, station_rows, interval_rows = parse_reducer_output(sys.stdin)

    busiest_id, busiest_count = find_busiest_interval(interval_rows)
    top_stations = top_k_stations(station_rows, args.k)

    out = sys.stdout
    for line in global_lines:
        out.write(line + "\n")
    out.write(f"BUSIEST_INTERVAL {busiest_id} {busiest_count}\n")
    out.write("TOP_STATIONS\n")
    # 6 decimal digits per the course clarification for Q7/Q8 ("All
    # floating-point values in the output should be printed with exactly 6
    # digits after the decimal point").
    for station_id, count, avg_temp, total_rainfall in top_stations:
        out.write(f"{station_id} {count} {avg_temp:.6f} {total_rainfall:.6f}\n")


if __name__ == "__main__":
    main()
