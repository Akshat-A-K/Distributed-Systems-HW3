#!/bin/bash
# compare_correctness.sh -- reusable comparison between a Hadoop-pipeline
# final output and the weather_seq_reference (HW2) output for the same
# dataset. Byte-for-byte diff, deliberately: every test run so far this
# session (mapper.cpp, reducer.cpp, finalize.py, each independently
# verified) has produced an EXACT match against weather_seq_reference, not
# merely a within-tolerance one, so this does not invent a numeric
# tolerance for a discrepancy that has never actually been observed. HW2's
# own MPI-vs-sequential comparison needed a 0.011 tolerance for a real,
# observed reason (MPI's reduction-tree summation order); if a similar,
# genuinely observed floating-point discrepancy ever shows up here at
# larger scale, add a tolerance then -- not preemptively.
#
# Usage:
#   compare_correctness.sh <actual_output_file> <expected_output_file>
#
# Exit status: 0 if they match exactly, 1 if they differ (diff's own exit
# code), 2 on a usage/file error.

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <actual_output_file> <expected_output_file>" >&2
    exit 2
fi

actual="$1"
expected="$2"

for f in "$actual" "$expected"; do
    if [[ ! -f "$f" ]]; then
        echo "compare_correctness.sh: file not found: $f" >&2
        exit 2
    fi
done

if diff -u "$expected" "$actual"; then
    echo "PASS: $actual matches $expected exactly."
    exit 0
else
    status=$?
    echo "FAIL: $actual differs from $expected (diff above; expected on the left, actual on the right)." >&2
    exit "$status"
fi
