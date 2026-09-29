#!/usr/bin/env python3
"""diagnose_mismatch.py -- classifies a correctness mismatch between two
HW2 Q8 report files (expected vs. actual) instead of just failing silently.

Per-line classification:
  STRUCTURAL  -- different keyword, different field count, or a line only
                 one side has. Always a real bug.
  INTEGER     -- an integer field differs. Always a real bug.
  FLOAT_MINOR -- every differing float field differs by at most one unit in
                 the 6th decimal (1e-6). Consistent with the documented
                 double-vs-long-double Kahan precision risk (see README) --
                 worth a second look, not automatically a bug.
  FLOAT_MAJOR -- a float field differs by more than that. A real bug.

Usage: diagnose_mismatch.py EXPECTED ACTUAL
Exit status: 0 if the files are identical, 1 if only FLOAT_MINOR diffs were
found, 2 if any STRUCTURAL/INTEGER/FLOAT_MAJOR diff was found.
"""

import sys

FLOAT_TOLERANCE = 1e-6 + 1e-12  # one unit in the 6th decimal, plus float slop


def _is_float_token(tok: str) -> bool:
    try:
        float(tok)
    except ValueError:
        return False
    return "." in tok


def classify_line(expected: str, actual: str) -> str:
    if expected == actual:
        return "MATCH"
    e_tokens = expected.split()
    a_tokens = actual.split()
    if not e_tokens or not a_tokens or e_tokens[0] != a_tokens[0] or len(e_tokens) != len(a_tokens):
        return "STRUCTURAL"

    max_float_diff = 0.0
    for e_tok, a_tok in zip(e_tokens, a_tokens):
        if e_tok == a_tok:
            continue
        if _is_float_token(e_tok) and _is_float_token(a_tok):
            max_float_diff = max(max_float_diff, abs(float(e_tok) - float(a_tok)))
        else:
            # Differs and at least one side isn't float-shaped -> an
            # integer/id/keyword field differs.
            return "INTEGER"

    if max_float_diff == 0.0:
        return "STRUCTURAL"  # differing tokens that both parsed equal? shouldn't happen
    return "FLOAT_MINOR" if max_float_diff <= FLOAT_TOLERANCE else "FLOAT_MAJOR"


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    expected_path, actual_path = sys.argv[1], sys.argv[2]

    with open(expected_path) as f:
        expected_lines = f.read().splitlines()
    with open(actual_path) as f:
        actual_lines = f.read().splitlines()

    worst = "MATCH"
    severity = {"MATCH": 0, "FLOAT_MINOR": 1, "FLOAT_MAJOR": 2, "INTEGER": 2, "STRUCTURAL": 3}

    max_len = max(len(expected_lines), len(actual_lines))
    for i in range(max_len):
        e = expected_lines[i] if i < len(expected_lines) else "<missing>"
        a = actual_lines[i] if i < len(actual_lines) else "<missing>"
        verdict = classify_line(e, a) if e != "<missing>" and a != "<missing>" else "STRUCTURAL"
        if verdict != "MATCH":
            print(f"line {i + 1}: {verdict}")
            print(f"  expected: {e}")
            print(f"  actual:   {a}")
        if severity[verdict] > severity[worst]:
            worst = verdict

    if worst == "MATCH":
        print("No differences.")
        return 0
    if worst == "FLOAT_MINOR":
        print("\nVerdict: FLOAT_MINOR only -- consistent with the documented double-vs-long-double "
              "Kahan precision risk (see README). Worth a manual check if this recurs across many "
              "datasets, but not necessarily a bug.")
        return 1
    print(f"\nVerdict: {worst} -- treat as a real bug.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
