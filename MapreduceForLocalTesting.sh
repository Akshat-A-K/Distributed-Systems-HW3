#!/bin/bash
# ==============================================================================
# MapreduceForLocalTesting.sh
# Local runner script for MapReduce testing.
# If Section 1 matrix tests are present, delegates to Section 1/run_local.sh
# to run all matrix categories. Otherwise runs python mapper/combiner/reducer.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "$SCRIPT_DIR/Section 1/run_local.sh" ]; then
    echo "Running Section 1 Local Matrix Benchmarks across all categories..."
    bash "$SCRIPT_DIR/Section 1/run_local.sh" "$@"
    exit $?
fi

INPUT_FILE=${1:-input.txt}
OUTPUT_FILE=${2:-output.txt}

if [ -f "mapper.py" ]; then
    python3 mapper.py < "$INPUT_FILE" | \
        sort | \
        python3 combiner.py | \
        sort | \
        python3 reducer.py > "$OUTPUT_FILE"
    echo "MapReduce pipeline completed. Output saved to $OUTPUT_FILE"
else
    echo "Error: Neither Section 1/run_local.sh nor mapper.py found in current directory."
    exit 1
fi