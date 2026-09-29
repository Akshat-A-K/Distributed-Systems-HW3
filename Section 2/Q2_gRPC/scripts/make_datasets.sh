#!/bin/bash
# Generates the standard set of test/benchmark datasets via the reused
# generate.cpp binary (build_reference.sh must have run first).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GEN="$PROJECT_ROOT/build/generate"
OUT="$PROJECT_ROOT/data/generated"
mkdir -p "$OUT"

# N K S seed
SPECS=(
    "5 5 100 42"
    "100 5 100 42"
    "10000 5 100 42"
    "100000 5 100 42"
    "1000000 5 100 42"
    "123457 10 37 7"
)

for spec in "${SPECS[@]}"; do
    read -r n k s seed <<< "$spec"
    out_file="$OUT/gen_n${n}_k${k}_s${s}_seed${seed}.txt"
    "$GEN" "$n" "$k" "$s" "$seed" > "$out_file"
    echo "Generated: $out_file"
done
