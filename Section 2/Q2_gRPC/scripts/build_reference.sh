#!/bin/bash
# Builds the C++ oracle and dataset generator (unmodified copies from Q1).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
mkdir -p "$PROJECT_ROOT/build"
CXXFLAGS="-O2 -std=c++17 -Wall -Wextra -pedantic"
g++ $CXXFLAGS "$PROJECT_ROOT/reference/weather_seq.cpp" -o "$PROJECT_ROOT/build/weather_seq"
g++ $CXXFLAGS "$PROJECT_ROOT/reference/generate.cpp" -o "$PROJECT_ROOT/build/generate"
echo "Built: $PROJECT_ROOT/build/weather_seq, $PROJECT_ROOT/build/generate"
