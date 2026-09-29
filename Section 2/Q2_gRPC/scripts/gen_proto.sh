#!/bin/bash
# Generates weather_stream_pb2.py / weather_stream_pb2_grpc.py from the
# .proto into src/. Never committed (protobuf's runtime version-checks the
# generated code against the installed library, so stubs generated on one
# machine can fail on another -- regenerate on each machine you run this on,
# per the plan's RCE execution notes).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

python3 -m grpc_tools.protoc \
    -I"$PROJECT_ROOT/proto" \
    --python_out="$PROJECT_ROOT/src" \
    --grpc_python_out="$PROJECT_ROOT/src" \
    "$PROJECT_ROOT/proto/weather_stream.proto"

echo "Generated: $PROJECT_ROOT/src/weather_stream_pb2.py, weather_stream_pb2_grpc.py"
