#!/bin/bash
# ==============================================================================
# run_client.sh - Start Interactive gRPC Client (Section 3) on RCE / Linux
# Usage:
#   ./run_client.sh               # connects to localhost:50051
#   ./run_client.sh 50051         # connects to localhost:50051
#   ./run_client.sh <IP>:50051    # connects to remote server host:port
# ==============================================================================

TARGET=${1:-"localhost:50051"}

# If only a port number was provided (e.g. ./run_client.sh 50051)
if [[ "$TARGET" =~ ^[0-9]+$ ]]; then
    TARGET="localhost:${TARGET}"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Source environment setup (handles modules on RCE and virtualenvs)
source ./setup_env.sh

echo "Connecting Document Client to: $TARGET"
"$PY_EXEC" client.py "$TARGET"
