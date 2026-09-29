#!/bin/bash
# ==============================================================================
# run_server.sh - Start gRPC Document Editing Server (Section 3) on RCE / Linux
# ==============================================================================

# Default port
PORT=${1:-50051}

# Change to Section 3 directory if not already there
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Source environment setup (handles modules on RCE and virtualenvs)
source ./setup_env.sh

echo "============================================="
echo " Starting Document gRPC Server on port $PORT"
echo " Host IP address(es):"
hostname -I 2>/dev/null || ip addr show | grep -E 'inet ' | awk '{print $2}' || echo "localhost"
echo " Python interpreter: $PY_EXEC"
echo "============================================="

# Run server listening on 0.0.0.0 so clients from any terminal/node can connect
"$PY_EXEC" server.py "0.0.0.0:${PORT}"
