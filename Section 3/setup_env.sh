#!/bin/bash
# ==============================================================================
# setup_env.sh - Environment detection and python setup for RCE cluster & local
# ==============================================================================

# Ensure SCRIPT_DIR is defined
if [ -z "$SCRIPT_DIR" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

# 1. If on RCE or HPC cluster with Environment Modules, load modern Python 3
if command -v module >/dev/null 2>&1; then
    CURRENT_PY_VER=$(python3 -c 'import sys; print(sys.version_info[0]*10 + sys.version_info[1])' 2>/dev/null || echo 0)
    if [ "$CURRENT_PY_VER" -lt 38 ]; then
        BEST_MOD=$(module avail python 2>&1 | grep -oE 'python/3\.[0-9]+(\.[0-9]+)?' | sort -t/ -k2 -V | tail -1)
        if [ -n "$BEST_MOD" ]; then
            echo "[Environment] Loading modern Python module: $BEST_MOD"
            module load "$BEST_MOD"
        fi
    fi
fi

# 2. Check for an existing working Python virtual environment
PY_EXEC=""
if [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
    PY_EXEC="$SCRIPT_DIR/.venv/bin/python"
elif [ -x "$SCRIPT_DIR/../Section 2/Q2_gRPC/.venv/bin/python3" ]; then
    PY_EXEC="$SCRIPT_DIR/../Section 2/Q2_gRPC/.venv/bin/python3"
elif command -v python3 >/dev/null 2>&1; then
    PY_EXEC="python3"
elif command -v python >/dev/null 2>&1; then
    PY_EXEC="python"
fi

# 3. Ensure grpcio and grpcio-tools are installed
if ! "$PY_EXEC" -c "import grpc, grpc_tools" >/dev/null 2>&1; then
    echo "[Notice] grpc modules not found for $PY_EXEC. Creating virtual environment in $SCRIPT_DIR/.venv..."
    "$PY_EXEC" -m venv "$SCRIPT_DIR/.venv" 2>/dev/null
    if [ -x "$SCRIPT_DIR/.venv/bin/pip" ]; then
        echo "[Notice] Installing packages from requirements.txt into .venv..."
        "$SCRIPT_DIR/.venv/bin/pip" install --quiet --upgrade pip
        if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
            "$SCRIPT_DIR/.venv/bin/pip" install --quiet -r "$SCRIPT_DIR/requirements.txt"
        else
            "$SCRIPT_DIR/.venv/bin/pip" install --quiet grpcio grpcio-tools protobuf
        fi
        PY_EXEC="$SCRIPT_DIR/.venv/bin/python"
    else
        echo "[Notice] Falling back to user pip install..."
        if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
            pip install --user -r "$SCRIPT_DIR/requirements.txt" 2>/dev/null || pip install -r "$SCRIPT_DIR/requirements.txt"
        else
            pip install --user grpcio grpcio-tools protobuf 2>/dev/null || pip install grpcio grpcio-tools protobuf
        fi
    fi
fi

# 4. Check protobuf stub compatibility and auto-regenerate if needed
(
    cd "$SCRIPT_DIR"
    if ! "$PY_EXEC" -c "import document_pb2, document_pb2_grpc" >/dev/null 2>&1; then
        echo "[Notice] Compiling document.proto for current environment ($PY_EXEC)..."
        "$PY_EXEC" -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. document.proto 2>/dev/null || true
    fi
)

export PY_EXEC
