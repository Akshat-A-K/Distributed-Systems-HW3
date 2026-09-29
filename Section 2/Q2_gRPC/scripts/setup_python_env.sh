#!/bin/bash
# setup_python_env.sh -- sourced (not executed) by submit_correctness.sh and
# submit_bench.sh. Loads the newest available `module load python/3.x` and
# (re)builds .venv on it if needed, instead of silently trusting whatever
# `python3` the login/compute node defaults to.
#
# Why this exists: our first RCE correctness run (job 98701) built .venv
# against this cluster's default `python3`, which turned out to be Python
# 3.6.8 -- a decade-old interpreter with much slower startup and older
# grpcio wheels. The whole 248-case matrix ran roughly 10-15x slower than on
# the dev machine and hit the job's time limit before finishing. A modern
# Python (confirmed available elsewhere on this same cluster via
# `module load python/3.12.5`) is the more likely fix than just raising the
# time limit further.
#
# Safe to source even if no `module` command exists (e.g. running this
# locally) or no python module is found -- falls back to the default
# `python3` with a clear warning rather than failing.

echo "Default python3: $(python3 --version 2>&1)   host: $(hostname)   arch: $(uname -m)"

if command -v module >/dev/null 2>&1; then
    BEST_PY_MODULE="$(module avail python 2>&1 | grep -oE 'python/3\.[0-9]+(\.[0-9]+)?' | sort -t/ -k2 -V | tail -1)"
    if [[ -n "$BEST_PY_MODULE" ]]; then
        echo "Loading module: $BEST_PY_MODULE"
        module load "$BEST_PY_MODULE"
        echo "python3 after module load: $(python3 --version 2>&1)"
    else
        echo "WARNING: no python/3.x module found via 'module avail' -- staying on the default above." >&2
    fi
fi

NEED_VENV_REBUILD=0
if [[ ! -x ".venv/bin/python3" ]]; then
    NEED_VENV_REBUILD=1
else
    VENV_PY_MINOR="$(.venv/bin/python3 -c 'import sys; print(sys.version_info[1])' 2>/dev/null || echo 0)"
    if [[ "${VENV_PY_MINOR:-0}" -lt 8 ]]; then
        echo ".venv was built on Python 3.$VENV_PY_MINOR -- rebuilding on $(python3 --version 2>&1)"
        rm -rf .venv
        NEED_VENV_REBUILD=1
    fi
fi
if [[ "$NEED_VENV_REBUILD" -eq 1 ]]; then
    python3 -m venv .venv
    .venv/bin/pip install --quiet --upgrade pip
    .venv/bin/pip install --quiet -r requirements.txt
fi
.venv/bin/python3 -c "import sys, grpc, grpc_tools, google.protobuf; print('grpc/protobuf import OK, venv python:', sys.version)"
