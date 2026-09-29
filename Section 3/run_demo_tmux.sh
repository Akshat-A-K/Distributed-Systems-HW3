#!/bin/bash
# ==============================================================================
# run_demo_tmux.sh - Launch Server and 3 Clients across 4 tmux panes automatically
# ==============================================================================

SESSION="grpc_demo"
PORT=${1:-50051}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if tmux is installed
if ! command -v tmux &> /dev/null; then
    echo "[!] tmux is not installed. You can run directly in separate terminals using ./run_server.sh and ./run_client.sh"
    exit 1
fi

# Kill previous session if running
tmux kill-session -t $SESSION 2>/dev/null

echo "Launching gRPC Document Editing System on port $PORT in tmux session '$SESSION' (4 panes)..."

# 1. Start tmux with server in top-left pane
tmux new-session -d -s $SESSION -n "Q3-Demo" "cd '$SCRIPT_DIR' && bash run_server.sh $PORT"

# 2. Split horizontally (create top-right pane) -> Client 1
tmux split-window -h -t $SESSION:0 "cd '$SCRIPT_DIR' && sleep 2 && bash run_client.sh localhost:$PORT"

# 3. Split top-left pane vertically (create bottom-left pane) -> Client 2
tmux split-window -v -t $SESSION:0.0 "cd '$SCRIPT_DIR' && sleep 2 && bash run_client.sh localhost:$PORT"

# 4. Split top-right pane vertically (create bottom-right pane) -> Client 3
tmux split-window -v -t $SESSION:0.1 "cd '$SCRIPT_DIR' && sleep 2 && bash run_client.sh localhost:$PORT"

echo "Panes created:"
echo "  [Pane 0 - Top Left]     : Server"
echo "  [Pane 1 - Bottom Left]  : Client 2"
echo "  [Pane 2 - Top Right]    : Client 1"
echo "  [Pane 3 - Bottom Right] : Client 3"
echo ""
echo "Attaching to tmux session now (Press Ctrl+b then d to detach)..."
tmux attach-session -t $SESSION
