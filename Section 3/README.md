# Section 3: Collaborative Document Editing System using gRPC

This project is a multi-user collaborative text document editing system built using Python and gRPC. It allows multiple clients to connect to a central server, create documents, read documents, edit documents concurrently, and receive live updates when a document is modified.

## Files in this Directory

- `document.proto`: Protocol Buffer file defining the service contract and message formats.
- `server.py`: Central gRPC server handling in-memory document storage, mutex-protected synchronization, and subscriber streaming.
- `client.py`: Multi-threaded interactive command-line interface (CLI) for user operations and live background update reception.
- `document_pb2.py`: Generated Python code for protocol buffer messages.
- `document_pb2_grpc.py`: Generated Python code for gRPC client stubs and server servicers.
- `requirements.txt`: Python package requirements (`grpcio`, `grpcio-tools`, `protobuf`).
- `setup_env.sh`: Automated environment detector for RCE cluster module loading and virtual environment configuration.
- `run_server.sh`: Shell script to start the gRPC server (binds `0.0.0.0` for cluster/local execution).
- `run_client.sh`: Shell script to launch a client connected to a specific host and port.
- `run_demo_tmux.sh`: Automated one-command runner creating a 4-pane tmux session (1 Server + 3 Clients).
- `report.md`: Complete project report explaining architecture, synchronization, and demonstration results.

## Requirements & Environment Setup

- Python 3.8 or above
- `grpcio >= 1.50.0`
- `grpcio-tools >= 1.50.0`
- `protobuf >= 3.20.0`

### Quick Install
```bash
pip install -r requirements.txt
```

### On RCE Cluster
The provided `setup_env.sh` automatically detects cluster environment modules (`module load python/3.12.5`), uses or creates `.venv`, and compiles the proto stubs if necessary.

## How to Compile the Proto File

If you modify `document.proto` or need to regenerate the stubs:
```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. document.proto
```

## How to Run

### Method 1: Automated 1-Command `tmux` Demo (Local / WSL / Cluster)

To launch the server and 3 clients simultaneously in 4 split panes within a single terminal:
```bash
bash run_demo_tmux.sh 53408
```
*(Switch panes using `Ctrl + b` then `Arrow Keys`; detach with `Ctrl + b` then `d`)*.

---

### Method 2: Manual Multi-Terminal Execution

#### Step 1: Start the Server (Terminal 1)
```bash
bash run_server.sh 53408
```
*(Or directly with Python: `python3 server.py 0.0.0.0:53408`)*

#### Step 2: Start Client 1 (Terminal 2)
```bash
bash run_client.sh 53408
```
*(Or directly with Python: `python3 client.py localhost:53408`)*

#### Step 3: Start Client 2 (Terminal 3)
```bash
bash run_client.sh 53408
```

---

### Method 3: Multi-Node Cluster Execution on RCE (as per RCE Execution Guide)

Following `rce_grpc_execution_guide.pdf`:

1. **Allocate Compute Nodes**:
   ```bash
   salloc --nodes=3 --ntasks-per-node=1
   scontrol show hostnames $SLURM_JOB_NODELIST
   # Example assigned nodes: node01, node02, node03
   ```

2. **Terminal 1 (Server on `node01`)**:
   ```bash
   ssh node01
   cd ~/Distributed-Systems-HW3/"Section 3"
   bash run_server.sh 53408
   ```

3. **Terminal 2 (Client 1 on `node02`)**:
   ```bash
   ssh node02
   cd ~/Distributed-Systems-HW3/"Section 3"
   bash run_client.sh node01:53408
   ```

4. **Terminal 3 (Client 2 on `node03`)**:
   ```bash
   ssh node03
   cd ~/Distributed-Systems-HW3/"Section 3"
   bash run_client.sh node01:53408
   ```

---

## Interactive Menu Options

When you start the client, you will see this menu:

```text
********** Document Client **********
1. Create Document
2. Open Document
3. Edit Document
4. Subscribe to Updates
5. Exit
************************************
```

1. **Create Document**: Enter a unique document name and initial content.
2. **Open Document**: Enter document name to see its current content.
3. **Edit Document**: Enter document name, integer insertion position (0-based index), and text to insert.
4. **Subscribe to Updates**: Enter document name to listen for any modifications in real time via server-streaming gRPC.
5. **Exit**: Cleanly closes connection and exits.

---

## Step-by-Step Demonstration Walkthrough

### 1. Creating a Document (Client 1)
```text
Enter choice: 1
Document name: report.txt
Initial content: Hello World
Output: [Client] Document report.txt created.
```

### 2. Opening the Document (Client 1 & Client 2)
```text
Enter choice: 2
Document name: report.txt
Output: [Client] Hello World
```

### 3. Subscribing to Updates (Client 2)
```text
Enter choice: 4
Document name: report.txt
Output: [Client] Subscribed to updates.
```

### 4. Editing the Document (Client 1)
```text
Enter choice: 3
Document name: report.txt
Position: 6
Text to insert: Distributed 
Output: [Client] Document edited successfully
```

### 5. Automatic Update Reception (Client 2)
Immediately after Client 1 edits, Client 2 **automatically receives and prints** the update without polling or calling Open Document again:
```text
[Update] Document report.txt modified.
Hello Distributed World
```

### 6. Concurrent Editing Test
Run two edits at approximately the same time from separate clients:
- **Client 1**: Inserts `"Systems "` at position 6.
- **Client 2 / Client 3**: Inserts `"Cloud "` at position 6.

Both requests are safely synchronized on the server via `threading.Lock()` mutex without data loss or corruption, and all subscribed clients receive the successive updates in real time.
