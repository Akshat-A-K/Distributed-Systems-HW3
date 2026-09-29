# Section 3: Collaborative Document Editing System using gRPC

**Course**: Distributed Systems  
**Assignment**: Homework 3 (Section 3: Problem 1)  
**Implementation**: Python 3, gRPC, Protocol Buffers  

---

## 1. Executive Summary

This project implements a multi-user, distributed collaborative text document editing system based on **gRPC** and **Protocol Buffers**. Documents are maintained centrally in memory on the gRPC server. Multiple clients running on identical or physically separate cluster nodes can simultaneously create, inspect, edit, and stream real-time document modifications. When any client modifies a document, all active subscribers receive the updated document content instantly via server-streaming gRPC without manual polling.

---

## 2. System Architecture

The collaborative editing system follows a central server, multi-client architecture:

```
                      +---------------------------------------+
                      |          gRPC Document Server         |
                      |        (Host: 0.0.0.0, Port: 53408)   |
                      |                                       |
                      |   - Shared In-Memory Documents Map    |
                      |   - Global Mutex (threading.Lock)     |
                      |   - Per-Document Subscriber Queues    |
                      +-------------------+-------------------+
                                          |
        +---------------------------------+---------------------------------+
        | (Unary RPCs: Create/Get/Edit)   | (Server Streaming RPC)          |
        v                                 v                                 v
+------------------+             +------------------+             +------------------+
|     Client 1     |             |     Client 2     |             |     Client 3     |
| (Compute Node A) |             | (Compute Node B) |             | (Compute Node C) |
| - CLI Interface  |             | - Live Streaming |             | - Concurrent     |
| - Creator/Editor |             |   Background Thd |             |   Editor         |
+------------------+             +------------------+             +------------------+
```

### 2.1 Service Interface (`document.proto`)

The RPC contract is defined in `document.proto` with four core operations:

1. **`CreateDocument(CreateDocumentRequest) -> CreateDocumentResponse`** (Unary RPC)
   - Parameters: `name` (string), `initial_content` (string)
   - Checks for duplicate document names or empty input.
   - Initializes an empty subscriber queue list for the document.

2. **`GetDocument(GetDocumentRequest) -> GetDocumentResponse`** (Unary RPC)
   - Parameters: `name` (string)
   - Retrieves the current content of an existing document.
   - Returns error status if the document is not found.

3. **`EditDocument(EditDocumentRequest) -> EditDocumentResponse`** (Unary RPC)
   - Parameters: `name` (string), `position` (int32), `text` (string)
   - Inserts text at the designated 0-based character index.
   - Validates boundary conditions (`0 <= position <= len(content)`).
   - Atomically broadcasts the updated document to all subscriber queues.

4. **`SubscribeToUpdates(UpdateRequest) -> stream DocumentUpdate`** (Server-Streaming RPC)
   - Parameters: `name` (string)
   - Streams `DocumentUpdate` messages containing the document's latest content whenever an edit occurs.
   - Maintains an individual thread-safe `queue.Queue` per client connection until the client disconnects.

---

## 3. Concurrency and Synchronization Design

### 3.1 Server-Side State Protection
The server maintains two primary shared data structures:
* `self.documents`: Dictionary mapping `document_name -> document_content (str)`
* `self.subscribers`: Dictionary mapping `document_name -> list[queue.Queue]`

All modifications and lookups are guarded by a central mutex (`self.lock = threading.Lock()`).

### 3.2 Ordering of Concurrent Edits
Per assignment specifications (Section 1.4: *"Students are not required to implement Operational Transformation (OT) or CRDTs. If multiple edits arrive concurrently, the server may apply them in the order in which they are processed"*), concurrent edit RPCs obtain the server mutex sequentially:
1. Thread acquires `self.lock`.
2. Verifies current document length against requested insertion index.
3. Computes `new_content = content[:pos] + text + content[pos:]`.
4. Updates `self.documents[name]`.
5. Pushes `DocumentUpdate` to every connected subscriber queue.
6. Releases `self.lock`.

This guarantees race-condition freedom, eliminates memory corruption, and provides sequential consistency across all connected clients.

### 3.3 Non-Blocking Streaming via Per-Client Queues
To prevent a slow network connection on one client from stalling other clients or server RPC handlers:
* Each active subscriber has an independent `queue.Queue`.
* Edit handlers place updates into the queues without blocking on network transmission.
* The streaming generator loop yields updates from the queue with a timeout, allowing graceful detection of client disconnections and clean resource deallocation in a `finally` block.

---

## 4. Multi-Node Cluster Execution (RCE)

As specified in the course `rce_grpc_execution_guide.pdf`, the system is structured to execute across physically separate compute nodes:

```bash
# 1. Allocate compute nodes via Slurm
salloc --nodes=3 --ntasks-per-node=1

# 2. Check assigned hostnames
scontrol show hostnames $SLURM_JOB_NODELIST
# Example output: node01, node02, node03

# 3. Terminal 1 (Node 1 - Server):
ssh node01
cd ~/Distributed-Systems-HW3/"Section 3"
bash run_server.sh 53408

# 4. Terminal 2 (Node 2 - Client 1):
ssh node02
cd ~/Distributed-Systems-HW3/"Section 3"
bash run_client.sh node01:53408

# 5. Terminal 3 (Node 3 - Client 2):
ssh node03
cd ~/Distributed-Systems-HW3/"Section 3"
bash run_client.sh node01:53408
```

---

## 5. Demonstration and Verification Walkthrough

The following step-by-step workflow was verified:

### Step 1: Document Creation (Client 1)
```text
Enter choice: 1
Document name: report.txt
Initial content: Hello World
[Client] Document report.txt created.
```

### Step 2: Document Inspection (Client 2)
```text
Enter choice: 2
Document name: report.txt
[Client] Hello World
```

### Step 3: Real-Time Update Subscription (Client 2)
```text
Enter choice: 4
Document name: report.txt
[Client] Subscribed to updates.
```
*Client 2 spawns a background listener thread on `SubscribeToUpdates` while keeping the interactive CLI fully responsive.*

### Step 4: Document Editing (Client 1)
```text
Enter choice: 3
Document name: report.txt
Position: 6
Text to insert: Distributed 
[Client] Document edited successfully
```

### Step 5: Live Stream Reception (Client 2)
Client 2 automatically intercepts and prints the stream message without polling:
```text
[Update] Document report.txt modified.
Hello Distributed World
```

### Step 6: Concurrent Editing Test (Client 1 & Client 3)
* Client 1 submits: insert `"Systems "` at position 6.
* Client 3 submits: insert `"Cloud "` at position 6.
Both RPCs execute safely in critical sections; the server safely serializes both insertions without race conditions, and all subscribers receive successive valid updates.

### Step 7: Error and Boundary Handling
* **Empty Name / Non-existent Document**: Returns `success=False` with informative message.
* **Duplicate Document Creation**: Prevents overwriting existing documents.
* **Out-of-Bounds Position**: Validated against current string bounds; rejected with `Invalid Position` before applying.

---

## 6. Conclusion

The implementation fully satisfies all requirements of Problem 1:
1. Complete Protocol Buffer definition and gRPC service methods.
2. Concurrent-safe in-memory document state management.
3. Server-streaming real-time update distribution.
4. Clean decoupled CLI interface capable of background streaming reception.
5. Cluster-ready shell automation compatible with RCE Slurm environments.
