# Section 3: Collaborative Document Editing System using gRPC

This project is a multi-user collaborative text document editing system built using Python and gRPC. It allows multiple clients to connect to a central server, create documents, read documents, edit documents concurrently, and receive live updates when a document is modified.

## Files in this Directory

- document.proto: Protocol Buffer file defining the service and message formats.
- server.py: The gRPC server implementation handling document storage, synchronization, and streaming.
- client.py: The interactive command-line interface (CLI) for users to interact with the server.
- document_pb2.py: Generated Python code for protocol buffer messages.
- document_pb2_grpc.py: Generated Python code for gRPC client stubs and server servicers.
- report.md: Project report explaining architecture, synchronization, and demonstration results.

## Requirements

- Python 3.8 or above
- grpcio
- grpcio-tools

Install dependencies:
pip install grpcio grpcio-tools

## How to Compile the Proto File

If you change document.proto or need to regenerate the code:
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. document.proto

## How to Run

### Step 1: Start the Server

Run on local machine:
python server.py localhost:50051

Or run on cluster / all interfaces (for RCE or multiple machines):
python server.py 0.0.0.0:50051

### Step 2: Start the Clients

Open another terminal and start Client 1:
python client.py localhost:50051

Open a third terminal and start Client 2:
python client.py localhost:50051

(Replace localhost with the server IP or hostname if running on different machines).

## Interactive Menu Options

When you start the client, you will see this menu:

********** Document Client **********
1. Create Document
2. Open Document
3. Edit Document
4. Subscribe to Updates
5. Exit
************************************

1. Create Document: Enter a unique document name and initial content.
2. Open Document: Enter document name to see its current content.
3. Edit Document: Enter document name, integer insertion position (0-based index), and text to insert.
4. Subscribe to Updates: Enter document name to listen for any modifications in real time.
5. Exit: Cleanly closes connection and exits.

## Step-by-Step Demonstration

### 1. Creating a Document (Client 1)
Select Option 1 on Client 1:
Enter choice: 1
Document name: report.txt
Initial content: Hello World
Output:
[Client] Document report.txt created.

### 2. Opening the Document (Client 1 & Client 2)
Select Option 2 on Client 1 and Client 2:
Enter choice: 2
Document name: report.txt
Output:
[Client] Hello World

### 3. Subscribing to Updates (Client 2)
Select Option 4 on Client 2:
Enter choice: 4
Document name: report.txt
Output:
[Client] Subscribed to updates.

### 4. Editing the Document (Client 1)
Select Option 3 on Client 1:
Enter choice: 3
Document name: report.txt
Position: 6
Text to insert: Distributed 
Output:
[Client] Document edited successfully

### 5. Automatic Update Reception (Client 2)
Immediately after Client 1 edits, Client 2 automatically displays:
[Update] Document report.txt modified.
Hello Distributed World

Client 2 does not need to refresh or call Open Document again.

### 6. Concurrent Editing Test
Run two edits at the same time:
- Client 1 inserts "Distributed " at position 6.
- Client 2 inserts "Systems " at position 6.
Both requests are safely processed by the server without data loss or corruption, and all subscribed clients receive the updated document.
