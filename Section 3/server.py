import sys
import grpc
import threading
from concurrent import futures
import document_pb2_grpc
import document_pb2
import queue

class DocumentService(document_pb2_grpc.DocumentServiceServicer):
    def __init__(self):
        self.documents = {}
        self.lock = threading.Lock()
        self.subscribers = {}

    def CreateDocument(self, req, context):
        with self.lock:
            if not req.name or not req.name.strip():
                return document_pb2.CreateDocumentResponse(success = False, message = "Document name cannot be empty")
            if req.name in self.documents:
                return document_pb2.CreateDocumentResponse(success = False, message = "Document already exists")
            self.documents[req.name] = req.initial_content
            self.subscribers[req.name] = []
            return document_pb2.CreateDocumentResponse(success = True, message = "Document created successfully")
    
    def GetDocument(self, req, context):
        with self.lock:
            if req.name not in self.documents:
                return document_pb2.GetDocumentResponse(success = False, message = "Document not found")
            content = self.documents[req.name]  
            return document_pb2.GetDocumentResponse(success = True, content = content, message = "Document retrieved successfully")
    
    def EditDocument(self, req, context):
        with self.lock:
            if req.name not in self.documents:
                return document_pb2.EditDocumentResponse(success = False, message = "Document not found")
            content = self.documents[req.name]
            position = req.position
            text = req.text

            if position < 0 or position > len(content):
                return document_pb2.EditDocumentResponse(success = False, message = "Invalid Position")
            new_content = content[:position] + text + content[position:]
            self.documents[req.name] = new_content
            
            subscribers = list(self.subscribers.get(req.name, []))
            update = document_pb2.DocumentUpdate(name = req.name, content = new_content)
            for squeue in subscribers:
                squeue.put(update)
            return document_pb2.EditDocumentResponse(success = True, message = "Document edited successfully")
    
    def SubscribeToUpdates(self, req, context):
        subscriber_queue = queue.Queue()
        with self.lock:
            if req.name not in self.documents:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Document not found")
                return
            self.subscribers.setdefault(req.name, []).append(subscriber_queue)
        print(f"Client Subscribed to {req.name}", flush =True)
        try:
            while context.is_active():
                try: 
                    update = subscriber_queue.get(timeout = 1)
                except queue.Empty:
                    continue
                yield update
        finally:
            with self.lock:
                if req.name in self.subscribers:
                    if subscriber_queue in self.subscribers[req.name]:
                        self.subscribers[req.name].remove(subscriber_queue)
            print("Subscriber disconnected:", req.name, flush = True)

def serve(address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers = 20))
    document_pb2_grpc.add_DocumentServiceServicer_to_server(DocumentService(), server)
    try:
        server.add_insecure_port(address)
    except Exception as e:
        print(f"Failed to bind to {address}: {e}")
        return
    server.start()
    print("Server started on", address, flush = True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Stopping server...")
        server.stop(0)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python server.py <port> or <host:port>")
        sys.exit(1)
    
    address = sys.argv[1]
    if ":" not in address:
        address = f"0.0.0.0:{address}"
    serve(address)
