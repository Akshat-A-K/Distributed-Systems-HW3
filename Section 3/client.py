import sys
import grpc
import threading
import document_pb2
import document_pb2_grpc

class DocumentClient:
    def __init__(self, server_address):
        self.channel = grpc.insecure_channel(server_address)
        self.stub = document_pb2_grpc.DocumentServiceStub(self.channel)
        self.running = True
        self.subscriptions = set()

    def create_document(self, name, content):
        request = document_pb2.CreateDocumentRequest(name = name, initial_content = content)
        try:
            response = self.stub.CreateDocument(request)
            if response.success:
                print(f"[Client] Document {name} created.")
            else:
                print("[Error]", response.message)
        except grpc.RpcError as e:
            print("[Error] Unable to connect to server. It may be offline.")

    def open_document(self, name):
        request = document_pb2.GetDocumentRequest(name = name)
        try:
            response = self.stub.GetDocument(request)
            if response.success:
                print("[Client]", response.content)
            else:
                print("[Error]", response.message)

        except grpc.RpcError as e:
            print("[Error] Unable to connect to server. It may be offline.")

    def edit_document(self, name, position, text):
        request = document_pb2.EditDocumentRequest(name = name, position = position, text = text)
        try:
            response = self.stub.EditDocument(request)
            if response.success:
                print("[Client]", response.message)
            else:
                print("[Error]", response.message)
        except grpc.RpcError as e:
            print("[Error] Unable to connect to server. It may be offline.")

    def subscribe(self, name):
        if name in self.subscriptions:
            print(f"[Client] Already subscribed to updates for {name}.")
            return

        request = document_pb2.GetDocumentRequest(name = name)
        try:
            response = self.stub.GetDocument(request)
            if not response.success:
                print("[Error]", response.message)
                return
        except grpc.RpcError as e:
            print("[Error] Unable to connect to server. It may be offline.")
            return

        def receive_updates():
            update_req = document_pb2.UpdateRequest(name = name)
            try:
                updates = self.stub.SubscribeToUpdates(update_req)
                for update in updates:
                    print()
                    print(f"[Update] Document {update.name} modified.")
                    print(update.content)
                    print()
            except grpc.RpcError as e:
                if self.running and e.code() != grpc.StatusCode.CANCELLED:
                    print("\n[Subscription Error] Connection to server lost or server is offline.\n")
            finally:
                self.subscriptions.discard(name)
        
        self.subscriptions.add(name)
        print("[Client] Subscribed to updates.")
        thread = threading.Thread(target=receive_updates, daemon=True)
        thread.start()

    def close(self):
        self.running = False
        self.channel.close()


def print_menu():
    print()
    print("********** Document Client **********")
    print("1. Create Document")
    print("2. Open Document")
    print("3. Edit Document")
    print("4. Subscribe to Updates")
    print("5. Exit")
    print("************************************")

def main():
    if len(sys.argv) != 2:
        print("Usage: python client.py <server-host:port>")
        sys.exit(1)
    server_address = sys.argv[1]
    if ":" not in server_address:
        server_address = f"localhost:{server_address}"
    client = DocumentClient(server_address)
    try:
        while client.running:
            print_menu()
            choice = input("Enter choice: ").strip()
            if choice == "1":
                name = input("Document name: ").strip()
                if not name:
                    print("[Error] Document name cannot be empty.")
                    continue
                content = input("Initial content: ")
                client.create_document(name, content)

            elif choice == "2":
                name = input("Document name: ").strip()
                if not name:
                    print("[Error] Document name cannot be empty.")
                    continue
                client.open_document(name)
            elif choice == "3":
                name = input("Document name: ").strip()
                if not name:
                    print("[Error] Document name cannot be empty.")
                    continue
                position = input("Position: ").strip()
                text = input("Text to insert: ")
                try:
                    position = int(position)
                    client.edit_document(name, position, text)
                except ValueError:
                    print("[Error] Position must be an integer.")

            elif choice == "4":
                name = input("Document name: ").strip()
                if not name:
                    print("[Error] Document name cannot be empty.")
                    continue
                client.subscribe(name)
            elif choice == "5":
                print("Exiting...")
                client.close()
            else:
                print("Invalid choice.")
    except (KeyboardInterrupt, EOFError):
        print("\nExiting...")
        client.close()

if __name__ == "__main__":
    main()
