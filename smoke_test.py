import socket
import threading


class Server:
    def __init__(self, server: str, port: int) -> None:
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((server, port))
        self.sock.listen()
        self.print_lock = threading.Lock()
        print(f"Server listening on {server}:{port}")

    def handle_connections(self) -> None:
        while True:
            conn, addr = self.sock.accept()
            with self.print_lock:
                print(f"Accepted Connection from {addr}")
            thread = threading.Thread(target=self.process_connection, args=(conn,))
            thread.start()

    def process_connection(self, conn) -> None:
        conn.settimeout(1)
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                conn.sendall(data)
            with self.print_lock:
                print("Closing Connection...")
        except socket.timeout:
            with self.print_lock:
                print("Connection timed out")
        conn.close()

    def close(self) -> None:
        self.sock.close()


def main() -> None:
    server = Server("0.0.0.0", 4444)
    try:
        server.handle_connections()
    except KeyboardInterrupt:
        server.close()


if __name__ == "__main__":
    main()
