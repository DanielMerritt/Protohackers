import socket
import threading
import json


def is_prime(num: int | float) -> bool:
    if num < 2 or type(num) == float:
        return False
    for i in range(2, int(num**0.5) + 1):
        if num % i == 0:
            return False
    return True


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
            thread = threading.Thread(
                target=self.process_connection, args=(conn,), daemon=True
            )
            thread.start()

    def process_connection(self, conn: socket.socket) -> None:
        buffer = ""
        try:
            while True:
                chunk: bytes = conn.recv(1024)
                if not chunk:
                    break
                buffer += chunk.decode()
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue
                    good_request = self.handle_request(conn, line)
                    if not good_request:
                        conn.close()
                        return
        except (socket.timeout, ConnectionResetError, BrokenPipeError):
            with self.print_lock:
                print("Connection timed out")
        finally:
            conn.close()

    def handle_request(self, conn: socket.socket, line: str) -> bool:
        try:
            json_obj = json.loads(line)
        except json.decoder.JSONDecodeError:
            with self.print_lock:
                print(f"Malformed Json: {line}")
            conn.sendall(b"Malformed\n")
            return False
        try:
            assert json_obj["method"] == "isPrime"
            number = json_obj["number"]
            assert type(number) == int or type(number) == float
        except (AssertionError, KeyError):
            with self.print_lock:
                print(f"Error in Json: {line}")
            conn.sendall(b"Malformed\n")
            return False
        with self.print_lock:
            print(f"Accepting {line}")
        prime = is_prime(number)
        new_json = {"method": "isPrime", "prime": prime}
        conn.sendall(json.dumps(new_json).encode() + b"\n")
        return True

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
