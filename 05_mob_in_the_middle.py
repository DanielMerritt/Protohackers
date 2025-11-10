import socket
import threading
import re


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
        conn.settimeout(30)
        buffer = ""
        upstream_conn = socket.socket()
        upstream_conn.connect(("chat.protohackers.com", 16963))
        upstream_connection_thread = threading.Thread(
            target=self.process_upstream_connection,
            args=(upstream_conn, conn),
            daemon=True,
        )
        upstream_connection_thread.start()
        try:
            while True:
                try:
                    chunk: bytes = conn.recv(1024)
                except OSError as e:
                    break
                if not chunk:
                    break
                buffer += chunk.decode()
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    self.send_message(upstream_conn, line)
        except (socket.timeout, ConnectionResetError, BrokenPipeError) as e:
            with self.print_lock:
                print(f"Connection Error: {e}")
        finally:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                upstream_conn.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            conn.close()
            upstream_conn.close()

    def process_upstream_connection(
        self, upstream_conn: socket.socket, client_conn: socket.socket
    ) -> None:
        buffer = ""
        upstream_conn.settimeout(30)
        try:
            while True:
                try:
                    chunk: bytes = upstream_conn.recv(1024)
                except OSError as e:
                    break
                if not chunk:
                    break
                buffer += chunk.decode()
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    self.send_message(client_conn, line)
        except (socket.timeout, ConnectionResetError, BrokenPipeError) as e:
            with self.print_lock:
                print(f"Connection Error: {e}")
        finally:
            try:
                upstream_conn.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                client_conn.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            upstream_conn.close()
            client_conn.close()

    @staticmethod
    def rewrite_boguscoin_addresses(message: str) -> str:
        boguscoin_pattern = re.compile(r"(?:(?<=^)|(?<= ))(7[a-zA-Z0-9]{25,34})(?= |$)")
        return re.sub(boguscoin_pattern, "7YWHMfk9JZe0LM0g1ZauHuiSxhI", message)

    def send_message(self, conn: socket.socket, message: str) -> None:
        modified_message = self.rewrite_boguscoin_addresses(message)
        try:
            conn.sendall(modified_message.encode() + b"\n")
        except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError) as e:
            with self.print_lock:
                print(f"Send failure: {e}")

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


def main() -> None:
    server = Server("0.0.0.0", 4444)
    try:
        server.handle_connections()
    except KeyboardInterrupt:
        server.close()


if __name__ == "__main__":
    main()
