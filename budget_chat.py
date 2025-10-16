import socket
import threading
from dataclasses import dataclass
from time import sleep


@dataclass
class User:
    name: str
    message_queue: list[str]
    conn: socket.socket
    conn_lock: threading.Lock


class Server:
    def __init__(self, server: str, port: int) -> None:
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((server, port))
        self.sock.listen()
        self.print_lock = threading.Lock()
        print(f"Server listening on {server}:{port}")

    def handle_connections(self) -> None:
        self.users: list[User] = []
        self.users_lock = threading.Lock()
        worker_thread = threading.Thread(
            target=self.process_message_queues, daemon=True
        )
        worker_thread.start()
        while True:
            conn, addr = self.sock.accept()
            with self.print_lock:
                print(f"Accepted Connection from {addr}")
            thread = threading.Thread(
                target=self.process_connection, args=(conn,), daemon=True
            )
            thread.start()

    def process_message_queues(self) -> None:
        while True:
            sleep(0.1)
            with self.users_lock:
                for user in self.users:
                    if not user.message_queue:
                        continue
                    for message in user.message_queue:
                        self.send_message(user.conn, message, user.conn_lock)
                    user.message_queue.clear()

    def process_connection(self, conn: socket.socket) -> None:
        conn.settimeout(30)
        buffer = ""
        joined_room = False
        socket_lock = threading.Lock()
        self.send_message(
            conn, "Welcome to budgetchat! What shall I call you?", socket_lock
        )
        try:
            while True:
                chunk: bytes = conn.recv(1024)
                if not chunk:
                    break
                buffer += chunk.decode()
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        break
                    if joined_room == False:
                        user_name = line
                        if not user_name.isalnum():
                            self.send_message(
                                conn,
                                "Your name must be alphanumeric. Disconnecting...",
                                socket_lock,
                            )
                            return
                        with self.users_lock:
                            for user in self.users:
                                if user.name == user_name:
                                    continue
                                user.message_queue.append(
                                    f"* {user_name} has entered the room"
                                )
                        joining_message = f"* The room contains: {', '.join(user.name for user in self.users)}"
                        self.send_message(conn, joining_message, socket_lock)
                        current_user = User(user_name, [], conn, socket_lock)
                        self.users.append(current_user)
                        joined_room = True
                        continue
                    with self.users_lock:
                        for user in self.users:
                            if user.name == current_user.name:
                                continue
                            user.message_queue.append(f"[{user_name}] {line}")

        except (socket.timeout, ConnectionResetError, BrokenPipeError) as e:
            with self.print_lock:
                print(f"Send failure: {e}")

        finally:
            conn.close()
            if not joined_room:
                return
            with self.users_lock:
                self.users = [
                    user for user in self.users if user.name != current_user.name
                ]
                for user in self.users:
                    user.message_queue.append(
                        f"* {current_user.name} has left the room"
                    )

    def send_message(
        self, conn: socket.socket, message: str, socket_lock: threading.Lock
    ) -> None:
        try:
            with socket_lock:
                conn.sendall(message.encode() + b"\n")
        except (socket.timeout, ConnectionResetError, BrokenPipeError):
            with self.print_lock:
                print("Connection timed out")

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
