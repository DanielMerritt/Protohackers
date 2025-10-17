import socket


class Server:
    def __init__(self, server: str, port: int) -> None:
        self.sock = socket.socket(type=socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((server, port))
        print(f"Server listening on {server}:{port}")

    def handle_connections(self) -> None:
        self.database: dict[str, str] = {"version": "Unusual Database v1.0"}
        while True:
            message, conn_info = self.sock.recvfrom(1024)
            message = message.decode()
            if "=" in message:
                key, value = message.split("=", maxsplit=1)
                if key == "version":
                    continue
                else:
                    self.database[key] = value
            else:
                if message not in self.database:
                    continue
                response = f"{message}={self.database[message]}".encode()
                self.sock.sendto(response, conn_info)

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
