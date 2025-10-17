import socket
import threading
from dataclasses import dataclass
import struct


@dataclass
class PriceData:
    timestamp: int
    price: int


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
        price_information: list[PriceData] = []
        buffer = b""
        try:
            while True:
                chunk: bytes = conn.recv(1024)
                if not chunk:
                    break
                buffer += chunk
                while len(buffer) >= 9:
                    binary_message = buffer[:9]
                    buffer = buffer[9:]
                    completed = self.handle_message(
                        conn, binary_message, price_information
                    )
                    if completed:
                        break
        except (socket.timeout, ConnectionResetError, BrokenPipeError):
            with self.print_lock:
                print("Connection timed out")
        finally:
            conn.close()

    def handle_message(
        self,
        conn: socket.socket,
        binary_message: bytes,
        price_information: list[PriceData],
    ) -> bool:
        if binary_message[:1] == b"I":
            self.insert_message(binary_message, price_information)
            return False
        elif binary_message[:1] == b"Q":
            self.query_message(binary_message, price_information, conn)
            return False
        else:
            return True

    def insert_message(
        self, binary_message: bytes, price_information: list[PriceData]
    ) -> None:

        decoded_timestamp, decoded_price = struct.unpack(">ii", binary_message[1:9])
        price_information.append(PriceData(decoded_timestamp, decoded_price))

    def query_message(
        self,
        binary_message: bytes,
        price_information: list[PriceData],
        conn: socket.socket,
    ):
        decoded_initial_timestamp, decoded_final_timestamp = struct.unpack(
            ">ii", binary_message[1:9]
        )
        prices: list[int] = []
        for price_data in price_information:
            if (
                decoded_initial_timestamp
                <= price_data.timestamp
                <= decoded_final_timestamp
            ):
                prices.append(price_data.price)
        if len(prices) == 0:
            data_to_send = struct.pack(">i", 0)
        else:
            average_price = sum(prices) // len(prices)
            data_to_send = struct.pack(">i", average_price)
        conn.sendall(data_to_send)

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
