import socket
from threading import Lock
import json
from time import sleep

from pwn import server


def is_prime(num: int | float) -> bool:
    if num < 2 or type(num) == float:
        return False
    for i in range(2, int(num**0.5) + 1):
        if num % i == 0:
            return False
    return True


class Server:
    def __init__(self) -> None:
        self.listener = server(4444, callback=self.process_connection)
        self.print_lock = Lock()
        print("Sever listening on 0.0.0.0:4444")
        sleep(30)

    def process_connection(self, conn) -> None:
        try:
            while True:
                data = conn.recvline().decode()
                try:
                    json_obj = json.loads(data)
                except json.decoder.JSONDecodeError:
                    with self.print_lock:
                        print(f"Malformed Json: {data}")
                    conn.sendline(b"Malformed")
                    conn.close()
                    return
                try:
                    assert json_obj["method"] == "isPrime"
                    number = json_obj["number"]
                    assert type(number) == int or type(number) == float
                except (AssertionError, KeyError):
                    with self.print_lock:
                        print(f"Error in Json: {data}")
                    conn.sendline(b"Malformed")
                    conn.close()
                    return
                with self.print_lock:
                    print(f"Accepting {data}")
                prime = is_prime(number)
                new_json = {"method": "isPrime", "prime": prime}
                conn.sendline(json.dumps(new_json).encode())

        except (socket.timeout, ConnectionResetError, BrokenPipeError, EOFError):
            with self.print_lock:
                print("Connection timed out")


def main() -> None:
    Server()


if __name__ == "__main__":
    main()
