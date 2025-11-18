import socket
import threading
import time
from dataclasses import dataclass
from enum import IntEnum
from queue import Queue, Empty


@dataclass
class Plate:
    plate: str
    timestamp: int
    road: int
    mile: int


@dataclass
class Ticket:
    plate: str
    road: int
    mile1: int
    timestamp1: int
    mile2: int
    timestamp2: int
    speed: int


@dataclass
class Client:
    heartbeat: int


@dataclass
class Camera(Client):
    road: int
    mile: int
    limit: int


@dataclass
class Dispatcher(Client):
    roads: list[int]
    conn: socket.socket
    connection_lock: threading.Lock


class MessageType(IntEnum):
    ERROR = 0x10
    PLATE = 0x20
    TICKET = 0x21
    WANTHEARTBEAT = 0x40
    HEARTBEAT = 0x41
    IAMCAMERA = 0x80
    IAMDISPATCHER = 0x81


class Server:
    def __init__(self, server: str, port: int) -> None:
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((server, port))
        self.sock.listen()
        self.print_lock = threading.Lock()
        print(f"Server listening on {server}:{port}")

    def handle_connections(self) -> None:
        self.plates: list[Plate] = []
        self.plates_lock = threading.Lock()
        self.dispatchers: list[Dispatcher] = []
        self.dispatchers_lock = threading.Lock()
        self.ticket_queue: Queue[Ticket] = Queue()
        tickets_thread = threading.Thread(
            target=self.process_ticket_queue, args=(), daemon=True
        )
        tickets_thread.start()
        while True:
            conn, _ = self.sock.accept()
            thread = threading.Thread(
                target=self.process_connection, args=(conn,), daemon=True
            )
            thread.start()

    def process_connection(self, conn: socket.socket) -> None:
        buffer = b""
        connection_lock = threading.Lock()
        client = Client(0)
        done = False
        try:
            while True:
                try:
                    chunk: bytes = conn.recv(1024)
                except OSError as e:
                    break
                if not chunk:
                    done = True
                buffer += chunk
                try:
                    while buffer:
                        message_type = buffer[0]
                        if message_type == MessageType.PLATE:
                            if not isinstance(client, Camera):
                                self.send_error(conn, connection_lock)
                                done = True
                                break
                            plate_bytes_len = 6 + buffer[1]
                            if plate_bytes_len > len(buffer):
                                break
                            plate_bytes = buffer[:plate_bytes_len]
                            plate = self.process_plate(plate_bytes, client)
                            buffer = buffer[plate_bytes_len:]
                            self.check_for_ticket(plate, client.limit)
                        elif message_type == MessageType.WANTHEARTBEAT:
                            if client.heartbeat != 0:
                                self.send_error(conn, connection_lock)
                                done = True
                                break
                            else:
                                heartbeat_bytes_len = 5
                                if heartbeat_bytes_len > len(buffer):
                                    break
                                heartbeat_bytes = buffer[:heartbeat_bytes_len]
                                client.heartbeat = int.from_bytes(
                                    heartbeat_bytes[1:5], "big"
                                )
                                heartbeat_thread = threading.Thread(
                                    target=self.process_heartbeat,
                                    args=(conn, connection_lock, heartbeat_bytes),
                                    daemon=True,
                                )
                                heartbeat_thread.start()
                                buffer = buffer[heartbeat_bytes_len:]
                        elif message_type == MessageType.IAMCAMERA:
                            if isinstance(client, (Camera, Dispatcher)):
                                self.send_error(conn, connection_lock)
                                done = True
                                break
                            camera_bytes_len = 7
                            if camera_bytes_len > len(buffer):
                                break
                            camera_bytes = buffer[:camera_bytes_len]
                            client = self.process_camera(camera_bytes)
                            buffer = buffer[camera_bytes_len:]
                        elif message_type == MessageType.IAMDISPATCHER:
                            if isinstance(client, (Camera, Dispatcher)):
                                self.send_error(conn, connection_lock)
                                done = True
                                break
                            dispatcher_bytes_len = 2 + (buffer[1] * 2)
                            if dispatcher_bytes_len > len(buffer):
                                break
                            dispatcher_bytes = buffer[:dispatcher_bytes_len]
                            client = self.process_dispatcher(
                                dispatcher_bytes, conn, connection_lock
                            )
                            buffer = buffer[dispatcher_bytes_len:]
                        else:
                            self.send_error(conn, connection_lock)
                            done = True
                            break
                    if done:
                        break
                except IndexError:
                    continue
                except UnicodeError:
                    self.send_error(conn, connection_lock)
                    break
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            with self.print_lock:
                print(f"Process Connection Send Error: {e}")
        finally:
            with self.dispatchers_lock:
                self.dispatchers = [
                    dispatcher
                    for dispatcher in self.dispatchers
                    if dispatcher.conn is not conn
                ]
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            conn.close()

    def send_error(self, conn: socket.socket, connection_lock: threading.Lock) -> None:
        error_message = "Bad Message"
        with connection_lock:
            conn.sendall(
                MessageType.ERROR.to_bytes(1, "big")
                + len(error_message).to_bytes(1, "big")
                + error_message.encode()
            )

    def process_plate(self, buffer: bytes, client: Camera) -> Plate:
        plate_length = buffer[1]
        plate_end = 2 + plate_length
        plate = buffer[2:plate_end].decode("ascii")
        timestamp = int.from_bytes(buffer[plate_end : plate_end + 4], "big")
        plate = Plate(plate, timestamp, client.road, client.mile)
        with self.plates_lock:
            self.plates.append(plate)
        return plate

    def check_for_ticket(self, plate_to_check: Plate, speed_limit: int) -> None:
        with self.plates_lock:
            for plate in self.plates:
                if not (
                    plate.road == plate_to_check.road
                    and plate.plate == plate_to_check.plate
                    and plate.mile != plate_to_check.mile
                ):
                    continue
                plate1, plate2 = (
                    (plate, plate_to_check)
                    if plate_to_check.timestamp > plate.timestamp
                    else (plate_to_check, plate)
                )
                distance = abs(plate2.mile - plate1.mile)
                time = plate2.timestamp - plate1.timestamp
                if time <= 0:
                    continue
                speed_100 = (distance * 3_600 * 100 + time // 2) // time
                if speed_100 >= speed_limit * 100 + 50:
                    self.issue_ticket(plate1, plate2, speed_100)

    def issue_ticket(self, plate1: Plate, plate2: Plate, speed_100: int) -> None:
        ticket = Ticket(
            plate1.plate,
            plate1.road,
            plate1.mile,
            plate1.timestamp,
            plate2.mile,
            plate2.timestamp,
            speed_100,
        )
        self.ticket_queue.put(ticket)
        return

    def process_ticket_queue(self) -> None:
        ticket_history: dict[str, set[int]] = {}
        while True:
            ticket_batch: list[Ticket] = []
            while True:
                try:
                    ticket = self.ticket_queue.get_nowait()
                except Empty:
                    break
                ticket_batch.append(ticket)
            if not ticket_batch:
                time.sleep(0.01)
            with self.dispatchers_lock:
                dispatchers_snapshot = list(self.dispatchers)
            requeue: list[Ticket] = []
            for ticket in ticket_batch:
                day1 = ticket.timestamp1 // 86400
                day2 = ticket.timestamp2 // 86400
                ticket_history_days = ticket_history.get(ticket.plate, set())
                if day1 in ticket_history_days or day2 in ticket_history_days:
                    continue
                sent = False
                for dispatcher in dispatchers_snapshot:
                    if ticket.road not in dispatcher.roads:
                        continue
                    try:
                        self.send_ticket(dispatcher, ticket)
                        ticket_history.setdefault(ticket.plate, set()).update(
                            (day1, day2)
                        )
                        sent = True
                        break
                    except (ConnectionResetError, BrokenPipeError, OSError):
                        continue
                if not sent:
                    requeue.append(ticket)
            if requeue:
                for ticket in requeue:
                    self.ticket_queue.put(ticket)

    def send_ticket(self, dispatcher: Dispatcher, ticket: Ticket) -> None:
        with dispatcher.connection_lock:
            dispatcher.conn.sendall(
                MessageType.TICKET.to_bytes(1, "big")
                + len(ticket.plate).to_bytes(1, "big")
                + ticket.plate.encode()
                + (ticket.road).to_bytes(2, "big")
                + (ticket.mile1).to_bytes(2, "big")
                + (ticket.timestamp1).to_bytes(4, "big")
                + (ticket.mile2).to_bytes(2, "big")
                + (ticket.timestamp2).to_bytes(4, "big")
                + (ticket.speed).to_bytes(2, "big")
            )

    def process_heartbeat(
        self, conn: socket.socket, connection_lock: threading.Lock, buffer: bytes
    ) -> None:
        interval = int.from_bytes(buffer[1:5], "big") / 10
        if interval == 0:
            return
        next_heartbeat = time.time() + interval
        while True:
            delay = next_heartbeat - time.time()
            if delay > 0:
                time.sleep(delay)
            next_heartbeat = time.time() + interval
            try:
                with connection_lock:
                    conn.sendall(MessageType.HEARTBEAT.to_bytes(1, "big"))
            except (
                ConnectionResetError,
                BrokenPipeError,
                OSError,
            ) as e:
                break

    def process_camera(self, buffer: bytes) -> Camera:
        road = int.from_bytes(buffer[1:3], "big")
        mile = int.from_bytes(buffer[3:5], "big")
        limit = int.from_bytes(buffer[5:7], "big")
        camera = Camera(0, road, mile, limit)
        return camera

    def process_dispatcher(
        self, buffer: bytes, conn: socket.socket, connection_lock: threading.Lock
    ) -> Dispatcher:
        numroads = buffer[1]
        roads = []
        idx = 2
        for _ in range(numroads):
            roads.append(int.from_bytes(buffer[idx : idx + 2], "big"))
            idx += 2
        dispatcher = Dispatcher(0, roads, conn, connection_lock)
        with self.dispatchers_lock:
            self.dispatchers.append(dispatcher)
        return dispatcher

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
