import json
import os
import socket
import sys
import threading
import time
import uuid
from datetime import datetime

# 1. utilidades
_COLORS = {
    "red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m", "blue": "\033[94m",
    "cyan": "\033[96m", "orange": "\033[38;5;208m", "gray": "\033[90m", "bold": "\033[1m",
}
_RESET = "\033[0m"
_print_lock = threading.Lock()

if os.name == "nt":
    os.system("")

def color(text, col=None):
    if not col or col not in _COLORS:
        return str(text)
    return f"{_COLORS[col]}{text}{_RESET}"

def log(tag, msg, col=None):
    ts = datetime.now().strftime("%H:%M:%S")
    with _print_lock:
        print(f"[{ts}] [{tag}] {color(msg, col)}", flush=True)

def env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default

def new_id(prefix="R"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

def parse_hostport(s, default_port=None):
    s = str(s).strip()
    if ":" in s:
        h, p = s.rsplit(":", 1)
        return h, int(p)
    if default_port is None:
        raise ValueError(f"Se esperaba host:puerto y se recibió '{s}'")
    return s, int(default_port)

def split_msg(msg):
    return (msg or "").split("#")

#  2. sockets
STX, ETX, ACK, NACK = b"\x02", b"\x03", b"\x06", b"\x15"
FORMAT = "utf-8"
MAX_DATA = 4096
MAX_RETRIES = 3

class ConnectionClosed(Exception):"""El otro extremo ha cerrado la conexión."""
class ProtocolError(Exception): """Trama mal formada, LRC incorrecto repetidamente o respuesta inesperada."""
class BusError(Exception): """Error de Kafka."""

def lrc(data: bytes) -> int:
    x = 0
    for b in data:
        x ^= b
    return x

def build_frame(text: str) -> bytes:
    data = text.encode(FORMAT)
    return STX + data + ETX + bytes([lrc(data)])

class FramedConn:
    def __init__(self, sock, peer=None):
        self.sock = sock
        self.peer = peer
        self.buf = b""
        self.lock = threading.Lock()

    def _read(self, n, timeout):
        self.sock.settimeout(timeout)
        while len(self.buf) < n:
            chunk = self.sock.recv(1024)          # socket.timeout (=TimeoutError) si vence
            if not chunk:
                raise ConnectionClosed("conexión cerrada por el otro extremo")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def _read_frame(self, timeout):
        # 1) buscar STX (se descarta cualquier basura previa)
        while self._read(1, timeout) != STX:
            pass
        # 2) leer hasta ETX
        data = b""
        while True:
            b = self._read(1, timeout)
            if b == ETX:
                break
            data += b
            if len(data) > MAX_DATA:
                raise ProtocolError("trama demasiado larga")
        # 3) LRC
        return data, self._read(1, timeout)[0]

    # -- API pública
    def send(self, text, timeout=5):
        frame = build_frame(text)
        for _ in range(MAX_RETRIES):
            self.sock.sendall(frame)
            if self._read(1, timeout) == ACK:
                return
        raise ProtocolError("el receptor rechazó la trama (NACK) repetidamente")

    def recv(self, timeout=None):
        for _ in range(MAX_RETRIES):
            data, rx_lrc = self._read_frame(timeout)
            if lrc(data) == rx_lrc:
                self.sock.sendall(ACK)
                return data.decode(FORMAT)
            self.sock.sendall(NACK)
        raise ProtocolError("LRC incorrecto repetidamente")

    def request(self, text, timeout=5):
        """send() + recv() de la respuesta."""
        with self.lock:
            self.send(text, timeout)
            return self.recv(timeout)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

def connect(host, port, timeout=5):
    s = socket.create_connection((host, int(port)), timeout=timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return FramedConn(s, (host, port))

# 3. Kafka
T_REQ = "wm.requests"        # FO / WS (riego manual) -> Central : peticiones de riego
T_CMD = "wm.ws.commands"     # Central -> Engine : START, STOP, BLOCK, ACTIVATE, DENIED
T_EVT = "wm.ws.events"       # Engine -> Central : START_ACK/NACK, TELEMETRY, END
T_FO = "wm.fo.notify"        # Central -> FO : INFO, AUTHORIZED, DENIED, TELEMETRY, INCIDENT, END
T_STATUS = "wm.status"       # Central -> todos : estado de todas las WS cada segundo (latido)
ALL_TOPICS = [T_REQ, T_CMD, T_EVT, T_FO, T_STATUS]

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition, OFFSET_END
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:            # permite importar el módulo sin Kafka (p. ej. el Monitor)
    Producer = None


class KafkaBus:
    def __init__(self, bootstrap, client_id="wm"):
        if Producer is None:
            raise BusError("falta la librería confluent-kafka (pip install confluent-kafka)")
        self.bootstrap = bootstrap
        self.client_id = client_id
        self.admin = AdminClient({"bootstrap.servers": bootstrap})
        try:
            self.admin.list_topics(timeout=5)          # comprueba que el broker responde
        except KafkaException as e:
            raise BusError(f"broker no disponible: {e}")
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,               # sin duplicados al reintentar
            "message.timeout.ms": 15000,
        })

    def ensure_topics(self):
        cfg = {T_STATUS: {"retention.ms": "60000"}}  # el latido no hace falta guardarlo
        new = [NewTopic(t, num_partitions=1, replication_factor=1, config=cfg.get(t, {}))
               for t in ALL_TOPICS]
        for t, fut in self.admin.create_topics(new).items():
            try:
                fut.result(10)
            except KafkaException as e:
                if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise

    @staticmethod
    def _encode(obj):
        obj = dict(obj)
        obj.setdefault("ts", time.time())
        return json.dumps(obj).encode(FORMAT)

    def send(self, topic, obj, key=None):
        self.producer.produce(topic, self._encode(obj), key=key)
        self.producer.poll(0)

    def send_sync(self, topic, obj, key=None, timeout=5):
        res = {}

        def cb(err, _msg):
            res["err"] = err
        try:
            self.producer.produce(topic, self._encode(obj), key=key, on_delivery=cb)
        except Exception:
            return False
        end = time.time() + timeout
        while "err" not in res and time.time() < end:
            self.producer.poll(0.1)
        return "err" in res and res["err"] is None

    def tail_consumer(self, topics):
        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": f"tail-{uuid.uuid4().hex}",
                      "enable.auto.commit": False, "auto.offset.reset": "latest"})
        parts = []
        md = c.list_topics(timeout=10)
        for t in topics:
            for p in md.topics[t].partitions:
                parts.append(TopicPartition(t, p, OFFSET_END))
        c.assign(parts)
        return c

    def group_consumer(self, topics, group):
        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": group, "enable.auto.commit": True, "auto.offset.reset": "earliest"})
        c.subscribe(topics)
        return c


def poll_json(consumer, timeout=1.0):
    msg = consumer.poll(timeout)
    if msg is None or msg.error():
        return None, None
    raw = msg.value()
    try:
        return json.loads(raw.decode(FORMAT)), raw
    except Exception:
        return None, raw


def connect_kafka(bootstrap, client_id, say):
    warned = False
    while True:
        try:
            bus = KafkaBus(bootstrap, client_id)
            bus.ensure_topics()
            say(f"Conectado a Kafka ({bootstrap})", "green")
            return bus
        except Exception as e:
            if not warned:
                say(f"Imposible conectar con Kafka ({bootstrap}): {e}. Reintentando...", "red")
                warned = True
            time.sleep(3)
