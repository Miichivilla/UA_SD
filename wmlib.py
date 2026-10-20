"""
wmlib.py - Librería común de WaterManagement (idéntica en WM_Central, WM_WS y WM_FO).

Contiene:
  1) Protocolo de sockets con tramas  <STX> DATA <ETX> <LRC>  y confirmación <ACK>/<NACK>.
  2) Utilidades de servidor concurrente / cliente TCP.
  3) Capa fina sobre Kafka (confluent-kafka): topics, productor, consumidor.
  4) Logging con colores y utilidades de parámetros.
"""
import json
import os
import socket
import sys
import threading
import time

# ----------------------------------------------------------------------------
# 4) Logging / utilidades
# ----------------------------------------------------------------------------
_COLOR_ON = "NO_COLOR" not in os.environ
COLORS = {
    "red": "\033[91m", "green": "\033[92m", "yellow": "\033[93m",
    "orange": "\033[38;5;208m", "gray": "\033[90m", "cyan": "\033[96m",
    "blue": "\033[94m", "bold": "\033[1m", "reset": "\033[0m",
}
_log_lock = threading.Lock()


def paint(text, color=None):
    if not color or not _COLOR_ON:
        return text
    return f"{COLORS[color]}{text}{COLORS['reset']}"


def log(tag, msg, color=None):
    """Línea de log con hora. Thread-safe."""
    line = f"{time.strftime('%H:%M:%S')} [{tag}] {msg}"
    with _log_lock:
        print(paint(line, color), flush=True)


def parse_hostport(tokens, default_port=None):
    """Acepta ['host:port'] o ['host', 'port'] y devuelve (host, port)."""
    if isinstance(tokens, str):
        tokens = [tokens]
    tokens = [t for t in tokens if t]
    if len(tokens) == 1 and ":" in tokens[0]:
        h, p = tokens[0].rsplit(":", 1)
        return h, int(p)
    if len(tokens) == 2:
        return tokens[0], int(tokens[1])
    if len(tokens) == 1 and default_port:
        return tokens[0], default_port
    raise ValueError(f"Se esperaba host:puerto (o 'host puerto'), recibido {tokens!r}")


def sanitize(value):
    """Los campos del protocolo se separan por '#': se elimina de los datos."""
    return str(value).replace("#", "_").replace("\x02", "").replace("\x03", "")


# ----------------------------------------------------------------------------
# 1) Protocolo de sockets: <STX> DATA <ETX> <LRC>
# ----------------------------------------------------------------------------
STX, ETX, ACK, NACK = 0x02, 0x03, 0x06, 0x15
SEP = "#"


class ProtocolError(Exception):
    pass


def lrc(data: bytes) -> int:
    """LRC = XOR byte a byte de los datos del mensaje."""
    x = 0
    for b in data:
        x ^= b
    return x


def build_frame(message: str) -> bytes:
    data = message.encode("utf-8")
    return bytes([STX]) + data + bytes([ETX, lrc(data)])


def pack(*fields) -> str:
    """Código de operación y campos separados por '#':  OP#campo1#campo2..."""
    return SEP.join(sanitize(f) for f in fields)


def unpack(message: str):
    return message.split(SEP)


class Channel:
    """
    Canal sobre un socket TCP que habla tramas <STX>DATA<ETX><LRC>.
    - send(): envía una trama y espera <ACK>; si recibe <NACK> reenvía (3 intentos).
    - recv(): recibe una trama, valida el LRC y contesta <ACK> (o <NACK> si no coincide).
    - request(): send + recv de la respuesta (protocolo estrictamente alternado).
    """

    def __init__(self, sock, name=""):
        self.sock = sock
        self.name = name
        self.buf = bytearray()
        self.send_lock = threading.Lock()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def _fill(self, deadline):
        remaining = None if deadline is None else deadline - time.time()
        if remaining is not None and remaining <= 0:
            raise socket.timeout("timeout")
        self.sock.settimeout(remaining)
        chunk = self.sock.recv(4096)
        if not chunk:
            raise ConnectionError("conexión cerrada por el otro extremo")
        self.buf.extend(chunk)

    def send(self, message: str, timeout=3.0, retries=3):
        frame = build_frame(message)
        with self.send_lock:
            for _ in range(retries):
                self.sock.settimeout(timeout)
                self.sock.sendall(frame)
                deadline = time.time() + timeout
                while not self.buf:
                    self._fill(deadline)
                ctrl = self.buf.pop(0)
                if ctrl == ACK:
                    return
                if ctrl == NACK:
                    continue  # reenviar
                raise ProtocolError(f"Se esperaba ACK/NACK y llegó 0x{ctrl:02x}")
            raise ProtocolError("trama rechazada 3 veces (NACK)")

    def recv(self, timeout=None) -> str:
        deadline = None if timeout is None else time.time() + timeout
        while True:
            # descartar basura anterior al STX
            while self.buf and self.buf[0] != STX:
                self.buf.pop(0)
            if self.buf:
                try:
                    etx = self.buf.index(ETX, 1)
                except ValueError:
                    etx = -1
                if etx != -1 and len(self.buf) > etx + 1:
                    data = bytes(self.buf[1:etx])
                    recv_lrc = self.buf[etx + 1]
                    del self.buf[:etx + 2]
                    if recv_lrc == lrc(data):
                        self._ctrl(ACK)
                        return data.decode("utf-8", errors="replace")
                    self._ctrl(NACK)
                    continue
            self._fill(deadline)

    def _ctrl(self, byte):
        with self.send_lock:
            self.sock.sendall(bytes([byte]))

    def request(self, message: str, timeout=3.0) -> str:
        self.send(message, timeout=timeout)
        return self.recv(timeout=timeout)


def connect(host, port, timeout=3.0) -> Channel:
    s = socket.create_connection((host, port), timeout=timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return Channel(s, f"{host}:{port}")


def serve(port, handler, name="server", bind="0.0.0.0"):
    """Servidor concurrente: un hilo por conexión. handler(Channel, addr)."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((bind, port))
    srv.listen(64)

    def wrap(conn, addr):
        ch = Channel(conn, f"{addr[0]}:{addr[1]}")
        try:
            handler(ch, addr)
        except (ConnectionError, socket.timeout, OSError, ProtocolError):
            pass
        except Exception as e:  # un fallo en una conexión nunca tumba el servidor
            log(name, f"error en conexión {addr}: {e!r}", "red")
        finally:
            ch.close()

    def loop():
        while True:
            try:
                conn, addr = srv.accept()
            except OSError:
                return
            threading.Thread(target=wrap, args=(conn, addr), daemon=True).start()

    t = threading.Thread(target=loop, daemon=True, name=f"{name}-accept")
    t.start()
    return srv


# ----------------------------------------------------------------------------
# 3) Kafka (confluent-kafka)
# ----------------------------------------------------------------------------
T_REQUESTS = "wm_requests"            # FO / WS(manual) / CENTRAL  -> CENTRAL
T_WS_CMD = "wm_ws_cmd"                # CENTRAL -> Engines (START, STOP, BLOCK, ACTIVATE)
T_WS_EVENTS = "wm_ws_events"          # Engines -> CENTRAL (HELLO, STARTED, TELEMETRY, FINISHED...)
T_FO_EVENTS = "wm_fo_events"          # CENTRAL -> FO (pasos, autorización, telemetría, resumen)
T_CENTRAL_STATUS = "wm_central_status"  # CENTRAL -> todos (latido + estado de todas las WS)
ALL_TOPICS = {
    T_REQUESTS: {}, T_WS_CMD: {}, T_WS_EVENTS: {}, T_FO_EVENTS: {},
    T_CENTRAL_STATUS: {"retention.ms": "600000"},
}

try:
    from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:  # pragma: no cover
    print("Falta la dependencia: pip install confluent-kafka", file=sys.stderr)
    raise


def ensure_topics(bootstrap, tries=10):
    """Crea los topics si no existen (idempotente). Reintenta si Kafka aún no responde."""
    for attempt in range(tries):
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap})
            futs = admin.create_topics(
                [NewTopic(t, num_partitions=1, replication_factor=1, config=cfg)
                 for t, cfg in ALL_TOPICS.items()], request_timeout=10)
            for t, f in futs.items():
                try:
                    f.result()
                except KafkaException as e:
                    if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                        raise
            return True
        except Exception as e:
            log("kafka", f"Kafka no disponible en {bootstrap} ({e}); reintento {attempt + 1}/{tries}", "yellow")
            time.sleep(2)
    return False


class BusProducer:
    """Productor con hilo de poll para servir callbacks. acks=all + idempotencia."""

    def __init__(self, bootstrap, client_id="wm"):
        self.p = Producer({
            "bootstrap.servers": bootstrap, "client.id": client_id,
            "acks": "all", "enable.idempotence": True,
            "message.timeout.ms": 120000, "socket.timeout.ms": 10000,
        })
        self._stop = False
        threading.Thread(target=self._poller, daemon=True, name="kafka-poll").start()

    def _poller(self):
        while not self._stop:
            self.p.poll(0.2)

    def send(self, topic, payload: dict, key=None, on_delivery=None):
        payload.setdefault("ts", time.time())
        data = json.dumps(payload).encode("utf-8")

        def cb(err, msg):
            if on_delivery:
                on_delivery(err)
            elif err is not None:
                log("kafka", f"entrega fallida en {topic}: {err}", "red")

        for _ in range(50):
            try:
                self.p.produce(topic, value=data, key=key, callback=cb)
                return
            except BufferError:
                time.sleep(0.1)
        if on_delivery:
            on_delivery("cola local llena")

    def flush(self, timeout=5):
        self.p.flush(timeout)


class BusConsumer:
    """Consumidor que devuelve (topic, dict). Commit manual opcional."""

    def __init__(self, bootstrap, group, topics, offset_reset="latest",
                 auto_commit=True, on_assign=None):
        self.auto_commit = auto_commit
        self.c = Consumer({
            "bootstrap.servers": bootstrap, "group.id": group,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": auto_commit,
            "session.timeout.ms": 10000,
        })
        kw = {}
        if on_assign:
            kw["on_assign"] = lambda c, parts: on_assign()
        self.c.subscribe(list(topics), **kw)
        self._last_err = 0
        self.last_msg = None

    def poll(self, timeout=1.0):
        msg = self.c.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            now = time.time()
            if now - self._last_err > 10:   # no inundar el log
                self._last_err = now
                log("kafka", f"aviso del cliente Kafka: {msg.error()}", "yellow")
            return None
        self.last_msg = msg
        try:
            return msg.topic(), json.loads(msg.value().decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            log("kafka", "mensaje no JSON descartado", "yellow")
            return None

    def commit(self):
        if self.last_msg is not None and not self.auto_commit:
            try:
                self.c.commit(message=self.last_msg, asynchronous=True)
            except KafkaException:
                pass

    def close(self):
        try:
            self.c.close()
        except Exception:
            pass
