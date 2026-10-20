"""
wm_common.py  --  Librería común de WaterManagement (SD 26/27)

FUENTE ÚNICA: este fichero vive en /shared y se copia (idéntico) a WM_Central/,
WM_WS/ y WM_FO/ con `./sync_common.sh`, para que cada módulo sea autocontenido
y se pueda construir con su propio Dockerfile (Railway usa la carpeta del módulo
como contexto de build).

Contiene:
  1. Protocolo de sockets:  <STX><DATA><ETX><LRC>  con respuesta <ACK>/<NACK>
  2. Utilidades de configuración y de log por consola
  3. Capa Kafka (KafkaBus) sobre confluent-kafka
"""
import json
import os
import socket
import sys
import threading
import time
import uuid

# ---------------------------------------------------------------------------
# 1. PROTOCOLO DE SOCKETS  <STX><DATA><ETX><LRC>
# ---------------------------------------------------------------------------
STX = b"\x02"
ETX = b"\x03"
ACK = b"\x06"
NACK = b"\x15"
SEP = "#"  # separador de campos dentro de DATA:  OPERACION#campo1#campo2...


class ConnectionClosed(Exception):
    """El otro extremo cerró (o se cayó) la conexión."""


class ProtocolError(Exception):
    """Trama mal formada o NACK repetido."""


def lrc(data: bytes) -> int:
    """LRC = XOR byte a byte del mensaje (DATA)."""
    x = 0
    for b in data:
        x ^= b
    return x


def build_frame(msg: str) -> bytes:
    data = msg.encode("utf-8")
    return STX + data + ETX + bytes([lrc(data)])


def clean(value) -> str:
    """Un campo no puede contener el separador ni caracteres de control."""
    s = str(value).replace(SEP, "-")
    return "".join(ch for ch in s if ch >= " ")


def make_msg(*fields) -> str:
    return SEP.join(clean(f) for f in fields)


def split_msg(msg: str):
    return msg.split(SEP)


class FramedConn:
    """
    Envuelve un socket TCP y habla el protocolo <STX><DATA><ETX><LRC>.

    send(msg): envía la trama y espera <ACK>; si recibe <NACK> reintenta (3 veces).
    recv():    lee una trama, comprueba el LRC y responde <ACK> (o <NACK> si el
               LRC no cuadra y espera la retransmisión).

    El diálogo en cada conexión es siempre petición -> respuesta (nunca hay dos
    tramas "cruzadas"), por lo que el byte que sigue a una trama enviada es
    siempre el ACK/NACK del otro extremo.
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buf = bytearray()
        self._wlock = threading.Lock()

    # -- bajo nivel ---------------------------------------------------------
    def _fill(self, timeout):
        self.sock.settimeout(timeout)
        try:
            chunk = self.sock.recv(4096)
        except socket.timeout:
            raise TimeoutError("timeout esperando datos")
        except OSError as e:
            raise ConnectionClosed(str(e))
        if not chunk:
            raise ConnectionClosed("conexión cerrada por el otro extremo")
        self.buf += chunk

    def _write(self, raw: bytes):
        try:
            with self._wlock:
                self.sock.settimeout(5)
                self.sock.sendall(raw)
        except OSError as e:
            raise ConnectionClosed(str(e))

    def _read_ack(self, timeout):
        deadline = time.time() + timeout
        while not self.buf:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise TimeoutError("timeout esperando ACK")
            self._fill(remaining)
        b = bytes(self.buf[:1])
        del self.buf[:1]
        return b

    def _extract(self):
        """Devuelve (data, lrc_ok) si hay una trama completa en el buffer."""
        i = self.buf.find(STX)
        if i < 0:
            self.buf.clear()  # basura sin STX
            return None
        if i > 0:
            del self.buf[:i]
        j = self.buf.find(ETX, 1)
        if j < 0 or len(self.buf) < j + 2:
            return None
        data = bytes(self.buf[1:j])
        recv_lrc = self.buf[j + 1]
        del self.buf[: j + 2]
        return data, recv_lrc == lrc(data)

    # -- API ----------------------------------------------------------------
    def send(self, msg: str, timeout: float = 3.0, retries: int = 3):
        frame = build_frame(msg)
        for _ in range(retries):
            self._write(frame)
            resp = self._read_ack(timeout)
            if resp == ACK:
                return
        raise ProtocolError("NACK repetido: la trama no llega íntegra")

    def recv(self, timeout=None) -> str:
        deadline = None if timeout is None else time.time() + timeout
        while True:
            got = self._extract()
            if got is not None:
                data, ok = got
                if ok:
                    self._write(ACK)
                    return data.decode("utf-8", errors="replace")
                self._write(NACK)
                continue
            remaining = None if deadline is None else deadline - time.time()
            if remaining is not None and remaining <= 0:
                raise TimeoutError("timeout esperando trama")
            self._fill(remaining)

    def request(self, msg: str, timeout: float = 3.0) -> str:
        """Envía una petición y devuelve la respuesta (ambas con ACK)."""
        self.send(msg, timeout=timeout)
        return self.recv(timeout=timeout)

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def connect(host: str, port: int, timeout: float = 3.0) -> FramedConn:
    s = socket.create_connection((host, port), timeout=timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return FramedConn(s)


# ---------------------------------------------------------------------------
# 2. CONFIGURACIÓN Y LOG
# ---------------------------------------------------------------------------
def parse_hostport(value: str, default_port: int = None):
    """'host:puerto' -> (host, puerto)."""
    value = value.strip()
    if ":" in value:
        h, p = value.rsplit(":", 1)
        return h, int(p)
    if default_port is None:
        raise ValueError(f"Se esperaba host:puerto y se recibió '{value}'")
    return value, default_port


def env(name: str, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


C = {
    "reset": "\033[0m", "bold": "\033[1m", "dim": "\033[2m",
    "red": "\033[31m", "green": "\033[32m", "yellow": "\033[33m",
    "blue": "\033[34m", "magenta": "\033[35m", "cyan": "\033[36m",
    "grey": "\033[90m", "orange": "\033[38;5;208m",
    "bg_green": "\033[42;30m", "bg_red": "\033[41;97m",
    "bg_orange": "\033[48;5;208;30m", "bg_grey": "\033[100;97m",
}
_use_color = sys.stdout.isatty() or os.environ.get("FORCE_COLOR") == "1"
if os.environ.get("NO_COLOR"):
    _use_color = False
_print_lock = threading.Lock()


def color(text, name):
    return f"{C[name]}{text}{C['reset']}" if _use_color else str(text)


def ts():
    return time.strftime("%H:%M:%S")


def log(tag: str, msg: str, col: str = None):
    """Línea de log con hora y etiqueta. Segura entre hilos."""
    line = f"{color(ts(), 'grey')} [{tag}] {color(msg, col) if col else msg}"
    with _print_lock:
        print(line, flush=True)


# ---------------------------------------------------------------------------
# 3. KAFKA
# ---------------------------------------------------------------------------
# Topics (1 partición, factor de replicación 1 -> broker único)
T_REQ = "wm.requests"       # FO / WS(manual) / Central  ->  Central   (peticiones de riego)
T_CMD = "wm.ws.commands"    # Central -> Engine                       (START/STOP/BLOCK/ACTIVATE/DENIED)
T_EVT = "wm.ws.events"      # Engine  -> Central                      (START_ACK/NACK, TELEMETRY, END)
T_OP = "wm.operators"       # Central -> FO                           (notificaciones al operario)
T_STATUS = "wm.status"      # Central -> todos                        (foto del estado de todas las WS)

# LogAppendTime: la marca de tiempo de cada mensaje la pone el BROKER (un único reloj de
# referencia). Así se puede saber cuánto "envejeció" un mensaje sin depender de que los
# relojes de los PCs de laboratorio estén sincronizados con los de la nube.
_LAT = {"message.timestamp.type": "LogAppendTime"}
TOPIC_CONFIG = {
    T_REQ: dict(_LAT), T_CMD: {"retention.ms": "3600000"}, T_EVT: dict(_LAT),
    T_OP: {}, T_STATUS: {"retention.ms": "600000"},
}


class BusError(Exception):
    pass


def new_id(prefix=""):
    return prefix + uuid.uuid4().hex[:8]


class KafkaBus:
    """
    Pequeña capa sobre confluent-kafka (la misma librería de los ejemplos de clase).

      - Productor con acks=all + idempotencia: no se duplican ni se pierden mensajes.
      - send()      asíncrono (telemetría, notificaciones)
      - send_sync() espera la confirmación del broker (transacciones críticas)
      - group_consumer(): consumidor con grupo y offsets confirmados a mano
          -> si el módulo se cae y vuelve, continúa donde lo dejó (no se pierde nada)
      - tail_consumer():  consumidor sin grupo que empieza en "lo último"
    """

    def __init__(self, bootstrap: str, client_id: str):
        from confluent_kafka import Producer  # import diferido
        self.bootstrap = bootstrap
        self.client_id = client_id
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 15000,
            "socket.timeout.ms": 10000,
        })
        self.ok = False                 # último resultado de entrega
        self.last_error = ""
        self._stop = False
        self._poller = threading.Thread(target=self._poll_loop, daemon=True)
        self._poller.start()

    def _poll_loop(self):
        while not self._stop:
            try:
                self.producer.poll(0.2)
            except Exception:
                time.sleep(0.2)

    def _delivery(self, err, msg):
        if err is not None:
            self.ok = False
            self.last_error = str(err)
        else:
            self.ok = True
            self.last_error = ""

    # -- topics -------------------------------------------------------------
    def ensure_topics(self, timeout: float = 20.0):
        """Crea los topics si no existen. Lanza BusError si Kafka no responde."""
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": self.bootstrap,
                             "socket.timeout.ms": 10000})
        try:
            md = admin.list_topics(timeout=timeout)
        except Exception as e:
            raise BusError(f"Kafka no responde en {self.bootstrap}: {e}")
        missing = [t for t in TOPIC_CONFIG if t not in md.topics]
        if missing:
            futures = admin.create_topics(
                [NewTopic(t, num_partitions=1, replication_factor=1,
                          config=TOPIC_CONFIG[t]) for t in missing])
            for t, f in futures.items():
                try:
                    f.result(timeout=timeout)
                except Exception as e:
                    if "TOPIC_ALREADY_EXISTS" not in str(e) and "already exists" not in str(e):
                        raise BusError(f"No se pudo crear el topic {t}: {e}")
        self.ok = True

    # -- producir -----------------------------------------------------------
    @staticmethod
    def encode(msg: dict) -> bytes:
        msg.setdefault("ts", time.time())
        return json.dumps(msg, separators=(",", ":")).encode("utf-8")

    def send(self, topic: str, msg: dict, key: str = None):
        try:
            self.producer.produce(topic, value=self.encode(msg),
                                  key=key, callback=self._delivery)
            self.producer.poll(0)
        except BufferError:
            raise BusError("cola del productor llena")
        except Exception as e:
            raise BusError(str(e))

    def send_sync(self, topic: str, msg: dict, key: str = None,
                  timeout: float = 5.0) -> bool:
        """Devuelve True sólo si el broker confirmó la escritura."""
        done = threading.Event()
        result = {"ok": False}

        def cb(err, m):
            result["ok"] = err is None
            if err is not None:
                self.last_error = str(err)
            else:
                self.ok = True
            done.set()

        try:
            self.producer.produce(topic, value=self.encode(msg), key=key, callback=cb)
        except Exception as e:
            self.last_error = str(e)
            return False
        return done.wait(timeout) and result["ok"]

    # -- consumir -----------------------------------------------------------
    def group_consumer(self, group: str, topics, from_start: bool = True):
        from confluent_kafka import Consumer
        c = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": group,
            "client.id": self.client_id,
            "auto.offset.reset": "earliest" if from_start else "latest",
            "enable.auto.commit": False,      # commit manual DESPUÉS de procesar
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
        })
        c.subscribe(list(topics))
        return c

    def tail_consumer(self, topics):
        """Consumidor sin grupo posicionado en el final actual de cada topic."""
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": "tail-" + new_id(),
            "client.id": self.client_id,
            "enable.auto.commit": False,
        })
        md = c.list_topics(timeout=10)
        assign = []
        for t in topics:
            if t not in md.topics:
                raise BusError(f"el topic {t} no existe")
            for p in md.topics[t].partitions:
                _lo, hi = c.get_watermark_offsets(TopicPartition(t, p), timeout=10)
                assign.append(TopicPartition(t, p, hi))
        c.assign(assign)
        return c

    def close(self):
        self._stop = True
        try:
            self.producer.flush(3)
        except Exception:
            pass


def poll_json(consumer, timeout: float = 1.0):
    """
    Lee un mensaje y lo devuelve como (dict, mensaje_kafka). (None, None) si no hay
    nada. Errores de Kafka no fatales se ignoran; los fatales se propagan.
    """
    msg = consumer.poll(timeout)
    if msg is None:
        return None, None
    err = msg.error()
    if err is not None:
        try:
            from confluent_kafka import KafkaError
            if err.code() == KafkaError._PARTITION_EOF:
                return None, None
        except Exception:
            pass
        raise BusError(str(err))
    try:
        return json.loads(msg.value().decode("utf-8")), msg
    except Exception:
        return {}, msg  # mensaje corrupto: se descarta pero se confirma


def msg_age(raw) -> float:
    """Segundos desde que el broker guardó el mensaje (0 si no hay marca de tiempo)."""
    try:
        _type, ms = raw.timestamp()
        if ms and ms > 0:
            return max(0.0, time.time() - ms / 1000.0)
    except Exception:
        pass
    return 0.0


def commit(consumer, msg):
    try:
        consumer.commit(message=msg, asynchronous=False)
    except Exception:
        pass
