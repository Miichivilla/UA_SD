"""
Módulo `confluent_kafka` FALSO para pruebas (misma superficie de API que usan los
módulos de la práctica). Habla con tests/fake_broker.py. No es Kafka real.
"""
import base64
import json
import socket
import threading
import time
from collections import deque

OFFSET_END = -1


class KafkaError:
    _PARTITION_EOF = -191
    _TRANSPORT = -195

    def __init__(self, code=0, text=""):
        self._code, self._text = code, text

    def code(self):
        return self._code

    def __str__(self):
        return self._text


class KafkaException(Exception):
    pass


class TopicPartition:
    def __init__(self, topic, partition=0, offset=-1001):
        self.topic, self.partition, self.offset = topic, partition, offset


def _addr(conf):
    h, p = conf["bootstrap.servers"].rsplit(":", 1)
    return h, int(p)


def _rpc(addr, req, timeout=5):
    try:
        with socket.create_connection(addr, timeout=timeout) as s:
            s.settimeout(timeout + req.get("wait", 0))
            s.sendall((json.dumps(req) + "\n").encode())
            buf = b""
            while not buf.endswith(b"\n"):
                chunk = s.recv(65536)
                if not chunk:
                    raise OSError("cerrada")
                buf += chunk
            return json.loads(buf)
    except (OSError, ValueError) as e:
        raise KafkaException(f"broker no disponible: {e}")


class Message:
    def __init__(self, topic, offset, key, value, ts=None):
        self._t, self._o, self._k, self._v, self._ts = topic, offset, key, value, ts

    def timestamp(self): return (2, int((self._ts or time.time()) * 1000))   # LogAppendTime

    def topic(self): return self._t
    def partition(self): return 0
    def offset(self): return self._o
    def key(self): return self._k
    def value(self): return self._v
    def error(self): return None


class _TopicMd:
    def __init__(self): self.partitions = {0: object()}


class _Md:
    def __init__(self, topics): self.topics = {t: _TopicMd() for t in topics}


class Producer:
    def __init__(self, conf):
        self.addr = _addr(conf)
        self.timeout = conf.get("message.timeout.ms", 15000) / 1000
        self.q = deque()
        self.done = deque()
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        threading.Thread(target=self._worker, daemon=True).start()

    def produce(self, topic, value=None, key=None, callback=None, **kw):
        with self.cv:
            self.q.append((topic, value, key, callback, time.time()))
            self.cv.notify()

    def _worker(self):
        while True:
            with self.cv:
                while not self.q:
                    self.cv.wait()
                topic, value, key, cb, t0 = self.q[0]
            v = base64.b64encode(value).decode()
            k = key if isinstance(key, str) else None
            try:
                r = _rpc(self.addr, {"op": "produce", "topic": topic, "k": k, "v": v}, timeout=2)
                err = None if r.get("ok") else KafkaError(-1, str(r))
            except KafkaException as e:
                if time.time() - t0 < self.timeout:
                    time.sleep(0.3)
                    continue
                err = KafkaError(KafkaError._TRANSPORT, str(e))
            with self.cv:
                self.q.popleft()
                self.done.append((cb, err, Message(topic, 0, key, value)))
                self.cv.notify_all()

    def poll(self, timeout=0):
        end = time.time() + timeout
        n = 0
        while True:
            with self.cv:
                item = self.done.popleft() if self.done else None
            if item:
                cb, err, msg = item
                if cb:
                    cb(err, msg)
                n += 1
                continue
            if time.time() >= end:
                return n
            time.sleep(0.02)

    def flush(self, timeout=None):
        end = time.time() + (timeout or 10)
        while time.time() < end:
            with self.cv:
                if not self.q and not self.done:
                    return 0
            self.poll(0.05)
        return len(self.q)

    def list_topics(self, topic=None, timeout=5):
        r = _rpc(self.addr, {"op": "topics"}, timeout)
        return _Md(r["topics"])


class Consumer:
    def __init__(self, conf):
        self.addr = _addr(conf)
        self.group = conf["group.id"]
        self.reset = conf.get("auto.offset.reset", "latest")
        self.pos = {}              # topic -> next offset
        self.topics = []
        self.manual = False
        self._rr = 0

    def subscribe(self, topics):
        self.topics = list(topics)

    def assign(self, tps):
        self.manual = True
        self.topics = [tp.topic for tp in tps]
        for tp in tps:
            self.pos[tp.topic] = tp.offset

    def assignment(self):
        return [TopicPartition(t, 0) for t in self.topics]

    def _position(self, topic):
        if topic in self.pos:
            return self.pos[topic]
        c = _rpc(self.addr, {"op": "committed", "group": self.group, "topic": topic}).get("offset")
        if c is not None:
            off = c
        elif self.reset == "earliest":
            off = 0
        else:
            off = _rpc(self.addr, {"op": "wm", "topic": topic})["hi"]
        self.pos[topic] = off
        return off

    def poll(self, timeout=1.0):
        end = time.time() + timeout
        try:
            while True:
                for i in range(len(self.topics)):
                    t = self.topics[(self._rr + i) % len(self.topics)]
                    r = _rpc(self.addr, {"op": "fetch", "topic": t, "offset": self._position(t), "wait": 0})
                    m = r.get("msg")
                    if m:
                        self.pos[t] = m["offset"] + 1
                        self._rr += 1
                        return Message(t, m["offset"], m.get("k"), base64.b64decode(m["v"]), m.get("ts"))
                if time.time() >= end:
                    return None
                time.sleep(0.05)
        except KafkaException:
            time.sleep(min(0.5, max(0, end - time.time())))
            return None

    def commit(self, message=None, asynchronous=True):
        _rpc(self.addr, {"op": "commit", "group": self.group, "topic": message.topic(),
                         "offset": message.offset() + 1})

    def list_topics(self, topic=None, timeout=5):
        return _Md(_rpc(self.addr, {"op": "topics"}, timeout)["topics"])

    def get_watermark_offsets(self, tp, timeout=5):
        return 0, _rpc(self.addr, {"op": "wm", "topic": tp.topic}, timeout)["hi"]

    def close(self):
        pass
