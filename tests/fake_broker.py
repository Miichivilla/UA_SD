#!/usr/bin/env python3
"""
Broker Kafka FALSO (sólo para pruebas locales sin Kafka/Docker).
Emula: topics de 1 partición, offsets, grupos con commit y long-poll.
Habla JSON por líneas con el paquete tests/fakelib/confluent_kafka.
NO sustituye a Kafka real: sirve para ejercitar la lógica de la práctica.
"""
import base64
import json
import socketserver
import sys
import threading
import time

LOG = {}            # topic -> [ {k, v, ts} ]
COMMITS = {}        # (group, topic) -> offset
COND = threading.Condition()


class H(socketserver.StreamRequestHandler):
    def handle(self):
        for line in self.rfile:
            try:
                req = json.loads(line)
            except Exception:
                return
            op = req["op"]
            res = {"ok": True}
            with COND:
                if op == "topics":
                    res["topics"] = list(LOG)
                elif op == "create":
                    if req["topic"] in LOG:
                        res = {"ok": False, "err": "TOPIC_ALREADY_EXISTS"}
                    else:
                        LOG[req["topic"]] = []
                elif op == "produce":
                    LOG.setdefault(req["topic"], []).append(
                        {"k": req.get("k"), "v": req["v"], "ts": time.time()})
                    res["offset"] = len(LOG[req["topic"]]) - 1
                    COND.notify_all()
                elif op == "wm":
                    res["hi"] = len(LOG.get(req["topic"], []))
                elif op == "committed":
                    res["offset"] = COMMITS.get((req["group"], req["topic"]))
                elif op == "commit":
                    COMMITS[(req["group"], req["topic"])] = req["offset"]
                elif op == "fetch":
                    end = time.time() + req.get("wait", 0)
                    while True:
                        log = LOG.get(req["topic"], [])
                        if req["offset"] < len(log):
                            res["msg"] = dict(log[req["offset"]], offset=req["offset"])
                            break
                        left = end - time.time()
                        if left <= 0:
                            res["msg"] = None
                            break
                        COND.wait(left)
            self.wfile.write((json.dumps(res) + "\n").encode())
            self.wfile.flush()


class S(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9092
    print(f"[fake_broker] escuchando en {port}", flush=True)
    S(("127.0.0.1", port), H).serve_forever()
