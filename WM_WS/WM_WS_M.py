#!/usr/bin/env python3
"""
WM_WS_M  --  Monitor de una Watering Station (SD 26/27)

  * Se AUTENTICA (o se da de alta con --register) en WM_Central por sockets.
  * Cada segundo envía a WM_Central un latido con la salud de la WS (HB#id#OK|KO).
    Si el monitor se cae, Central deja de recibir latidos -> WS "Desconectada".
  * Es SERVIDOR de sockets para su Engine (WM_WS_E): cada segundo le envía un
    PING de salud. Si el engine no responde o contesta KO, el monitor notifica la
    avería (fuga) a Central en el siguiente latido -> WS en ROJO.
  * Le comunica al engine el estado de Central (conectada/caída) y si la WS está
    bloqueada (Out of Service).

Parámetros (línea de comandos o variable de entorno):
  --id           / WS_ID              identificador único de la WS (obligatorio)
  --central      / WM_CENTRAL         host:puerto de WM_Central
  --engine-port  / WM_ENGINE_PORT     puerto donde escucha al Engine           (6000)
  --location     / WS_LOCATION        ubicación (sólo para --register)
  --register                          dar de alta la WS si Central no la conoce
"""
import argparse
import os
import socket
import sys
import threading
import time
from collections import deque

from wm_common import (ConnectionClosed, FramedConn, ProtocolError, color, connect, env, log,
                       make_msg, parse_hostport, split_msg)

STATE_COL = {"AVAILABLE": "green", "WATERING": "green", "LEAK": "red",
             "OUT_OF_SERVICE": "orange", "DISCONNECTED": "grey"}


class Monitor:
    def __init__(self, args):
        self.args = args
        self.ws_id = args.id
        self.location = args.location or ""
        self.chost, self.cport = parse_hostport(args.central)
        self.lock = threading.RLock()
        self.authed = False
        self.denied = None
        self.central_up = False
        self.central_info = {"state": "-", "blocked": 0, "flow": "0", "volume": "0", "op": "-"}
        self.engine_state = "ABSENT"       # ABSENT | OK | KO
        self.engine_conn = None
        self.events = deque(maxlen=10)
        self.live = sys.stdout.isatty() and env("WM_UI", "1") == "1"
        self._last_central_err = None

    # ------------------------------------------------------------------ eventos
    def event(self, msg, col=None):
        line = f"{time.strftime('%H:%M:%S')} {msg}"
        with self.lock:
            self.events.append(line if not col else color(line, col))
        if not self.live:
            log(f"{self.ws_id} MONITOR", msg, col)

    @property
    def health(self):
        return "OK" if self.engine_state == "OK" else "KO"

    # ------------------------------------------------------------------ enlace con Central
    def central_loop(self):
        while True:
            fc = None
            try:
                fc = connect(self.chost, self.cport, timeout=4)
                if self.args.register:
                    first = make_msg("REGISTER", self.ws_id, self.location or "Sin ubicación")
                else:
                    first = make_msg("AUTH", self.ws_id)
                r = split_msg(fc.request(first, timeout=6))
                if r[0] != "AUTH_OK":
                    self.denied = r[1] if len(r) > 1 else "denegada"
                    self.event(f"AUTENTICACIÓN DENEGADA por WM_Central: {self.denied}", "red")
                    time.sleep(1)           # deja que la pantalla se repinte
                    self.shutdown(2)
                self.location = r[2] if len(r) > 2 else self.location
                with self.lock:
                    self.authed, self.central_up = True, True
                self._last_central_err = None
                self.event(f"Autenticada en WM_Central ({self.chost}:{self.cport}) · ubicación: {self.location}", "green")
                while True:
                    reply = split_msg(fc.request(make_msg("HB", self.ws_id, self.health), timeout=4))
                    if reply[0] == "HB_OK" and len(reply) >= 6:
                        with self.lock:
                            self.central_info = {"state": reply[1], "blocked": int(reply[2]),
                                                 "flow": reply[3], "volume": reply[4], "op": reply[5]}
                    time.sleep(1)
            except (ConnectionClosed, ProtocolError, TimeoutError, OSError) as e:
                with self.lock:
                    self.authed = False
                    was_up, self.central_up = self.central_up, False
                    self.central_info["state"] = "-"
                msg = f"Imposible conectar con WM_Central ({self.chost}:{self.cport}): {e}"
                if was_up or msg != self._last_central_err:
                    self.event(msg, "red")
                    self._last_central_err = msg
                time.sleep(2)
            finally:
                if fc:
                    fc.close()

    # ------------------------------------------------------------------ servidor para el Engine
    def engine_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.args.engine_port))
        srv.listen(5)
        self.event(f"Esperando al Engine en el puerto {self.args.engine_port}", "cyan")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle_engine, args=(conn, addr), daemon=True).start()

    def handle_engine(self, sock, addr):
        fc = FramedConn(sock)
        with self.lock:
            if self.engine_conn is not None:
                self.engine_conn.close()          # un engine nuevo sustituye al anterior
            self.engine_conn = fc
        try:
            while True:                            # --- handshake: HELLO -> ID
                m = split_msg(fc.recv(timeout=60))
                if m[0] != "HELLO":
                    fc.send("ERR#Se esperaba HELLO")
                    continue
                if self.denied:
                    fc.send(make_msg("DENIED", self.denied))
                    return
                if self.authed:
                    fc.send(make_msg("ID", self.ws_id, self.location))
                    break
                fc.send("WAIT#Esperando autenticación del monitor con WM_Central")
            m = split_msg(fc.recv(timeout=60))     # --- READY (el engine ya tiene Kafka)
            if m[0] != "READY":
                return
            fc.send("OK")
            self.event(f"Engine conectado desde {addr[0]}", "green")
            while True:                            # --- salud cada segundo
                time.sleep(1)
                with self.lock:
                    c_up = 1 if self.central_up else 0
                    blocked = self.central_info["blocked"] if self.central_up else -1
                fc.send(make_msg("PING", c_up, blocked), timeout=2)
                resp = split_msg(fc.recv(timeout=1.5))
                new = "OK" if resp[0] == "OK" else "KO"
                if new != self.engine_state:
                    self.event("Engine reporta " + ("OK: avería resuelta" if new == "OK"
                               else "KO: fuga/avería detectada"), "green" if new == "OK" else "red")
                self.engine_state = new
        except (ConnectionClosed, ProtocolError, TimeoutError, OSError) as e:
            with self.lock:
                if self.engine_conn is fc:
                    if self.engine_state != "ABSENT":
                        self.event(f"Engine NO responde ({e}) -> se notifica FUGA/avería a Central", "red")
                    self.engine_state = "ABSENT"
                    self.engine_conn = None
        finally:
            fc.close()

    # ------------------------------------------------------------------ pantalla
    def render_loop(self):
        while True:
            time.sleep(1)
            if not self.live:
                continue
            with self.lock:
                ci, es = dict(self.central_info), self.engine_state
                cup, evs = self.central_up, list(self.events)
            out = ["\033[H\033[2J" + color("=" * 66, "cyan"),
                   color(f"  WM_WS_M · MONITOR de {self.ws_id}  ·  {self.location}", "bold"),
                   color("=" * 66, "cyan")]
            out.append("  WM_Central : " + (color(f"CONECTADA ({self.chost}:{self.cport})", "green") if cup
                       else color("Imposible conectar con WM_Central", "red")))
            eng = {"OK": color("OK", "green"), "KO": color("KO · fuga/avería", "red"),
                   "ABSENT": color("NO CONECTADO (se notifica avería)", "red")}[es]
            out.append(f"  Engine     : {eng}")
            st = ci["state"]
            out.append("  Estado WS  : " + (color(f" {st} ", {"AVAILABLE": "bg_green", "WATERING": "bg_green",
                       "LEAK": "bg_red", "OUT_OF_SERVICE": "bg_orange", "DISCONNECTED": "bg_grey"}.get(st, "dim"))
                       if cup and st != "-" else color("desconocido (sin Central)", "grey")))
            if cup and st == "WATERING":
                out.append(f"  Riego      : {ci['flow']} L/min · {ci['volume']} L · operario {ci['op']}")
            out.append(f"  Latido     : enviando HB#{self.ws_id}#{self.health} cada 1 s")
            out.append(color("-" * 66, "grey"))
            out.extend("  " + e for e in evs)
            print("\n".join(out), flush=True)

    def shutdown(self, code):
        os._exit(code)


def main():
    ap = argparse.ArgumentParser(description="WM_WS_M - Monitor de la Watering Station")
    ap.add_argument("--id", default=env("WS_ID"), required=env("WS_ID") is None, help="ID de la WS")
    ap.add_argument("--central", default=env("WM_CENTRAL", "localhost:5050"), help="host:puerto de WM_Central")
    ap.add_argument("--engine-port", type=int, default=int(env("WM_ENGINE_PORT", 6000)),
                    help="puerto del servidor de sockets que ofrece al WM_WS_E")
    ap.add_argument("--location", default=env("WS_LOCATION", ""), help="ubicación (con --register)")
    ap.add_argument("--register", action="store_true", default=env("WS_REGISTER") == "1",
                    help="dar de alta la WS en Central si no existe")
    args = ap.parse_args()
    m = Monitor(args)
    for t in (m.central_loop, m.engine_server, m.render_loop):
        threading.Thread(target=t, daemon=True).start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nMonitor detenido.")


if __name__ == "__main__":
    main()
