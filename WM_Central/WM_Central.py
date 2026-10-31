import argparse
import json
import os
import socket
import sqlite3
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from wm_common import (T_CMD, T_EVT, T_FO, T_REQ, T_STATUS, ConnectionClosed, FramedConn, ProtocolError,
                       color, connect_kafka, env, log, new_id, poll_json, split_msg)

MAX_CONEXIONES = 50
HEARTBEAT_TIMEOUT = 3.5   # s sin latido del monitor -> DESCONECTADA
START_TIMEOUT = 6         # s esperando START_ACK del engine
MAX_REQ_AGE = 20          # s: peticiones más antiguas se rechazan (evita procesar respuestas viejas)

# Estados de las estaciones de riego
AVAILABLE, WATERING, LEAK, OUT, DISCONNECTED = "AVAILABLE", "WATERING", "LEAK", "OUT_OF_SERVICE", "DISCONNECTED"

# Etiquetas para mostrar en la interfaz
LABEL = {AVAILABLE: "Available", WATERING: "Watering", LEAK: "Leak", OUT: "Out of Service",
         DISCONNECTED: "Disconnected"}

# Colores de fondo para el panel de terminal
BG = {AVAILABLE: "\033[42;30m", WATERING: "\033[42;30;5m", LEAK: "\033[41;97m",
      OUT: "\033[48;5;208;30m", DISCONNECTED: "\033[100;97m"}

# Archivos web
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
WEB_FILES = {"/": ("panel.html", "text/html; charset=utf-8"),
             "/panel.html": ("panel.html", "text/html; charset=utf-8"),
             "/panel.css": ("panel.css", "text/css; charset=utf-8"),
             "/panel.js": ("panel.js", "application/javascript; charset=utf-8")}

EVENTS = deque(maxlen=200)


def add_event(msg, col=None):
    EVENTS.append({"ts": time.strftime("%H:%M:%S"), "msg": msg, "col": col})


def say(msg, col=None):
    log("CENTRAL", msg, col)
    add_event(msg, col)


class Database:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # Lock para evitar problemas de concurrencia entre hilos
        self.lock = threading.Lock()
        with self.lock:
            self.conn.execute("""CREATE TABLE IF NOT EXISTS
                                WM_Stations (id TEXT PRIMARY KEY, location TEXT, blocked INTEGER DEFAULT 0)""")
            self.conn.execute("""CREATE TABLE IF NOT EXISTS
                                Operators (id TEXT PRIMARY KEY, created REAL)""")
            self.conn.execute("""CREATE TABLE IF NOT EXISTS
                                Sessions (req_id TEXT PRIMARY KEY, ws TEXT, operator_id TEXT,
                                          duration INTEGER, started REAL)""")
            self.conn.execute("""CREATE TABLE IF NOT EXISTS
                                Waterings (req_id TEXT PRIMARY KEY, ws TEXT, operator_id TEXT, volume REAL,
                                           duration INTEGER, reason TEXT, ended REAL)""")
            self.conn.commit()

    def query(self, sql, params=()):
        with self.lock:
            cursor = self.conn.execute(sql, params)
            self.conn.commit()
            return cursor.fetchall()


class WSState:
    def __init__(self, ws_id, location, blocked=False):
        self.id = ws_id
        self.location = location
        self.blocked = blocked
        self.connected = False
        self.last_seen = 0.0
        self.connection_token = None   
        self.health = "OK"             
        self.health_msg = ""
        self.current_session = None    # dict(req_id, operator_id, duration, flow, volume, elapsed) o None
        self.total_watered = 0.0       # litros totales suministrados (histórico)

    def status(self):
        if not self.connected:
            return DISCONNECTED
        if self.health == "KO":
            return LEAK
        if self.blocked:
            return OUT
        if self.current_session:
            return WATERING
        return AVAILABLE


class Central:
    def __init__(self, args):
        self.args = args
        self.db = Database(args.db)
        self.lock = threading.RLock()     
        self.ws_states = {}              
        self.pending_requests = {}        # req_id -> petición esperando START_ACK
        self.interrupted_sessions = {}    # req_id -> sesión cortada por fuga/desconexión (falta el END)
        self.bus = None
        self.load_ws_states_from_db()

    # ------------------------------------------------------------------ arranque
    def load_ws_states_from_db(self):
        for ws_id, location, blocked in self.db.query("SELECT id, location, blocked FROM WM_Stations"):
            self.ws_states[ws_id] = WSState(ws_id, location, bool(blocked))

        for ws_id, total in self.db.query("SELECT ws, SUM(volume) FROM Waterings GROUP BY ws"):
            if ws_id in self.ws_states:
                self.ws_states[ws_id].total_watered = total or 0.0

        # riegos que estaban en curso cuando la Central se paró
        for req_id, ws_id, operator_id, duration, _ in self.db.query("SELECT * FROM Sessions"):
            if ws_id in self.ws_states:
                self.ws_states[ws_id].current_session = {"req_id": req_id, "operator_id": operator_id,
                                                         "duration": duration, "flow": 0.0,
                                                         "volume": 0.0, "elapsed": 0}

        say(f"Base de datos cargada: {len(self.ws_states)} estaciones registradas "
            f"({', '.join(self.ws_states) or 'ninguna'}). Todas DESCONECTADAS hasta que conecte su monitor.",
            "cyan")

    # ------------------------------------------------------------------ Kafka
    def kafka_thread(self):
        self.bus = connect_kafka(self.args.kafka, "wm-central", say)
        threading.Thread(target=self.consume, args=([T_REQ], "wm-central-req"), daemon=True).start()
        threading.Thread(target=self.consume, args=([T_EVT], "wm-central-evt"), daemon=True).start()

    def consume(self, topics, group):
        consumer = self.bus.group_consumer(topics, group)
        while True:
            try:
                msg, _ = poll_json(consumer, 1.0)
                if msg:
                    self.on_kafka(msg)
            except Exception as e:
                say(f"Error procesando mensaje de Kafka: {e}", "red")

    def cmd(self, ws_id, cmd_type, **kwargs):
        """Orden a un Engine por Kafka. La clave del mensaje debe ser "t"."""
        msg = {"t": cmd_type, "ws": ws_id, **kwargs}
        if not (self.bus and self.bus.send_sync(T_CMD, msg, key=ws_id)):
            say(f"No se pudo enviar la orden {cmd_type} a {ws_id} (Kafka no disponible)", "red")
            return False
        return True

    def notify_fo(self, operator_id, req_id, msg_type, text, **kwargs):
        """Mensaje para el operario (solo lo muestra ese operario)."""
        if not operator_id or operator_id == "CENTRAL" or operator_id.startswith("WS:") or not self.bus:
            return
        self.bus.send_sync(T_FO, {"t": msg_type, "op": operator_id, "req_id": req_id, "msg": text, **kwargs},
                           key=operator_id)

    def on_kafka(self, msg):
        msg_type = msg.get("t")
        if msg_type == "REQ":
            self.on_request(msg)
        elif msg_type in ("START_ACK", "START_NACK"):
            self.on_start_reply(msg)
        elif msg_type == "TELEMETRY":
            self.on_telemetry(msg)
        elif msg_type == "END":
            self.on_end(msg)

    def on_request(self, msg):
        req_id = msg.get("req_id")
        operator_id = msg.get("op", "?")
        ws_id = msg.get("ws")
        source = msg.get("src", "FO")
        duration = int(msg.get("duration", 30))

        if source == "FO":
            if not self.db.query("SELECT 1 FROM Operators WHERE id=?", (operator_id,)):
                self.db.query("INSERT INTO Operators VALUES (?, ?)", (operator_id, time.time()))
                say(f"Nuevo operario dado de alta: {operator_id}", "cyan")

        if time.time() - float(msg.get("ts", time.time())) > MAX_REQ_AGE:
            self.deny(req_id, operator_id, ws_id, source, "petición caducada (se envió con WM_Central caída)")
            return

        say(f"Petición {req_id} de {operator_id} para {ws_id} ({duration} s). Comprobando disponibilidad...",
            "cyan")
        self.start_watering(ws_id, operator_id, duration, req_id, source)

    def start_watering(self, ws_id, operator_id, duration, req_id=None, source="CENTRAL"):
        req_id = req_id or new_id("C")
        with self.lock:
            ws_state = self.ws_states.get(ws_id)
            if ws_state is None:
                return self.deny(req_id, operator_id, ws_id, source, "WS no registrada")

            status = ws_state.status()
            if status != AVAILABLE:
                return self.deny(req_id, operator_id, ws_id, source,
                                 f"WS no disponible (estado {LABEL[status]})")
            if any(p["ws"] == ws_id for p in self.pending_requests.values()):
                return self.deny(req_id, operator_id, ws_id, source, "la WS tiene otra activación pendiente")
            if not self.bus:
                return self.deny(req_id, operator_id, ws_id, source, "WM_Central sin conexión con Kafka")
            self.pending_requests[req_id] = {"ws": ws_id, "operator_id": operator_id, "duration": duration,
                                             "source": source, "t0": time.time()}

        self.notify_fo(operator_id, req_id, "INFO",
                       f"WM_Central: {ws_id} disponible. Solicitando autorización a la WS...", ws=ws_id)
        say(f"{ws_id} disponible: enviando START {req_id} a la WS", "cyan")
        if not self.cmd(ws_id, "START", req_id=req_id, op=operator_id, duration=duration):
            with self.lock:
                self.pending_requests.pop(req_id, None)
            self.deny(req_id, operator_id, ws_id, source, "no se pudo contactar con la WS")

    def deny(self, req_id, operator_id, ws_id, source, reason):
        say(f"Riego {req_id} de {operator_id} en {ws_id} DENEGADO: {reason}", "red")
        self.notify_fo(operator_id, req_id, "DENIED", f"Riego DENEGADO: {reason}", ws=ws_id)
        if source == "WS" and ws_id:
            self.cmd(ws_id, "DENIED", req_id=req_id, reason=reason)

    def on_start_reply(self, msg):
        req_id = msg.get("req_id")
        ws_id = msg.get("ws")

        with self.lock:
            pending = self.pending_requests.pop(req_id, None)
            if pending is None:
                return
            if msg["t"] == "START_ACK":
                self.ws_states[ws_id].current_session = {"req_id": req_id,
                                                         "operator_id": pending["operator_id"],
                                                         "duration": pending["duration"],
                                                         "flow": 0.0, "volume": 0.0, "elapsed": 0}
                self.db.query("INSERT OR REPLACE INTO Sessions VALUES (?, ?, ?, ?, ?)",
                              (req_id, ws_id, pending["operator_id"], pending["duration"], time.time()))

        if msg["t"] == "START_NACK":
            self.deny(req_id, pending["operator_id"], ws_id, pending["source"],
                      f"la WS rechazó el inicio: {msg.get('reason')}")
            return

        say(f"Riego {req_id} AUTORIZADO en {ws_id} para {pending['operator_id']} "
            f"({pending['duration']} s): electroválvula abierta", "green")
        self.notify_fo(pending["operator_id"], req_id, "AUTHORIZED",
                       f"Riego AUTORIZADO en {ws_id} ({pending['duration']} s). Electroválvula abierta.",
                       ws=ws_id)

    def on_telemetry(self, msg):
        ws_id = msg.get("ws")
        req_id = msg.get("req_id")

        with self.lock:
            ws_state = self.ws_states.get(ws_id)
            if not ws_state or req_id in self.interrupted_sessions:
                return
            if ws_state.current_session is None or ws_state.current_session["req_id"] != req_id:
                ws_state.current_session = {"req_id": req_id, "operator_id": "?", "duration": 0,
                                            "flow": 0.0, "volume": 0.0, "elapsed": 0}
            session = ws_state.current_session
            session.update(flow=msg.get("flow", 0.0), volume=msg.get("volume", 0.0),
                           elapsed=msg.get("elapsed", 0))
            operator_id = session["operator_id"]
            flow, volume = session["flow"], session["volume"]
            elapsed, duration = session["elapsed"], session["duration"]

        # solo al panel web (en la terminal ya se ve en la tabla)
        add_event(f"[{operator_id}] Regando en {ws_id}: {flow:.1f} L/min | {volume:.2f} L acumulados | "
                  f"{elapsed}/{duration} s")
        self.notify_fo(operator_id, req_id, "TELEMETRY", "", ws=ws_id, flow=flow, volume=volume,
                       elapsed=elapsed, duration=duration)

    def on_end(self, msg):
        ws_id = msg.get("ws")
        req_id = msg.get("req_id")
        volume = float(msg.get("volume", 0.0))
        duration = int(msg.get("duration", 0))
        reason = msg.get("reason", "?")
        operator_id = msg.get("op")

        with self.lock:
            ws_state = self.ws_states.get(ws_id)
            if ws_state and ws_state.current_session and ws_state.current_session["req_id"] == req_id:
                if ws_state.current_session["operator_id"] != "?":
                    operator_id = ws_state.current_session["operator_id"]
                ws_state.current_session = None
            self.interrupted_sessions.pop(req_id, None)

            if self.db.query("SELECT 1 FROM Waterings WHERE req_id=?", (req_id,)):
                return  
            self.db.query("DELETE FROM Sessions WHERE req_id=?", (req_id,))
            self.db.query("INSERT INTO Waterings VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (req_id, ws_id, operator_id, volume, duration, reason, time.time()))
            if ws_state:
                ws_state.total_watered += volume

        say(f"RIEGO FINALIZADO {req_id} en {ws_id} ({reason}): {volume:.2f} L en {duration} s. "
            f"Operario {operator_id}", "cyan")
        self.notify_fo(operator_id, req_id, "END",
                       f"Resumen final: {volume:.2f} L en {duration} s (motivo: {reason})",
                       ws=ws_id, volume=volume, duration=duration, reason=reason)

    def cut_session(self, ws_state, reason):
        """Fuga o desconexión durante un riego: avisa al FO; el END llegará cuando la WS se recupere."""
        session = ws_state.current_session
        if not session:
            return

        ws_state.current_session = None
        self.interrupted_sessions[session["req_id"]] = dict(session, ws=ws_state.id)
        say(f"Riego {session['req_id']} en {ws_state.id} INTERRUMPIDO: {reason}", "red")
        self.notify_fo(session["operator_id"], session["req_id"], "INCIDENT",
                       f"Riego interrumpido en {ws_state.id}: {reason}. Llevaba {session['volume']:.2f} L. "
                       f"El resumen final llegará cuando la WS se recupere.", ws=ws_state.id)
        self.cmd(ws_state.id, "STOP", req_id=session["req_id"],
                 reason="FUGA" if "fuga" in reason.lower() else "CENTRAL")

    def order(self, action, target, duration=30):
        with self.lock:
            targets = list(self.ws_states) if target.upper() == "ALL" else [target]

        for ws_id in targets:
            if ws_id not in self.ws_states:
                say(f"La WS {ws_id} no existe", "red")
                continue
            if action == "start":
                self.start_watering(ws_id, "CENTRAL", int(duration))
            elif action == "block":
                with self.lock:
                    self.ws_states[ws_id].blocked = True
                    self.db.query("UPDATE WM_Stations SET blocked=1 WHERE id=?", (ws_id,))
                self.cmd(ws_id, "BLOCK")
                say(f"{ws_id} BLOQUEADA: Out of Service", "orange")
            elif action == "activate":
                with self.lock:
                    self.ws_states[ws_id].blocked = False
                    self.db.query("UPDATE WM_Stations SET blocked=0 WHERE id=?", (ws_id,))
                self.cmd(ws_id, "ACTIVATE")
                say(f"{ws_id} ACTIVADA", "green")

    def socket_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", self.args.port))
        server.listen(MAX_CONEXIONES)
        say(f"Servidor de sockets escuchando en el puerto {self.args.port}", "green")
        while True:
            conn, addr = server.accept()
            # servidor CONCURRENTE: un hilo por monitor
            threading.Thread(target=self.handle_monitor, args=(conn, addr), daemon=True).start()

    def handle_monitor(self, sock, addr):
        fc = FramedConn(sock, addr)
        token = object()
        ws_id = None
        try:
            while True:
                m = split_msg(fc.recv(timeout=10))
                code = m[0]
                if code == "REGISTRO" and len(m) >= 3:
                    rid, loc = m[1], m[2]
                    with self.lock:
                        new = rid not in self.ws_states
                        if new:
                            self.ws_states[rid] = WSState(rid, loc)
                        else:
                            self.ws_states[rid].location = loc
                        self.db.query("INSERT INTO WM_Stations(id, location) VALUES (?, ?) "
                                      "ON CONFLICT(id) DO UPDATE SET location=excluded.location", (rid, loc))
                    say(f"[REGISTRO] WS {rid} {'registrada' if new else 'actualizada'} en '{loc}' "
                        f"(desde {addr[0]})", "cyan")
                    fc.send(f"OK#WS {rid} {'registrada' if new else 'actualizada'}")
                elif code == "AUTH" and len(m) >= 2:
                    rid = m[1]
                    with self.lock:
                        ws_state = self.ws_states.get(rid)
                        if ws_state is None:
                            reason = "WS no registrada en WM_Central"
                        elif ws_state.connected and time.time() - ws_state.last_seen < HEARTBEAT_TIMEOUT:
                            reason = "ya hay un monitor activo con ese ID"
                        else:
                            reason = None
                            ws_id = rid
                            ws_state.connected = True
                            ws_state.last_seen = time.time()
                            ws_state.connection_token = token
                            ws_state.health, ws_state.health_msg = "KO", "esperando al engine"
                    if reason:
                        say(f"[AUTH] WS {rid} DENEGADA: {reason}", "red")
                        fc.send(f"DENIED#{reason}")
                        return
                    say(f"[AUTH] WS {rid} autenticada (monitor {addr[0]}:{addr[1]})", "green")
                    fc.send(f"OK#{rid}#{ws_state.location}#{int(ws_state.blocked)}")
                elif code == "STATUS" and ws_id and len(m) >= 3:
                    self.on_health(ws_id, token, m[2], m[3] if len(m) > 3 else "")
                    fc.send(f"ACK#{int(self.ws_states[ws_id].blocked)}")
                elif code == "FIN":
                    return
                else:
                    fc.send("ERROR#mensaje no reconocido")
        except (ConnectionClosed, ProtocolError, OSError) as e:
            if ws_id:
                say(f"Conexión con el monitor de {ws_id} perdida ({e.__class__.__name__})", "red")
        finally:
            fc.close()
            if ws_id:
                self.set_disconnected(ws_id, token, "conexión con el monitor cerrada")

    def on_health(self, ws_id, token, health, reason):
        with self.lock:
            ws_state = self.ws_states[ws_id]
            if ws_state.connection_token is not token:
                return
            ws_state.last_seen = time.time()
            ws_state.connected = True
            old, old_reason = ws_state.health, ws_state.health_msg
            ws_state.health, ws_state.health_msg = health, reason

            if old != health or (health == "KO" and old_reason != reason):
                if health == "KO" and reason == "esperando al engine":
                    say(f"{ws_id}: monitor conectado, esperando a su engine", "yellow")
                elif health == "KO":
                    say(f"*** {ws_id}: AVERÍA/FUGA reportada por el monitor ({reason}) ***", "red")
                    self.cut_session(ws_state, f"fuga/avería detectada ({reason})")
                elif old_reason == "esperando al engine":
                    say(f"{ws_id}: monitor y engine conectados → WS DISPONIBLE", "green")
                else:
                    say(f"{ws_id}: avería resuelta, WS operativa", "green")

    def set_disconnected(self, ws_id, token, reason):
        with self.lock:
            ws_state = self.ws_states.get(ws_id)
            if not ws_state or ws_state.connection_token is not token or not ws_state.connected:
                return
            ws_state.connected = False
            ws_state.connection_token = None
            say(f"{ws_id} DESCONECTADA: {reason}", "gray")
            self.cut_session(ws_state, "WS desconectada de WM_Central")

    #hilo periódico que comprueba latidos de los monitores y expira peticiones pendientes
    def watchdog(self):
        last_panel = None
        while True:
            time.sleep(1)
            now = time.time()
            with self.lock:
                for ws_state in self.ws_states.values():
                    if ws_state.connected and now - ws_state.last_seen > HEARTBEAT_TIMEOUT:
                        self.set_disconnected(ws_state.id, ws_state.connection_token,
                                              "sin latidos del monitor")
                # peticiones que esperan START_ACK demasiado tiempo
                expired = [(r, p) for r, p in self.pending_requests.items() if now - p["t0"] > START_TIMEOUT]
                for req_id, _ in expired:
                    self.pending_requests.pop(req_id)

            for req_id, pending in expired:
                self.deny(req_id, pending["operator_id"], pending["ws"], pending["source"],
                          "la WS no responde")
                self.cmd(pending["ws"], "STOP", req_id=req_id, reason="CENTRAL")

            # latido para FO (saben que la Central vive) + lista de WS
            if self.bus:
                try:
                    self.bus.send(T_STATUS, {"t": "STATUS", "ws": self.snapshot()})
                except Exception:
                    pass

            panel = self.panel_text()
            if panel != last_panel:
                last_panel = panel
                print(panel, flush=True)

    def snapshot(self):
        with self.lock:
            out = []
            for ws_state in sorted(self.ws_states.values(), key=lambda w: w.id):
                state = ws_state.status()
                session = ws_state.current_session or {}
                out.append({"id": ws_state.id, "location": ws_state.location, "state": state,
                            "label": LABEL[state],
                            "flow": session.get("flow", 0.0), "volume": session.get("volume", 0.0),
                            "op": session.get("operator_id", ""), "elapsed": session.get("elapsed", 0),
                            "duration": session.get("duration", 0), "total": round(ws_state.total_watered, 2),
                            "reason": ws_state.health_msg if state == LEAK else "",
                            "health": ws_state.health if ws_state.connected else None,
                            "blocked": ws_state.blocked})
            return out

    def recent_waterings(self, limit=15):
        with self.lock:
            live = {w.current_session["req_id"]: w.current_session["volume"]
                    for w in self.ws_states.values() if w.current_session}
            cut = set(self.interrupted_sessions)

        rows = []
        for req_id, ws_id, operator_id, _duration, started in self.db.query(
                "SELECT req_id, ws, operator_id, duration, started FROM Sessions"):
            rows.append({"ts": started, "ws": ws_id, "op": operator_id, "volume": live.get(req_id, 0.0),
                         "reason": "", "state": "INTERRUMPIDO" if req_id in cut else "ACTIVE"})

        for _req_id, ws_id, operator_id, volume, _duration, reason, ended in self.db.query(
                "SELECT * FROM Waterings ORDER BY ended DESC LIMIT ?", (limit,)):
            rows.append({"ts": ended, "ws": ws_id, "op": operator_id, "volume": volume,
                         "reason": reason, "state": "FINALIZADO"})

        rows.sort(key=lambda r: r["ts"] or 0, reverse=True)
        return rows[:limit]

    def panel_text(self):
        rows = self.snapshot()
        lines = ["", color("=" * 92, "cyan"),
                 color(f"  WM_CENTRAL · Panel de monitorización   Kafka: {'OK' if self.bus else 'CAÍDO'}"
                       f"   Sockets: {self.args.port}", "bold"),
                 color("=" * 92, "cyan"),
                 f"  {'ID':<12}{'UBICACIÓN':<18}{'ESTADO':<16}{'CAUDAL':>10}{'VOLUMEN':>10}{'TOTAL':>10}  OPERARIO"]
        for r in rows:
            if r["state"] == WATERING:
                detail = f"{r['flow']:>7.1f} L/m{r['volume']:>8.2f} L"
            elif r["state"] == LEAK:
                detail = f"  {r['reason']}"[:20]
            else:
                detail = " " * 20
            txt = (f"  {r['id']:<12}{r['location'][:17]:<18}{r['label']:<16}{detail:<20}"
                   f"{r['total']:>8.2f} L  {r['op']}")
            lines.append(f"{BG[r['state']]}{txt:<92}\033[0m")
        if not rows:
            lines.append("  (no hay WS registradas)")
        lines.append(color("  Órdenes (iniciar riego · bloquear · activar) desde el panel web", "gray"))
        return "\n".join(lines)

    def http_server(self):
        central = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, body, ctype):
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.end_headers()
                self.wfile.write(body)

            def _json(self, data):
                self._send(200, json.dumps(data).encode(), "application/json")

            def do_GET(self):
                path = self.path.split("?")[0]
                if path == "/api/state":
                    self._json({"kafka": central.bus is not None, "ws": central.snapshot()})
                elif path == "/api/events":
                    self._json(list(EVENTS)[-100:])
                elif path == "/api/waterings":
                    self._json(central.recent_waterings())
                elif path in WEB_FILES:
                    name, ctype = WEB_FILES[path]
                    try:
                        with open(os.path.join(WEB_DIR, name), "rb") as f:
                            self._send(200, f.read(), ctype)
                    except FileNotFoundError:
                        self._send(500, f"Falta el archivo web/{name}".encode(), "text/plain; charset=utf-8")
                else:
                    self._send(404, b"", "text/plain")

            def do_POST(self):
                if self.path.startswith("/api/cmd"):
                    n = int(self.headers.get("Content-Length", 0))
                    try:
                        d = json.loads(self.rfile.read(n) or b"{}")
                        threading.Thread(target=central.order, daemon=True,
                                         args=(d["action"], d["ws"], d.get("duration", 30))).start()
                        self._send(200, b'{"ok":true}', "application/json")
                    except Exception as e:
                        self._send(400, json.dumps({"error": str(e)}).encode(), "application/json")
                else:
                    self._send(404, b"", "text/plain")

        ThreadingHTTPServer(("0.0.0.0", self.args.http_port), Handler).serve_forever()
























"""

#Railway
PORT = int(os.environ.get("PORT", 3000))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Hello World desde Railway y UA_SD!, por parte de Imran y Michelle")

def lanzar_web():
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

# Lanzamos el servidor HTTP en segundo plano para que Railway detecte el puerto activo
threading.Thread(target=lanzar_web, daemon=True).start()


#Creamos la base de datos.

connection = sqlite3.connect('wmcentral.db', check_same_thread = False)
cursor = connection.cursor()

cursor.execute(
    ''' CREATE TABLE IF NOT EXISTS stations(
            id TEXT,
            status TEXT,
            location TEXT)
    ''')
connection.commit()



HEADER = 64
PORT = 5050
SERVER = '0.0.0.0'
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"

REGISTRO = "REGISTRO"

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] Estación conectada desde: {addr}")

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
                print(f"[DESCONEXIÓN] La estación {addr} ha solicitado finalizar la conexión")
                break
            
            print(f"[MENSAJE] Mensaje recibido de la estación [{addr}]: {msg}")
            
            if REGISTRO in msg:
                splitted = msg.split('#')
                if len(splitted)>=3:
                    ws_id = splitted[1]
                    location = splitted[2]
                    cursor.execute("INSERT OR REPLACE INTO stations VALUES (?, ?, ?)", (ws_id, "Available" ,location))
                    connection.commit()
                    print(f"[BASE DE DATOS] Estación '{ws_id}' registrada en la ubicación '{location}'")
                    conn.send(f"[REGISTRADO] Estación {ws_id} registrada correctamente".encode(FORMAT))
                else:
                    conn.send("[ERR_REGISTRO] Formato inválido. Uso: REGISTRO#ID#UBICACION".encode(FORMAT))
            else:
                conn.send(f"CENTRAL OK: mensaje recibido: {msg}".encode(FORMAT))
    print(f"[FIN_CONEXION] Cerrando conexión con {addr}.")
    conn.close()
    
        

def start():
    server.listen()
    print(f"[ESCUCHANDO] Servidor a la escucha en el puerto {PORT}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count()
        if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("[ADVERTENCIA] Demasiadas conexiones simultáneas.")
            conn.send("Demasiadas conexiones activas. Inténtelo de nuevo más tarde.".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1
        

######################### MAIN ##########################

"""

def main():
    ap = argparse.ArgumentParser(description="WM_Central - Central de WaterManagement")
    ap.add_argument("--port", type=int, default=int(env("WM_PORT", 5050)), help="puerto de sockets para los WM_WS_M")
    ap.add_argument("--kafka", default=env("KAFKA_BOOTSTRAP", "localhost:9092"), help="host:puerto del broker Kafka")
    ap.add_argument("--http-port", type=int, default=int(env("PORT", 3000)), help="puerto del panel web")
    ap.add_argument("--db", default=env("WM_DB", "wmcentral.db"), help="fichero SQLite")
    args = ap.parse_args()

    print(color("=" * 64, "cyan"))
    print(color("  WM_Central · WaterManagement · SD 26/27", "bold"))
    print(color("=" * 64, "cyan"), flush=True)
    c = Central(args)
    for t in (c.http_server, c.socket_server, c.kafka_thread, c.watchdog):
        threading.Thread(target=t, daemon=True).start()
    say(f"Panel web escuchando en el puerto {args.http_port}", "green")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        say("WM_Central detenida")


if __name__ == "__main__":
    main()



