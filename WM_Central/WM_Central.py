import argparse
from importlib.resources import path
import json
import socket
import sqlite3
import os
import time
import threading
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from wm_common import (T_CMD, T_EVT, T_FO, T_REQ, T_STATUS, ConnectionClosed, FramedConn, ProtocolError, color, connect_kafka, env, log, new_id, poll_json, split_msg)

MAX_CONEXIONES = 50
HEARTBEAT_TIMEOUT = 3.5
START_TIMEOUT = 6
MAX_REQ_AGE = 20    #si se cumple rechazamos la respuesta, para evitar que se acumulen respuestas antiguas en la cola de eventos

# Estados de las estaciones de riego
AVAILABLE, WATERING, LEAK, OUT, DISCONNECTED = "AVAILABLE", "WATERING", "LEAK", "OUT_OF_SERVICE", "DISCONNECTED"

# Etiquetas para mostrar en la interfaz, 
LABEL = {AVAILABLE: "Available", WATERING: "Watering", LEAK: "Leak", OUT: "Out of Service", DISCONNECTED: "Disconnected"}

# Colores de fondo para mostrar en la interfaz
BG = {AVAILABLE: "\033[42;30m", WATERING: "\033[42;30;5m", LEAK: "\033[41;97m", OUT: "\033[48;5;208;30m", DISCONNECTED: "\033[100;97m"}

# Archivos web
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
WEB_FILES = {"/": ("panel.html", "text/html; charset=utf-8"),
             "/panel.html": ("panel.html", "text/html; charset=utf-8"),
             "/panel.css": ("panel.css", "text/css; charset=utf-8"),
             "/panel.js": ("panel.js", "application/javascript; charset=utf-8")}


EVENTS = deque(maxlen=200)  # Cola de eventos recientes, para mostrar en la interfaz


def add_event(msg, col=None):
    EVENTS.append({"time": time.time("%H:%M:%S"), "msg": msg, "col": col})

def say(msg, col=None):
    log("CENTRAL", msg, col)
    add_event(msg, col)


#Base de datos SQLite para registrar las estaciones de riego
class Database:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)

        #Lock para evitar problemas de concurrencia al acceder a la base de datos desde múltiples hilos
        self.lock = threading.Lock()

        with self.lock:
            self.conn.execute("""CREATE TABLE IF NOT EXISTS
                                WM_Stations (id TEXT PRIMARY KEY, location TEXT, blocked INTEGER DEFAULT 0)""")

            self.conn.execute("""CREATE TABLE IF NOT EXISTS 
                                Operators(id TEXT PRIMARY KEY, created REAL)""")
            self.conn.execute("""CREATE TABLE IF NOT EXISTS
                                Sessions(req_id TEXT PRIMARY KEY, ws TEXT, operator_id TEXT, duration INTEGER, started REAL)""")
            
            self.conn.execute("""CREATE TABLE IF NOT EXISTS
                                Waterings(req_id TEXT PRIMARY KEY, ws TEXT, operator_id TEXT, volume REAL, duration INTEGER, reason TEXT, ended REAL)""")

            self.conn.commit()

    def query(self, sql, params=()):
        with self.lock:
            cursor = self.conn.execute(sql, params)
            self.conn.commit()
            return cursor.fetchall()
        
# Clase para representar el estado de una estación de riego
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
        self.current_session = None  # ID de la sesión actual si está regando, None si no lo está
        self.total_watered = 0.0  # Total de agua regada en litros

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
        self.ws_states = {}  # Diccionario de estados de estaciones de riego, clave: ws_id, valor: WSState
        self.pending_requests = {}  # Diccionario de solicitudes pendientes, clave: req_id, valor: (ws_id, timestamp)
        self.interrupted_sessions = {}  # Diccionario de sesiones interrumpidas, clave: req_id, valor: (ws_id, operator_id, duration, started)
        self.bus = None  # Inicialmente no hay bus de eventos, se conectará más tarde
        self.load_ws_states_from_db()  # Cargamos los estados de las estaciones de riego desde la base de datos

    def load_ws_states_from_db(self):
        for ws_id, location, blocked in self.db.query("SELECT id, location, blocked FROM WM_Stations"):
            self.ws_states[ws_id] = WSState(ws_id, location, bool(blocked))

        for ws_id, total_watered in self.db.query("SELECT ws, SUM(volume) FROM Waterings GROUP BY ws"):
            if ws_id in self.ws_states:
                self.ws_states[ws_id].total_watered = total_watered or 0.0

        for req_id, ws, operator_id, duration, _ in self.db.query("SELECT * FROM Sessions"):
            if ws in self.ws_states:
                self.ws_states[ws_id].current_session = {"req_id": req_id, "operator_id": operator_id, "duration": duration, "flow": 0.0, "volume": 0.0, "elapsed": 0.0}

        say(f"Cargados {len(self.ws_states)} estados de estaciones de riego desde la base de datos" f"({', '.join(self.ws) or 'ninguna'}) Todas DESCONECTADAS hasta que se conecte su monitor", "cyan")


    #Kafka
    def kafka_thread(self):
        self.bus = connect_kafka(self.args.kafka, "wm-central", say)
        threading.Thread(target=self.consume, args=([T_REQ], "wm-central"), daemon=True).start()
        threading.Thread(target=self.consume, args=([T_EVT], "wm-central"), daemon=True).start

    def consume(self, topics, group):
        #grupos por si kafka se cae y se reinicia, para que no se pierdan mensajes
        consumer = self.bus.consumer(topics, group)

        while True:
            try:
                msg, _ = consumer.poll(timeout=1.0)
                if msg:
                    self.on_kafka(msg)

            except Exception as e:
                say(f"ERROR: Error al consumir mensajes de Kafka: {e}", "red")

    def cmd(self, ws_id, type, **kwargs):
        msg = {"type": type, "ws": ws_id, **kwargs}
        if not (self.bus and self.bus.send_sync(T_CMD, msg, key=ws_id)):
            say(f"ERROR: No se pudo enviar el comando {type} a la estación {ws_id}", "red")
            return False
        return True

    def notify_fo(self, operator_id, req_id, type, text, **kwargs):
        if not operator_id or operator_id == "CENTRAL" or operator_id.startswith("WS:") or not self.bus:
            say(f"ERROR: No se pudo notificar al operador {operator_id} sobre la solicitud {req_id}: condiciones no cumplidas", "red")
            return
        self.bus.send_sync(T_FO, {"type": type, "req_id": req_id, "msg": text, **kwargs}, key=operator_id)

    def on_kafka(self, msg):
        #Procesamos los mensajes recibidos de Kafka
        type = msg.get("type")
        if type == T_REQ:
            self.on_kafka_req(msg)
        elif type in ("START_ACK", "START_NACK"):
            self.on_sart_reply(msg)
        elif type == "TELEMETRY":
            self.on_telemetry(msg)
        elif type == "END":
            self.on_end(msg)


    #peticiones de inicio de riego, que vienen de los operadores, y que se envían a las estaciones de riego
    def on_request(self, msg):
        req_id = msg.get("req_id")
        operator_id = msg.get("op", "?")
        ws_id = msg.get("ws")
        source = msg.get("source", "FO")
        duration = int(msg.get("duration", 30))

        if source == "FO":
            if not self.db.query("SELECT 1 FROM Operators WHERE id=?", (operator_id,)):
                self.db.query("INSERT INTO Operators VALUES (?, ?)", (operator_id, time.time()))
                say(f"Nuevo operador registrado: {operator_id}", "cyan")

            if time.time() - float(msg.get("timestamp", time.time())) > MAX_REQ_AGE:
                self.deny(req_id, operator_id, ws_id, source, "Solicitud demasiado antigua")
                return

            say(f"Solicitud de riego recibida de {operator_id} para la estación {ws_id} (duración: {duration}s)", "cyan")
            self.start_watering(req_id, operator_id, ws_id, source, duration)

    def start_watering(self, ws_id, operator_id, duration, req_id=None, source="CENTRAL"):
        req_id = req_id or new_id("C")
        with self.lock:
            ws_state = self.ws_states.get(ws_id)
            if ws_state is None:
                return self.deny(req_id, operator_id, ws_id, source, "Estación no registrada")

            status = ws_state.status()
            if status != AVAILABLE:
                return self.deny(req_id, operator_id, ws_id, source, f"Estación no disponible ({status})")
            if any(p["ws"] == ws_id for p in self.pending_requests.values()):
                return self.deny(req_id, operator_id, ws_id, source, "Ya hay una solicitud pendiente para esta estación")

            if not self.bus:
                return self.deny(req_id, operator_id, ws_id, source, "Central no conectada al bus de eventos de Kafka")
            self.pending_requests[req_id] = {"ws": ws_id, "operator_id": operator_id, "duration": duration, "source": source, "timestamp": time.time()}
        self.notify_fo(operator_id, req_id, "INFO", f"Central disponible, solicitando inicio de riego a la estación...", ws=ws_id)

        say(f"{ws_id} disponible, enviando START {req_id} a la estación", "cyan")
        if not self.cmd(ws_id, "START", req_id=req_id, operator_id=operator_id, duration=duration):
            with self.lock:
                self.pending_requests.pop(req_id, None)
            self.deny(req_id, operator_id, ws_id, source, "Error al enviar comando START a la estación, no se pudo comunicar con ella")

    def deny(self, req_id, operator_id, ws_id, source, reason):
        say(f"Solicitud {req_id} de {operator_id} para la estación {ws_id} denegada: {reason}", "red")
        self.notify_fo(operator_id, req_id, "DENIED", f"Solicitud denegada: {reason}", ws=ws_id)

        if source == "WS" and ws_id:
            self.cmd(ws_id, "DENIED", req_id=req_id, reason=reason)

    def on_sart_reply(self, msg):
        req_id = msg.get("req_id")
        ws_id = msg.get("ws")
        

        with self.lock:
            pending = self.pending_requests.pop(req_id, None)

        if pending is None:
            say(f"Respuesta de inicio de riego {msg.get('type')} recibida para solicitud {req_id}, pero no hay solicitud pendiente", "red")
            return

        if msg["type"] == "START_ACK":
            self.ws[ws_id].current_session = {"req_id": req_id, "operator_id": pending["operator_id"], "duration": pending["duration"], "flow": 0.0, "volume": 0.0, "elapsed": 0.0}
            self.db.query("INSERT INTO Sessions VALUES (?, ?, ?, ?, ?)", (req_id, ws_id, pending["operator_id"], pending["duration"], time.time()))

        if msg["type"] == "START_NACK":
            self.deny(req_id, pending["operator_id"], ws_id, pending["source"], f"Estación {ws_id} denegó la solicitud de inicio de riego: {msg.get('reason')}")
            return
        say(f"Estación {ws_id} ha aceptado la solicitud de inicio de riego {req_id} de {pending['operator_id']} (duración: {pending['duration']}s): electroválvula activada", "green")
        self.notify_fo(pending["operator_id"], req_id, "AUTHORIZED", f"Estación {ws_id} ha iniciado el riego (duración: {pending['duration']}s). Electrovalvula activada.", ws=ws_id)


    def on_telemetry(self, msg):
        ws_id = msg.get("ws")
        req_id = msg.get("req_id")

        with self.lock:
            ws = self.ws.get(ws_id)
            if not ws or req_id in self.interrupted_sessions:
                say(f"Telemetría recibida de {ws_id} para solicitud {req_id}, pero la estación no está registrada o la sesión está interrumpida", "red")
                return
            if ws.current_session is None or ws.current_session["req_id"] != req_id:
                ws.current_session = {"req_id": req_id, "operator_id":"?", "duration": 0, "flow": 0.0, "volume": 0.0, "elapsed": 0.0}

                session = ws.current_session
                session.update(flow=msg.get("flow", 0.0), volume=msg.get("volume", 0.0), elapsed=msg.get("elapsed", 0.0))
                operator_id = session["operator_id"]

        add_event(f"[operator {operator_id}] Regando en {ws_id}: {session['flow']:1f} L/min, | {session['volume']:2f} L acumulados | " f"{session['elapsed']}/{session['duration']} s")
        self.notify_fo(operator_id, req_id, "TELEMETRY", "", ws=ws_id, flow=session["flow"], volume=session["volume"], elapsed=session["elapsed"], duration=session["duration"])

    def on_end(self, msg):
        ws_id = msg.get("ws")
        req_id = msg.get("req_id")
        volume = float(msg.get("volume", 0.0))
        duration = int(msg.get("duration", 0))
        reason = msg.get("reason", "?")      
        operator_id = msg.get("operator_id")    

        with self.lock:
            ws = self.ws.get(ws_id)
            if ws and ws.current_session and ws.current_session["req_id"] == req_id:
                operator_id = ws.current_session["operator_id"] if ws.current_session["operator_id"] != "?" else operator_id
                ws.current_session = None
            self.interrupted_sessions.pop(req_id, None)

            if self.db.query("SELECT 1 FROM Waterings WHERE req_id=?", (req_id,)):
                return
            self.db.query("INSERT INTO Waterings VALUES (?, ?, ?, ?, ?, ?, ?)", (req_id, ws_id, operator_id, volume, duration, reason, time.time()))
            if ws:
                ws.total_watered += volume
            say(f"RIEGO FINALIZADO {req_id} en {ws_id} ({reason}): {volume:.2f} L en {duration}s Operario: {operator_id}", "cyan")
            self.notify_for_operator(operator_id, req_id, "END", f"Resumen final: {volume:.2f} L en {duration}s (motivo: {reason})", ws=ws_id, volume=volume, duration=duration, reason=reason)

    # Simula la desconexion o fuga de agua de una estación de riego, para probar la gestión de interrupciones
    def cut_session(self, ws, reason):
        session = ws.current_session
        if not session:
            return

        ws.current_session = None
        self.interrupted_sessions[session["req_id"]] = dict(session, ws=ws.id)
        say(f"SESIÓN INTERUMPIDA {session['req_id']} en {ws.id} ({reason})", "red")

        self.notify_fo(session["operator_id"], session["req_id"], "INCIDENT", f"Riego interrumpido en {ws.id} {reason}. Litros regados hasta ahora: {session['volume']:.2f} L" f"El resumen final se enviará cuando la estación se reconecte y finalice el riego", ws=ws.id)
        self.cmd(ws.id, "STOP", req_id=session["req_id"], reason="FUGA" if "fuga" in reason.lower() else "CENTRAL")


    def order(self, action, target, duration=30):
        with self.lock:
            targets = list(self.ws) if target.upper() == "ALL" else [target]
        
        for ws_id in targets:
            if ws_id not in self.ws:
                say(f"La WS {ws_id} no existe", "red")
                continue
            if action == "start":
                self.start_watering(ws_id, "CENTRAL", int(duration))
            elif action == "block":
                with self.lock:
                    self.ws[ws_id].blocked = True
                    self.db.q("UPDATE ws SET blocked=1 WHERE id=?", (ws_id,))
                self.cmd(ws_id, "BLOCK")
                say(f"{ws_id} BLOQUEADA: Out of Service", "orange")
            elif action == "activate":
                with self.lock:
                    self.ws[ws_id].blocked = False
                    self.db.q("UPDATE ws SET blocked=0 WHERE id=?", (ws_id,))
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
            say(f"Estación conectada desde {addr}", "cyan")
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
                        new = rid not in self.ws
                        if new:
                            self.ws[rid] = WSState(rid, loc)
                        else:
                            self.ws[rid].location = loc
                        self.db.q("INSERT INTO ws(id, location) VALUES(?, ?) "
                                "ON CONFLICT(id) DO UPDATE SET location=excluded.location", (rid, loc))
                    say(f"[REGISTRO] WS {rid} {'registrada' if new else 'actualizada'} en '{loc}' (desde {addr[0]})", "cyan")
                    fc.send(f"OK#WS {rid} {'registrada' if new else 'actualizada'}")
                elif code == "AUTH" and len(m) >= 2:
                    rid = m[1]
                    with self.lock:
                        ws = self.ws.get(rid)
                        if ws is None:
                            reason = "WS no registrada en WM_Central"
                        elif ws.connected and time.time() - ws.last_seen < HEARTBEAT_TIMEOUT:
                            reason = "ya hay un monitor activo con ese ID"
                        else:
                            reason = None
                            ws_id = rid
                            ws.connected, ws.last_seen, ws.conn_token = True, time.time(), token
                            ws.health, ws.health_reason = "KO", "esperando al engine"
                    if reason:
                        say(f"[AUTH] WS {rid} DENEGADA: {reason}", "red")
                        fc.send(f"DENIED#{reason}")
                        return
                    say(f"[AUTH] WS {rid} autenticada (monitor {addr[0]}:{addr[1]})", "green")
                    fc.send(f"OK#{rid}#{ws.location}#{int(ws.blocked)}")
                elif code == "STATUS" and ws_id and len(m) >= 3:
                    self.on_health(ws_id, token, m[2], m[3] if len(m) > 3 else "")
                    fc.send(f"ACK#{int(self.ws[ws_id].blocked)}")
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
            ws = self.ws[ws_id]
            if ws.conn_token is not token:
                say(f"[HEALTH] WS {ws_id} reportó estado {health} pero el token no coincide, ignorando", "red")
                return
            ws.last_seen = time.time()
            ws.connected = True
            old, old_reason = ws.health, ws.health_reason
            ws.health, ws.health_reason = health, reason

            if old != health or (health == "KO" and old_reason != reason):
                if health == "KO":
                    self.cut_session(ws, reason)
                    say(f"{ws_id}: monitor conectado, esperando a su engine", "yellow")

                elif health == "KO":
                    say(f"*** {ws_id}: AVERÍA/FUGA reportada por el monitor ({reason}) ***", "red")
                    self.cut_session(ws, f"fuga/avería detectada ({reason})")
                elif old_reason == "esperando al engine":
                    say(f"{ws_id}: monitor y engine conectados → WS DISPONIBLE", "green")
                else:
                    say(f"{ws_id}: avería resuelta, WS operativa", "green")

    def set_disconnected(self, ws_id, token, reason):
        with self.lock:
            ws = self.ws.get(ws_id)
            if not ws or ws.conn_token is not token or not ws.connected:
                return
            ws.connected = False
            ws.conn_token = None
            say(f"{ws_id} DESCONECTADA: {reason}", "gray")
            self.cut_session(ws, "WS desconectada de WM_Central")

    # hilo que comprueba periódicamente el estado de las estaciones de riego y cierra las sesiones interrumpidas si no se reconectan a tiempo
    def watchdog(self):
        last_panel = None

        while True:
            time.sleep(1)
            now = time.time()
            with self.lock:
                for ws in self.ws.values():
                    if ws.connected and now - ws.last_seen > HEARTBEAT_TIMEOUT:
                        self.set_disconnected(ws.id, ws.conn_token, "monitor desconectado (timeout)")

                expired_sessions = [(req_id, session) for req_id, session in self.interrupted_sessions.items() if now - session["started"] > START_TIMEOUT]

                for req_id, _ in expired_sessions:
                    self.pending_requests.pop(req_id)

                for req_id, session in expired_sessions:
                    self.deny(req_id, session["operator_id"], session["ws"], session["source"], "La estación no respondió a tiempo, sesión interrumpida")
                    self.cmd(session["ws"], "STOP", req_id=req_id, reason="CENTRAL")

                if self.bus:
                    try:
                        self.bus.send(T_STATUS, {"type": "STATUS", "ws": self.snapshot()})
                    except Exception:
                        pass
                
                panel = self.panel_text()
                if panel != last_panel:
                    last_panel = panel
                    print(panel, flush=True)


    def snapshot(self):
        with self.lock:
            out = []
            for ws in sorted(self.ws.values(), key=lambda w: w.id):

                state = ws.status()
                session = ws.current_session or {}

                out.append({"id": ws.id, "location": ws.location, "status": state, "label": LABEL[state], 
                            "flow": session.get("flow", 0.0), "volume": session.get("volume", 0.0), 
                            "operator_id": session.get("operator_id"), 
                            "elapsed": session.get("elapsed", 0.0), "duration": session.get("duration", 0), 
                            "total_watered": round(ws.total_watered, 2), 
                            "reason": ws.health_reason if state == LEAK else "",
                            "health": ws.health if ws.connected else None,
                            "blocked": ws.blocked})
            return out


    def recent_waterings(self, limit=15):
        with self.lock:
            live = {ws.current_session["req_id"]: ws.current_session["volume"] for ws in self.ws.values() if ws.current_session}
            cut = set(self.interrupted_sessions)

        rows = []
        for req_id, ws_id, operator_id, _duration, started in self.db.query("SELECT req_id, ws, operator_id, duration, started FROM Sessions"):
            rows.append({"timestamp": started, "ws": ws_id, "operator_id": operator_id, "volume": live.get(req_id, 0.0) ,"reason": "",
                         "state": "INTERRUPTED" if req_id in cut else "ACTIVE"})

        for _req_id, ws_id, operator_id, volume, _duration, reason, ended in self.db.query(
                "SELECT * FROM Waterings ORDER BY ended DESC LIMIT ?", (limit,)):
            rows.append({"timestamp": ended, "ws": ws_id, "operator_id": operator_id, "volume": volume, "reason": reason, "state": "FINALIZADO"})
        rows.sort(key=lambda r: r["timestamp"] or 0, reverse=True)
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
            txt = f"  {r['id']:<12}{r['location'][:17]:<18}{r['label']:<16}{detail:<20}{r['total']:>8.2f} L  {r['op']}"
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



