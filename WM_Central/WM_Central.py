import argparse
import json
import os
import socket
import sqlite3
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from wm_common import (T_CMD, T_EVT, T_FO, T_REQ, T_STATUS, ConnectionClosed, FramedConn, ProtocolError,
                       color, connect_kafka, env, log, new_id, poll_json, split_msg)

HEARTBEAT_TIMEOUT = 3.5     # s sin latido del monitor -> DESCONECTADA
START_TIMEOUT = 6           # s esperando START_ACK del engine
MAX_REQ_AGE = 20            # s: peticiones más antiguas (p.ej. enviadas con la Central caída) se rechazan

AVAILABLE, WATERING, LEAK, OUT, DISCONNECTED = "AVAILABLE", "WATERING", "LEAK", "OUT_OF_SERVICE", "DISCONNECTED"
LABEL = {AVAILABLE: "Available", WATERING: "Watering", LEAK: "Leak", OUT: "Out of Service",
         DISCONNECTED: "Disconnected"}
# Fondo de color para el panel de terminal
BG = {AVAILABLE: "\033[42;30m", WATERING: "\033[42;30;5m", LEAK: "\033[41;97m",
      OUT: "\033[48;5;208;30m", DISCONNECTED: "\033[100;97m"}

# Carpeta con la interfaz web (archivos independientes del código Python)
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
WEB_FILES = {"/": ("panel.html", "text/html; charset=utf-8"),
             "/panel.html": ("panel.html", "text/html; charset=utf-8"),
             "/panel.css": ("panel.css", "text/css; charset=utf-8"),
             "/panel.js": ("panel.js", "application/javascript; charset=utf-8")}

# Últimos eventos para el apartado "Eventos en tiempo real" del panel web
EVENTS = deque(maxlen=200)

def add_event(msg, col=None):
    EVENTS.append({"ts": time.strftime("%H:%M:%S"), "msg": msg, "col": col})

def say(msg, col=None):
    log("CENTRAL", msg, col)
    add_event(msg, col)
# ============================================================================ base de datos
class DB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        with self.lock:
            c = self.conn
            c.execute("""CREATE TABLE IF NOT EXISTS ws(
                            id TEXT PRIMARY KEY, location TEXT, blocked INTEGER DEFAULT 0)""")
            c.execute("""CREATE TABLE IF NOT EXISTS operators(
                            id TEXT PRIMARY KEY, created REAL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS sessions(      -- riegos EN CURSO
                            req_id TEXT PRIMARY KEY, ws TEXT, op TEXT, duration INTEGER, started REAL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS waterings(     -- histórico de riegos terminados
                            req_id TEXT PRIMARY KEY, ws TEXT, op TEXT, volume REAL,
                            duration INTEGER, reason TEXT, ended REAL)""")
            c.commit()

    def q(self, sql, args=()):
        with self.lock:
            cur = self.conn.execute(sql, args)
            self.conn.commit()
            return cur.fetchall()
# ============================================================================ estado de una WS
class WS:
    def __init__(self, ws_id, location, blocked=False):
        self.id = ws_id
        self.location = location
        self.blocked = blocked
        self.connected = False
        self.last_seen = 0.0
        self.conn_token = None        # identifica la conexión del monitor activa
        self.health = "OK"            # lo que reporta el monitor: OK / KO
        self.health_reason = ""
        self.session = None           # riego en curso: dict(req_id, op, duration, flow, volume, elapsed)
        self.total = 0.0              # litros totales suministrados (histórico)

    def state(self):
        if not self.connected:
            return DISCONNECTED
        if self.health == "KO":
            return LEAK
        if self.blocked:
            return OUT
        if self.session:
            return WATERING
        return AVAILABLE
# ============================================================================ Central
class Central:
    def __init__(self, args):
        self.args = args
        self.db = DB(args.db)
        self.lock = threading.RLock()
        self.ws = {}
        self.pending = {}         # req_id -> petición esperando START_ACK
        self.interrupted = {}     # req_id -> sesión cortada por fuga/desconexión (falta el END)
        self.bus = None
        self.load_from_db()

    # ------------------------------------------------------------------ arranque
    def load_from_db(self):
        """Punto 1 de la mecánica: al arrancar se muestran las WS registradas como DESCONECTADAS."""
        for ws_id, loc, blocked in self.db.q("SELECT id, location, blocked FROM ws"):
            self.ws[ws_id] = WS(ws_id, loc, bool(blocked))
        for ws_id, total in self.db.q("SELECT ws, SUM(volume) FROM waterings GROUP BY ws"):
            if ws_id in self.ws:
                self.ws[ws_id].total = total or 0.0
        # riegos que estaban en curso cuando la Central se paró: siguen en la WS
        for req_id, ws_id, op, dur, _ in self.db.q("SELECT * FROM sessions"):
            if ws_id in self.ws:
                self.ws[ws_id].session = {"req_id": req_id, "op": op, "duration": dur,
                                          "flow": 0.0, "volume": 0.0, "elapsed": 0}
        say(f"Base de datos cargada: {len(self.ws)} WS registradas "
            f"({', '.join(self.ws) or 'ninguna'}). Todas DESCONECTADAS hasta que conecte su monitor.", "cyan")

    # ------------------------------------------------------------------ Kafka
    def kafka_thread(self):
        self.bus = connect_kafka(self.args.kafka, "wm-central", say)
        threading.Thread(target=self.consume, args=([T_REQ], "wm-central-req"), daemon=True).start()
        threading.Thread(target=self.consume, args=([T_EVT], "wm-central-evt"), daemon=True).start()

    def consume(self, topics, group):
        # Consumidor con grupo: si la Central cae, al volver procesa lo que llegó mientras tanto.
        c = self.bus.group_consumer(topics, group)
        while True:
            try:
                m, _ = poll_json(c, 1.0)
                if m:
                    self.on_kafka(m)
            except Exception as e:
                say(f"Error procesando mensaje de Kafka: {e}", "red")

    def cmd(self, ws_id, t, **kw):
        """Orden a un Engine por Kafka."""
        msg = {"t": t, "ws": ws_id, **kw}
        if not (self.bus and self.bus.send_sync(T_CMD, msg, key=ws_id)):
            say(f"No se pudo enviar la orden {t} a {ws_id} (Kafka no disponible)", "red")
            return False
        return True

    def notify_fo(self, op, req_id, t, text, **kw):
        """Mensaje para el operario 'op' (solo lo muestra ese operario)."""
        if not op or op == "CENTRAL" or op.startswith("WS:") or not self.bus:
            return
        self.bus.send_sync(T_FO, {"t": t, "op": op, "req_id": req_id, "msg": text, **kw}, key=op)

    def on_kafka(self, m):
        t = m.get("t")
        if t == "REQ":
            self.on_request(m)
        elif t in ("START_ACK", "START_NACK"):
            self.on_start_reply(m)
        elif t == "TELEMETRY":
            self.on_telemetry(m)
        elif t == "END":
            self.on_end(m)

    # ------------------------------------------------------------------ peticiones de riego
    def on_request(self, m):
        req_id, op, ws_id = m.get("req_id"), m.get("op", "?"), m.get("ws")
        src, dur = m.get("src", "FO"), int(m.get("duration", 30))
        if src == "FO":
            if not self.db.q("SELECT 1 FROM operators WHERE id=?", (op,)):
                self.db.q("INSERT INTO operators VALUES (?, ?)", (op, time.time()))
                say(f"Nuevo operario dado de alta: {op}", "cyan")
        if time.time() - float(m.get("ts", time.time())) > MAX_REQ_AGE:
            self.deny(req_id, op, ws_id, src, "petición caducada (se envió con WM_Central caída)")
            return
        say(f"Petición {req_id} de {op} para {ws_id} ({dur} s). Comprobando disponibilidad...", "cyan")
        self.start_watering(ws_id, op, dur, req_id, src)

    def start_watering(self, ws_id, op, dur, req_id=None, src="CENTRAL"):
        req_id = req_id or new_id("C")
        with self.lock:
            ws = self.ws.get(ws_id)
            if ws is None:
                return self.deny(req_id, op, ws_id, src, "WS no registrada")
            st = ws.state()
            if st != AVAILABLE:
                return self.deny(req_id, op, ws_id, src, f"WS no disponible (estado {LABEL[st]})")
            if any(p["ws"] == ws_id for p in self.pending.values()):
                return self.deny(req_id, op, ws_id, src, "la WS tiene otra activación pendiente")
            if not self.bus:
                return self.deny(req_id, op, ws_id, src, "WM_Central sin conexión con Kafka")
            self.pending[req_id] = {"ws": ws_id, "op": op, "duration": dur, "src": src, "t0": time.time()}
        self.notify_fo(op, req_id, "INFO", f"WM_Central: {ws_id} disponible. Solicitando autorización a la WS...",
                       ws=ws_id)
        say(f"{ws_id} disponible: enviando START {req_id} a la WS", "cyan")
        if not self.cmd(ws_id, "START", req_id=req_id, op=op, duration=dur):
            with self.lock:
                self.pending.pop(req_id, None)
            self.deny(req_id, op, ws_id, src, "no se pudo contactar con la WS")

    def deny(self, req_id, op, ws_id, src, reason):
        say(f"Riego {req_id} de {op} en {ws_id} DENEGADO: {reason}", "red")
        self.notify_fo(op, req_id, "DENIED", f"Riego DENEGADO: {reason}", ws=ws_id)
        if src == "WS" and ws_id:
            self.cmd(ws_id, "DENIED", req_id=req_id, reason=reason)

    def on_start_reply(self, m):
        req_id, ws_id = m.get("req_id"), m.get("ws")
        with self.lock:
            p = self.pending.pop(req_id, None)
            if p is None:
                return
            if m["t"] == "START_NACK":
                pass
            else:
                ws = self.ws[ws_id]
                ws.session = {"req_id": req_id, "op": p["op"], "duration": p["duration"],
                              "flow": 0.0, "volume": 0.0, "elapsed": 0}
                self.db.q("INSERT OR REPLACE INTO sessions VALUES (?,?,?,?,?)",
                          (req_id, ws_id, p["op"], p["duration"], time.time()))
        if m["t"] == "START_NACK":
            self.deny(req_id, p["op"], ws_id, p["src"], f"la WS rechazó el inicio: {m.get('reason')}")
            return
        say(f"Riego {req_id} AUTORIZADO en {ws_id} para {p['op']} ({p['duration']} s): electroválvula abierta",
            "green")
        self.notify_fo(p["op"], req_id, "AUTHORIZED",
                       f"Riego AUTORIZADO en {ws_id} ({p['duration']} s). Electroválvula abierta.", ws=ws_id)

    def on_telemetry(self, m):
        ws_id, req_id = m.get("ws"), m.get("req_id")
        with self.lock:
            ws = self.ws.get(ws_id)
            if not ws or req_id in self.interrupted:
                return
            if ws.session is None or ws.session["req_id"] != req_id:
                ws.session = {"req_id": req_id, "op": "?", "duration": 0, "flow": 0.0, "volume": 0.0, "elapsed": 0}
            s = ws.session
            s.update(flow=m.get("flow", 0.0), volume=m.get("volume", 0.0), elapsed=m.get("elapsed", 0))
            op = s["op"]
        # solo al panel web (en la terminal ya se ve en la tabla, que se repinta cada segundo)
        add_event(f"[{op}] Regando en {ws_id}: {s['flow']:.1f} L/min | {s['volume']:.2f} L acumulados | "
                  f"{s['elapsed']}/{s['duration']} s")
        self.notify_fo(op, req_id, "TELEMETRY", "", ws=ws_id, flow=s["flow"], volume=s["volume"],
                       elapsed=s["elapsed"], duration=s["duration"])

    def on_end(self, m):
        ws_id, req_id = m.get("ws"), m.get("req_id")
        vol, dur, reason, op = float(m.get("volume", 0)), int(m.get("duration", 0)), m.get("reason", "?"), m.get("op")
        with self.lock:
            ws = self.ws.get(ws_id)
            if ws and ws.session and ws.session["req_id"] == req_id:
                op = ws.session["op"] if ws.session["op"] != "?" else op
                ws.session = None
            self.interrupted.pop(req_id, None)
            if self.db.q("SELECT 1 FROM waterings WHERE req_id=?", (req_id,)):
                return                                             # duplicado: ya contabilizado
            self.db.q("DELETE FROM sessions WHERE req_id=?", (req_id,))
            self.db.q("INSERT INTO waterings VALUES (?,?,?,?,?,?,?)", (req_id, ws_id, op, vol, dur, reason, time.time()))
            if ws:
                ws.total += vol
        say(f"RIEGO FINALIZADO {req_id} en {ws_id} ({reason}): {vol:.2f} L en {dur} s. Operario {op}", "cyan")
        self.notify_fo(op, req_id, "END", f"Resumen final: {vol:.2f} L en {dur} s (motivo: {reason})",
                       ws=ws_id, volume=vol, duration=dur, reason=reason)

    def cut_session(self, ws, why):
        """Fuga o desconexión durante un riego: avisa al FO; el END llegará cuando la WS se recupere."""
        s = ws.session
        if not s:
            return
        ws.session = None
        self.interrupted[s["req_id"]] = dict(s, ws=ws.id)
        say(f"Riego {s['req_id']} en {ws.id} INTERRUMPIDO: {why}", "red")
        self.notify_fo(s["op"], s["req_id"], "INCIDENT",
                       f"Riego interrumpido en {ws.id}: {why}. Llevaba {s['volume']:.2f} L. "
                       f"El resumen final llegará cuando la WS se recupere.", ws=ws.id)
        self.cmd(ws.id, "STOP", req_id=s["req_id"], reason="FUGA" if "fuga" in why.lower() else "CENTRAL")

    # ------------------------------------------------------------------ órdenes de la Central (punto 11)
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
                    ws = self.ws[ws_id]
                    ws.blocked = True
                    self.db.q("UPDATE ws SET blocked=1 WHERE id=?", (ws_id,))
                self.cmd(ws_id, "BLOCK")
                say(f"{ws_id} BLOQUEADA: Out of Service", "orange")
            elif action == "activate":
                with self.lock:
                    ws = self.ws[ws_id]
                    ws.blocked = False
                    self.db.q("UPDATE ws SET blocked=0 WHERE id=?", (ws_id,))
                self.cmd(ws_id, "ACTIVATE")
                say(f"{ws_id} ACTIVADA", "green")

    # ------------------------------------------------------------------ sockets con los monitores
    def socket_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.args.port))
        srv.listen(50)
        say(f"Servidor de sockets escuchando en el puerto {self.args.port}", "green")
        while True:
            conn, addr = srv.accept()
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
                        new = rid not in self.ws
                        if new:
                            self.ws[rid] = WS(rid, loc)
                        else:
                            self.ws[rid].location = loc
                        self.db.q("INSERT INTO ws(id, location) VALUES (?, ?) "
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
                return
            ws.last_seen = time.time()
            ws.connected = True
            old, old_reason = ws.health, ws.health_reason
            ws.health, ws.health_reason = health, reason
            if old != health or (health == "KO" and old_reason != reason):
                if health == "KO" and reason == "esperando al engine":
                    say(f"{ws_id}: monitor conectado, esperando a su engine", "yellow")
                elif health == "KO":
                    say(f"*** {ws_id}: AVERÍA/FUGA reportada por el monitor ({reason}) ***", "red")
                    self.cut_session(ws, f"fuga/avería detectada ({reason})")
                elif old_reason == "esperando al engine":
                    say(f"{ws_id}: monitor y engine conectados → WS DISPONIBLE", "green")
                else:
                    say(f"{ws_id}: avería resuelta, WS operativa", "green")

    def set_disconnected(self, ws_id, token, why):
        with self.lock:
            ws = self.ws.get(ws_id)
            if not ws or ws.conn_token is not token or not ws.connected:
                return
            ws.connected = False
            ws.conn_token = None
            say(f"{ws_id} DESCONECTADA: {why}", "gray")
            self.cut_session(ws, "WS desconectada de WM_Central")

    # ------------------------------------------------------------------ hilos periódicos
    def watchdog(self):
        last_panel = None
        while True:
            time.sleep(1)
            now = time.time()
            with self.lock:
                for ws in self.ws.values():
                    if ws.connected and now - ws.last_seen > HEARTBEAT_TIMEOUT:
                        self.set_disconnected(ws.id, ws.conn_token, "sin latidos del monitor")
                expired = [(r, p) for r, p in self.pending.items() if now - p["t0"] > START_TIMEOUT]
                for r, _ in expired:
                    self.pending.pop(r)
            for r, p in expired:
                self.deny(r, p["op"], p["ws"], p["src"], "la WS no responde")
                self.cmd(p["ws"], "STOP", req_id=r, reason="CENTRAL")
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
            for ws in sorted(self.ws.values(), key=lambda w: w.id):
                st = ws.state()
                s = ws.session or {}
                out.append({"id": ws.id, "location": ws.location, "state": st, "label": LABEL[st],
                            "flow": s.get("flow", 0.0), "volume": s.get("volume", 0.0),
                            "op": s.get("op", ""), "elapsed": s.get("elapsed", 0),
                            "duration": s.get("duration", 0), "total": round(ws.total, 2),
                            "reason": ws.health_reason if st == LEAK else "",
                            "health": ws.health if ws.connected else None, "blocked": ws.blocked})
            return out

    def recent_waterings(self, n=15):
        """Riegos en curso (tabla sessions) + últimos terminados (tabla waterings) para el panel web."""
        with self.lock:
            live = {ws.session["req_id"]: ws.session["volume"] for ws in self.ws.values() if ws.session}
            cut = set(self.interrupted)
        rows = []
        for req, ws_id, op, _dur, started in self.db.q("SELECT req_id, ws, op, duration, started FROM sessions"):
            rows.append({"ts": started, "ws": ws_id, "op": op, "volume": live.get(req, 0.0), "reason": "",
                         "state": "INTERRUMPIDO" if req in cut else "ACTIVE"})
        for _req, ws_id, op, vol, _dur, reason, ended in self.db.q(
                "SELECT * FROM waterings ORDER BY ended DESC LIMIT ?", (n,)):
            rows.append({"ts": ended, "ws": ws_id, "op": op, "volume": vol, "reason": reason, "state": "FINALIZADO"})
        rows.sort(key=lambda r: r["ts"] or 0, reverse=True)
        return rows[:n]

    def panel_text(self):
        rows = self.snapshot()
        lines = ["", color("=" * 92, "cyan"),
                 color(f"  WM_CENTRAL · Panel de monitorización   Kafka: {'OK' if self.bus else 'CAÍDO'}"
                       f"   Sockets: {self.args.port}", "bold"),
                 color("=" * 92, "cyan"),
                 f"  {'ID':<12}{'UBICACIÓN':<18}{'ESTADO':<16}{'CAUDAL':>10}{'VOLUMEN':>10}{'TOTAL':>10}  OPERARIO"]
        for r in rows:
            detail = ""
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
        lines.append(color("  Órdenes (escribe + Enter): start <ws|all> [seg] · block <ws|all> · activate <ws|all>"
                           f"   ·   Panel web: http://localhost:{self.args.http_port}", "gray"))
        return "\n".join(lines)

    def console(self):
        """Órdenes por teclado (solo si hay terminal: en local con docker attach / python directo)."""
        if not sys.stdin or not sys.stdin.isatty():
            return
        for line in sys.stdin:
            p = line.strip().split()
            if not p:
                continue
            if p[0] in ("start", "block", "activate") and len(p) >= 2:
                self.order(p[0], p[1], p[2] if len(p) > 2 and p[2].isdigit() else 30)
            else:
                say("Órdenes: start <ws|all> [seg] · block <ws|all> · activate <ws|all>", "yellow")

    # ------------------------------------------------------------------ panel web
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
                    # Se lee el archivo en cada petición: se puede retocar el diseño sin reiniciar la Central
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

def main():
    ap = argparse.ArgumentParser(description="WM_Central - Central de WaterManagement")
    ap.add_argument("--port", type=int, default=int(env("WM_PORT", 5050)), help="puerto de sockets para los WM_WS_M")
    ap.add_argument("--kafka", default=env("KAFKA_BOOTSTRAP", "localhost:9092"), help="host:puerto del broker Kafka")
    ap.add_argument("--http-port", type=int, default=int(env("PORT", 3000)), help="puerto del panel web")
    ap.add_argument("--db", default=env("WM_DB", "wmcentral.db"), help="fichero SQLite")
    args = ap.parse_args()

    print(color("=" * 64, "cyan"))
    print(color("  WM_Central · WaterManagement · SD 26/27", "bold"))
    print(color("=" * 64, "cyan"))
    c = Central(args)
    for t in (c.http_server, c.socket_server, c.kafka_thread, c.watchdog, c.console):
        threading.Thread(target=t, daemon=True).start()
    say(f"Panel web en http://localhost:{args.http_port}", "green")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        say("WM_Central detenida")

if __name__ == "__main__":
    main()