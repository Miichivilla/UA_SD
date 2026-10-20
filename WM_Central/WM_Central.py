#!/usr/bin/env python3
"""
WM_Central  --  Sistema central de WaterManagement (SD 26/27)

Responsabilidades
  * Servidor de SOCKETS concurrente (un hilo por monitor) para que los WM_WS_M
    se autentiquen / registren y envíen su latido de salud cada segundo.
  * Consumidor/productor KAFKA: recibe peticiones de riego (FO, menú de la WS,
    panel de Central), ordena a los engines (START/STOP/BLOCK/ACTIVATE) y
    notifica a los operarios cada paso.
  * Base de datos SQLite: operarios, estaciones, historial de riegos.
  * Panel de monitorización web (colores por estado) + log por consola.

Parámetros (línea de comandos o variable de entorno):
  --port        / WM_PORT          puerto del servidor de sockets para WM_WS_M  (5050)
  --kafka       / KAFKA_BOOTSTRAP  host:puerto del broker                     (localhost:9092)
  --http-port   / PORT             puerto del panel web (Railway inyecta PORT) (8080)
  --db          / WM_DB            fichero SQLite                              (/data/wm_central.db)
"""
import argparse
import json
import re
import socket
import threading
import time
from collections import deque

from central_db import Db
from wm_common import (ConnectionClosed, FramedConn, KafkaBus, ProtocolError,
                       T_CMD, T_EVT, T_OP, T_REQ, T_STATUS, BusError, _print_lock, color,
                       commit, env, log, make_msg, msg_age, new_id, parse_hostport,
                       poll_json, split_msg)

# --- estados -----------------------------------------------------------------
AVAILABLE, WATERING, LEAK, OOS, DISCONNECTED = (
    "AVAILABLE", "WATERING", "LEAK", "OUT_OF_SERVICE", "DISCONNECTED")
STATE_LABEL = {
    AVAILABLE: "Available / Disponible", WATERING: "Watering / Regando",
    LEAK: "Leak / Fuga", OOS: "Out of Service / Fuera de servicio",
    DISCONNECTED: "Disconnected / Desconectada",
}
STATE_COLOR = {AVAILABLE: "bg_green", WATERING: "bg_green", LEAK: "bg_red",
               OOS: "bg_orange", DISCONNECTED: "bg_grey"}

REASON_TXT = {
    "TIEMPO": "tiempo de riego cumplido",
    "MANUAL_WS": "finalizado desde la propia WS",
    "FUGA": "fuga / avería detectada",
    "BLOQUEO": "WS bloqueada por Central (Out of Service)",
    "MONITOR": "monitor de la WS desconectado",
    "INTERRUMPIDO": "el engine de la WS se cayó durante el riego",
    "SIN_TELEMETRIA": "se perdió la comunicación con la estación",
    "CENTRAL": "parada ordenada por Central",
}

HB_TIMEOUT = 5.0        # s sin latido del monitor -> Desconectada
START_TIMEOUT = 6.0     # s esperando la autorización (START_ACK) de la estación
TEL_TIMEOUT = 12.0      # s sin telemetría durante un riego
REQ_MAX_AGE = 30.0      # s: una petición más antigua se considera caducada
ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,24}$")


class WS:
    """Vista en memoria de una estación (el estado 'real' lo notifica su monitor)."""

    def __init__(self, ws_id, location, blocked=False, total=0.0):
        self.id = ws_id
        self.location = location
        self.blocked = blocked
        self.total = total          # litros ya suministrados (histórico)
        self.connected = False      # hay monitor conectado y con latido
        self.health = "OK"          # OK/KO según el monitor
        self.last_hb = 0.0
        self.conn = None
        self.gen = 0
        self.session = None         # riego en curso
        self.pending = None         # petición autorizada pendiente de ACK del engine
        self.flow = 0.0
        self.volume = 0.0
        self.operator = ""

    @property
    def state(self):
        if not self.connected:
            return DISCONNECTED
        if self.health == "KO":
            return LEAK
        if self.blocked:
            return OOS
        if self.session:
            return WATERING
        return AVAILABLE


class Central:
    def __init__(self, args):
        self.args = args
        self.db = Db(args.db)
        self.lock = threading.RLock()
        self.ws = {}
        self.events = deque(maxlen=300)
        self.bus = None
        self.kafka_ok = False
        self.started = time.time()
        self._last_sig = None
        self._seed()
        self._load()

    # ------------------------------------------------------------------ arranque
    def _seed(self):
        """Datos iniciales sólo si la BD está vacía (primera ejecución)."""
        if not self.db.stations() and env("WM_SEED", "1") == "1":
            seed = env("WM_SEED_WS",
                       "WS-01:Parque de Lo Morant;WS-02:Parque El Palmeral;WS-03:Jardines del Castillo")
            for item in seed.split(";"):
                if ":" in item:
                    i, loc = item.split(":", 1)
                    self.db.add_station(i.strip(), loc.strip())
        if not self.db.operators() and env("WM_SEED", "1") == "1":
            for i in range(1, 6):
                self.db.add_operator(f"OP-{i:02d}", f"Operario {i}")

    def _load(self):
        """Al arrancar/reiniciar: carga de BD. Todas las WS 'Desconectadas' hasta que conecten."""
        for r in self.db.stations():
            self.ws[r["id"]] = WS(r["id"], r["location"], bool(r["blocked"]), r["total_liters"])
        for s in self.db.active_sessions():  # riegos que seguían activos al caer Central
            w = self.ws.get(s["ws_id"])
            if w:
                w.session = {"req_id": s["req_id"], "op": s["operator_id"], "duration": s["duration"],
                             "started": s["started_at"] or time.time(), "volume": s["volume"],
                             "flow": 0.0, "last_tel": time.time(), "src": s["source"]}
                w.volume = s["volume"]
                w.operator = s["operator_id"]
                self.event(f"Riego {s['req_id']} recuperado de la BD (WS {w.id}, op {s['operator_id']})", "yellow")
        log("CENTRAL", f"BD '{self.args.db}': {len(self.ws)} WS registradas, "
                       f"{len(self.db.operators())} operarios", "cyan")
        self.print_table(force=True)

    # ------------------------------------------------------------------ utilidades
    def event(self, msg, col=None, level="info"):
        with self.lock:
            self.events.append({"ts": time.strftime("%H:%M:%S"), "msg": msg, "level": level})
        log("CENTRAL", msg, col)

    def publish(self, topic, msg, key=None):
        bus = self.bus
        if bus is None:
            self.event(f"Kafka no disponible: se descarta {msg.get('t')}", "red", "error")
            return False
        try:
            bus.send(topic, msg, key=key)
            return True
        except BusError as e:
            self.event(f"Kafka no disponible ({e}): se descarta {msg.get('t')}", "red", "error")
            return False

    def notify(self, op, kind, text, ws_id=None, req_id=None, **extra):
        """Mensaje para un operario (sólo los de SU petición) y traza en Central."""
        self.event(f"[{op}] {text}")
        if op and op != "CENTRAL" and not op.startswith("WS:"):
            self.publish(T_OP, {"t": "NOTIFY", "op": op, "kind": kind, "msg": text,
                                "ws": ws_id, "req_id": req_id, **extra}, key=op)

    def send_cmd(self, ws_id, t, **kw):
        return self.publish(T_CMD, {"t": t, "ws": ws_id, **kw}, key=ws_id)

    def snapshot(self):
        out = []
        with self.lock:
            for w in sorted(self.ws.values(), key=lambda x: x.id):
                live = w.volume if w.session else 0.0
                out.append({
                    "id": w.id, "location": w.location, "state": w.state,
                    "label": STATE_LABEL[w.state],
                    "flow": round(w.flow, 1) if w.session else 0.0,
                    "volume": round(live, 2), "total": round(w.total + live, 2),
                    "operator": w.operator if w.session else "",
                    "blocked": w.blocked, "health": w.health,
                    "duration": w.session["duration"] if w.session else 0,
                    "elapsed": int(time.time() - w.session["started"]) if w.session else 0,
                })
        return out

    def print_table(self, force=False):
        snap = self.snapshot()
        sig = tuple((s["id"], s["state"]) for s in snap)
        if not force and sig == self._last_sig:
            return
        self._last_sig = sig
        lines = [color("  ID      UBICACIÓN                       ESTADO                    AGUA", "bold")]
        for s in snap:
            extra = f"{s['total']:.1f} L"
            if s["state"] == WATERING:
                extra += f"  ({s['flow']} L/min, {s['volume']} L, op {s['operator']})"
            st = color(f" {s['label']:<34}", STATE_COLOR[s["state"]])
            lines.append(f"  {s['id']:<7} {s['location']:<31} {st} {extra}")
        with _print_lock:
            print("\n".join(lines), flush=True)

    # ------------------------------------------------------------------ sockets
    def serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.args.port))
        srv.listen(50)
        self.event(f"Servidor de sockets escuchando en el puerto {self.args.port}", "green")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.serve_monitor, args=(conn, addr), daemon=True).start()

    def serve_monitor(self, sock, addr):
        fc = FramedConn(sock)
        ws_id, gen, why = None, None, "conexión cerrada"
        try:
            first = split_msg(fc.recv(timeout=10))
            op = first[0]
            if op == "REGISTER" and len(first) == 3:
                ok, reply = self.authenticate(first[1], first[2], register=True)
            elif op == "AUTH" and len(first) >= 2:
                ok, reply = self.authenticate(first[1], None, register=False)
            else:
                ok, reply = False, "AUTH_KO#Mensaje no válido"
            fc.send(reply)
            if not ok:
                self.event(f"Autenticación DENEGADA desde {addr[0]}: {reply.split('#', 1)[1]}", "red", "error")
                return
            ws_id = first[1]
            gen = self.monitor_connected(ws_id, fc, addr)
            while True:
                f = split_msg(fc.recv(timeout=HB_TIMEOUT))
                if f[0] == "HB" and len(f) >= 3 and f[1] == ws_id:
                    fc.send(self.on_heartbeat(ws_id, f[2]))
                else:
                    fc.send("ERR#Operación desconocida")
        except TimeoutError:
            why = f"sin latidos durante {HB_TIMEOUT:.0f}s"
        except (ConnectionClosed, ProtocolError, OSError) as e:
            why = f"conexión perdida ({e})"
        finally:
            fc.close()
            if ws_id and gen:
                self.monitor_lost(ws_id, gen, why)

    def authenticate(self, ws_id, location, register):
        if not ID_RE.match(ws_id or ""):
            return False, "AUTH_KO#ID de WS no válido"
        with self.lock:
            if register and ws_id not in self.ws:
                loc = (location or "Sin ubicación").strip()[:60]
                self.db.add_station(ws_id, loc)
                self.ws[ws_id] = WS(ws_id, loc)
                self.event(f"ALTA de nueva WS {ws_id} en '{loc}'", "green")
            w = self.ws.get(ws_id)
            if w is None:
                return False, "AUTH_KO#WS no registrada en WM_Central"
            return True, make_msg("AUTH_OK", w.id, w.location)

    def monitor_connected(self, ws_id, fc, addr):
        with self.lock:
            w = self.ws[ws_id]
            if w.conn is not None:       # reconexión: el monitor nuevo sustituye al viejo
                w.conn.close()
            w.gen += 1
            w.conn, w.connected, w.health, w.last_hb = fc, True, "OK", time.time()
            gen = w.gen
        self.event(f"WS {ws_id} AUTENTICADA y conectada desde {addr[0]}", "green")
        self.print_table()
        return gen

    def monitor_lost(self, ws_id, gen, why):
        with self.lock:
            w = self.ws.get(ws_id)
            if not w or w.gen != gen or not w.connected:
                return
            self._disconnect(w, why)

    def _disconnect(self, w, why):
        w.connected = False
        w.conn = None
        self.event(f"WS {w.id} DESCONECTADA: {why}", "red", "error")
        if w.session:
            req = w.session["req_id"]
            self._end_session(w, w.volume, "MONITOR")
            self.send_cmd(w.id, "STOP", req_id=req, reason="MONITOR")
        self.print_table()

    def on_heartbeat(self, ws_id, health):
        health = "KO" if health == "KO" else "OK"
        with self.lock:
            w = self.ws[ws_id]
            w.last_hb = time.time()
            if health != w.health:
                if health == "KO":
                    self.event(f"WS {ws_id}: FUGA / avería notificada por su monitor", "red", "error")
                    if w.session:
                        req = w.session["req_id"]
                        self._end_session(w, w.volume, "FUGA")
                        self.send_cmd(ws_id, "STOP", req_id=req, reason="FUGA")
                else:
                    self.event(f"WS {ws_id}: avería RESUELTA, vuelve a estar disponible", "green")
                w.health = health
                self.print_table()
            return make_msg("HB_OK", w.state, 1 if w.blocked else 0, round(w.flow, 1),
                            round(w.volume, 2), w.operator if w.session else "-")

    # ------------------------------------------------------------------ sesiones
    def _end_session(self, w, volume, reason, duration=None):
        """Cierra el riego en curso (lock ya tomado)."""
        s = w.session
        if not s:
            return
        w.session = None
        dur = duration if duration is not None else time.time() - s["started"]
        w.total += volume
        self.db.add_liters(w.id, volume)
        self.db.end_session(s["req_id"], volume, reason)
        w.flow, w.volume, w.operator = 0.0, 0.0, ""
        self.notify(s["op"], "FINAL",
                    f"RIEGO FINALIZADO en {w.id}: {REASON_TXT.get(reason, reason)}. "
                    f"Volumen total {volume:.1f} L en {dur:.0f} s.",
                    w.id, s["req_id"], volume=round(volume, 2), duration=round(dur), reason=reason)

    def deny(self, op, req_id, ws_id, reason, src="FO", duration=0):
        self.db.insert_session(req_id, ws_id or "-", op, src, duration, "DENIED", reason=reason)
        self.notify(op, "DENIED", f"PETICIÓN DENEGADA: {reason}", ws_id, req_id)
        if src == "WS" and ws_id:
            self.send_cmd(ws_id, "DENIED", req_id=req_id, reason=reason)

    def on_request(self, m, age=0.0):
        req_id = m.get("req_id") or new_id("R")
        op = str(m.get("op") or "?")
        ws_id = str(m.get("ws") or "")
        src = m.get("src", "FO")
        try:
            duration = max(5, min(600, int(m.get("duration", 30))))
        except (TypeError, ValueError):
            duration = 30
        if self.db.session(req_id):      # duplicado (reentrega de Kafka)
            return
        self.notify(op, "INFO", f"WM_Central recibió tu petición de riego en {ws_id} ({duration} s).",
                    ws_id, req_id)
        if age > REQ_MAX_AGE:
            return self.deny(op, req_id, ws_id, "petición caducada (WM_Central no estaba disponible)", src, duration)
        with self.lock:
            if src == "FO" and not self.db.operator_exists(op):
                return self.deny(op, req_id, ws_id, f"operario '{op}' no registrado", src, duration)
            w = self.ws.get(ws_id)
            if w is None:
                return self.deny(op, req_id, ws_id, f"la estación '{ws_id}' no existe", src, duration)
            st = w.state
            if w.pending:
                return self.deny(op, req_id, ws_id, "WS ocupada (otra petición en curso)", src, duration)
            if st == WATERING:
                return self.deny(op, req_id, ws_id, f"WS ocupada: ya está regando para {w.operator}", src, duration)
            if st != AVAILABLE:
                why = {DISCONNECTED: "WS desconectada", LEAK: "WS con fuga/avería",
                       OOS: "WS fuera de servicio (bloqueada por Central)"}[st]
                return self.deny(op, req_id, ws_id, why, src, duration)
            self.notify(op, "INFO", f"{ws_id} está disponible. Solicitando autorización a la estación...",
                        ws_id, req_id)
            w.pending = {"req_id": req_id, "op": op, "duration": duration, "t0": time.time(), "src": src}
            self.db.insert_session(req_id, ws_id, op, src, duration, "PENDING")
            if not self.send_cmd(ws_id, "START", req_id=req_id, op=op, duration=duration):
                w.pending = None
                self.deny(op, req_id, ws_id, "no se pudo contactar con la estación (Kafka)", src, duration)

    def on_event(self, m, age=0.0):
        t, ws_id = m.get("t"), m.get("ws")
        with self.lock:
            w = self.ws.get(ws_id)
            if w is None:
                return
            if t == "START_ACK":
                p = w.pending
                if not p or p["req_id"] != m.get("req_id"):
                    return
                w.pending = None
                w.session = {"req_id": p["req_id"], "op": p["op"], "duration": p["duration"],
                             "started": time.time(), "volume": 0.0, "flow": 0.0,
                             "last_tel": time.time(), "src": p["src"]}
                w.operator, w.volume, w.flow = p["op"], 0.0, 0.0
                self.db.activate_session(p["req_id"], time.time())
                self.notify(p["op"], "AUTH", f"RIEGO AUTORIZADO: {w.id} abre la electroválvula "
                            f"({p['duration']} s).", w.id, p["req_id"])
                self.print_table()
            elif t == "START_NACK":
                p = w.pending
                if p and p["req_id"] == m.get("req_id"):
                    w.pending = None
                    self.deny(p["op"], p["req_id"], w.id, f"la estación rechazó el riego: {m.get('reason', '?')}",
                              p["src"], p["duration"])
            elif t == "TELEMETRY":
                s = w.session
                if age > 5 or not s or s["req_id"] != m.get("req_id"):
                    return
                s["last_tel"] = time.time()
                w.flow, w.volume = float(m.get("flow", 0)), float(m.get("volume", 0))
                s["volume"] = w.volume
                self.db.update_volume(s["req_id"], w.volume)
                el = int(m.get("elapsed", 0))
                self.notify(s["op"], "TELEMETRY",
                            f"Regando en {w.id}: {w.flow:.1f} L/min | {w.volume:.2f} L acumulados | "
                            f"{el}/{s['duration']} s", w.id, s["req_id"],
                            flow=round(w.flow, 1), volume=round(w.volume, 2), elapsed=el)
            elif t == "END":
                self._on_end(w, m)

    def _on_end(self, w, m):
        req_id, vol = m.get("req_id"), float(m.get("volume", 0))
        reason, dur = m.get("reason", "TIEMPO"), m.get("duration")
        s = w.session
        if s and s["req_id"] == req_id:
            self._end_session(w, vol, reason, dur)
            self.print_table()
            return
        row = self.db.session(req_id)
        if row and row["status"] == "ENDED":
            # Central ya lo había cerrado (fuga, bloqueo...): el engine envía el dato FINAL exacto.
            diff = vol - (row["volume"] or 0)
            if abs(diff) > 0.005:
                w.total += diff
                self.db.add_liters(w.id, diff)
                self.db.set_session_volume_final(req_id, vol)
                self.notify(row["operator_id"], "FINAL",
                            f"RESUMEN FINAL actualizado por {w.id}: volumen total {vol:.1f} L "
                            f"(estado final enviado por la estación tras recuperarse).",
                            w.id, req_id, volume=round(vol, 2), duration=dur, reason=reason)
        elif row is None:
            self.event(f"END de un riego desconocido ({req_id}) en {w.id}: {vol:.1f} L", "yellow")

    # ------------------------------------------------------------------ órdenes de Central (panel)
    def cmd_start(self, target, duration=30):
        ids = [w.id for w in self.ws.values()] if target == "ALL" else [target]
        for i in ids:
            self.on_request({"req_id": new_id("C"), "op": "CENTRAL", "ws": i,
                             "src": "CENTRAL", "duration": duration})

    def cmd_block(self, target):
        for i in self._targets(target):
            with self.lock:
                w = self.ws[i]
                w.blocked = True
                self.db.set_blocked(i, True)
                if w.session:
                    req = w.session["req_id"]
                    self._end_session(w, w.volume, "BLOQUEO")
                    self.send_cmd(i, "STOP", req_id=req, reason="BLOQUEO")
            self.send_cmd(i, "BLOCK")
            self.event(f"WS {i} BLOQUEADA por Central -> Out of Service", "yellow")
        self.print_table()

    def cmd_activate(self, target):
        for i in self._targets(target):
            with self.lock:
                self.ws[i].blocked = False
                self.db.set_blocked(i, False)
            self.send_cmd(i, "ACTIVATE")
            self.event(f"WS {i} ACTIVADA por Central", "green")
        self.print_table()

    def _targets(self, target):
        return list(self.ws) if target == "ALL" else ([target] if target in self.ws else [])

    def add_ws(self, ws_id, location):
        if not ID_RE.match(ws_id or ""):
            raise ValueError("ID no válido")
        with self.lock:
            if ws_id in self.ws:
                raise ValueError("la WS ya existe")
            self.db.add_station(ws_id, location or "Sin ubicación")
            self.ws[ws_id] = WS(ws_id, location or "Sin ubicación")
        self.event(f"ALTA de WS {ws_id} ('{location}') desde el panel", "green")
        self.print_table()

    def add_operator(self, op_id, name):
        if not ID_RE.match(op_id or ""):
            raise ValueError("ID no válido")
        self.db.add_operator(op_id, name or "")
        self.event(f"ALTA del operario {op_id}", "green")

    # ------------------------------------------------------------------ hilos de fondo
    def kafka_loop(self):
        host, port = parse_hostport(self.args.kafka)
        bootstrap = f"{host}:{port}"
        while True:
            consumer = None
            try:
                bus = KafkaBus(bootstrap, "wm-central")
                bus.ensure_topics()
                consumer = bus.group_consumer("wm-central", [T_REQ, T_EVT], from_start=True)
                self.bus, self.kafka_ok = bus, True
                self.event(f"Conectado a Kafka ({bootstrap})", "green")
                while True:
                    m, raw = poll_json(consumer, 1.0)
                    if m is None:
                        continue
                    try:
                        age = msg_age(raw)           # antigüedad según el reloj del BROKER
                        if raw.topic() == T_REQ:
                            self.on_request(m, age)
                        else:
                            self.on_event(m, age)
                    except Exception as e:
                        self.event(f"Error procesando mensaje {m.get('t')}: {e!r}", "red", "error")
                    commit(consumer, raw)       # sólo se confirma DESPUÉS de procesar
            except Exception as e:
                self.kafka_ok = False
                self.event(f"Kafka no disponible ({e}). Reintentando en 3 s...", "red", "error")
                time.sleep(3)
            finally:
                try:
                    if consumer:
                        consumer.close()
                except Exception:
                    pass

    def status_loop(self):
        while True:
            time.sleep(1)
            if self.bus and self.kafka_ok:
                self.publish(T_STATUS, {"t": "SNAPSHOT", "wss": self.snapshot()}, key="status")
            self.print_table()

    def reaper_loop(self):
        while True:
            time.sleep(1)
            now = time.time()
            with self.lock:
                for w in list(self.ws.values()):
                    if w.connected and now - w.last_hb > HB_TIMEOUT:
                        self._disconnect(w, f"sin latidos durante {HB_TIMEOUT:.0f}s")
                    if w.pending and now - w.pending["t0"] > START_TIMEOUT:
                        p, w.pending = w.pending, None
                        self.deny(p["op"], p["req_id"], w.id,
                                  "la estación no respondió a tiempo", p["src"], p["duration"])
                    if w.session and now - w.session["last_tel"] > TEL_TIMEOUT:
                        req = w.session["req_id"]
                        self._end_session(w, w.volume, "SIN_TELEMETRIA")
                        self.send_cmd(w.id, "STOP", req_id=req, reason="CENTRAL")
                        self.print_table()


def main():
    ap = argparse.ArgumentParser(description="WM_Central - sistema central de WaterManagement")
    ap.add_argument("--port", type=int, default=int(env("WM_PORT", 5050)),
                    help="puerto del servidor de sockets para los WM_WS_M")
    ap.add_argument("--kafka", default=env("KAFKA_BOOTSTRAP", "localhost:9092"),
                    help="host:puerto del broker/bootstrap-server de Kafka")
    ap.add_argument("--http-port", type=int, default=int(env("PORT", 8080)),
                    help="puerto del panel web de monitorización")
    ap.add_argument("--db", default=env("WM_DB", "/data/wm_central.db"), help="fichero SQLite")
    args = ap.parse_args()

    print(color("=" * 64, "cyan"))
    print(color("  WM_Central  ·  WaterManagement  ·  SD 26/27", "bold"))
    print(color("=" * 64, "cyan"))
    central = Central(args)

    from central_web import start_web
    for target in (central.serve, central.kafka_loop, central.status_loop, central.reaper_loop):
        threading.Thread(target=target, daemon=True).start()
    start_web(central, args.http_port)           # bloquea (hilo principal)


if __name__ == "__main__":
    main()
