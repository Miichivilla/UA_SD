import argparse
import json
import os
import random
import sys
import threading
import time

from wm_common import (BusError, ConnectionClosed, KafkaBus, ProtocolError, T_CMD, T_EVT, T_REQ, color, connect, env, log, new_id, parse_hostport, poll_json, split_msg)

try:                     # lectura de UNA tecla sin Enter (Linux/macOS/contenedor con -it)
    import termios
    import tty
    HAVE_TERMIOS = True
except ImportError:      # Windows nativo: se usa el modo por líneas
    HAVE_TERMIOS = False

def read_key():
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        return sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


REASON_TXT = {"TIEMPO": "tiempo cumplido", "MANUAL_WS": "finalizado desde la WS", "FUGA": "fuga/avería",
              "BLOQUEO": "bloqueada por Central", "MONITOR": "monitor caído", "CENTRAL": "parada por Central",
              "INTERRUMPIDO": "engine interrumpido"}


class Outbox:
    """Cola persistente de eventos críticos (resúmenes finales) pendientes de entregar a Kafka."""

    def __init__(self):
        self.path = None
        self.items = []
        self.lock = threading.Lock()

    def open(self, path):
        with self.lock:
            self.path = path
            if os.path.exists(path):
                with open(path) as f:
                    self.items = [json.loads(l) for l in f if l.strip()]

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            for it in self.items:
                f.write(json.dumps(it) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def add(self, ev):
        with self.lock:
            self.items.append(ev)
            self._save()

    def pending(self):
        with self.lock:
            return list(self.items)

    def done(self, ev):
        with self.lock:
            if ev in self.items:
                self.items.remove(ev)
                self._save()

class Engine:
    def __init__(self, args):
        self.args = args
        self.mhost, self.mport = parse_hostport(args.monitor)
        self.kafka = args.kafka
        os.makedirs(args.data_dir, exist_ok=True)
        self.lock = threading.RLock()
        self.ws_id = None
        self.location = ""
        self.bus = None
        self.cmd_consumer = None
        self.monitor_up = False
        self.central_up = False
        self.blocked = False
        self.ko = False                 # avería simulada con la tecla 'k'
        self.session = None             # riego en curso
        self.manual_req = None
        self.outbox = Outbox()
        self.wake = threading.Event()
        self.tag = "WS ENGINE"
        self._last_status = None

    #  utilidades
    def say(self, msg, col=None):
        log(self.tag, msg, col)

    def status(self):
        if not self.monitor_up:
            return ("AVERIADA (sin monitor)", "red")
        if self.ko:
            return ("KO · fuga/avería simulada", "red")
        if self.session:
            s = self.session
            return (f"REGANDO · {s['flow']:.1f} L/min · {s['volume']:.2f} L · {s['ticks']}/{s['duration']} s · op {s['op']}", "green")
        if self.blocked:
            return ("OUT OF SERVICE (bloqueada por Central)", "orange")
        if not self.central_up:
            return ("SIN CONEXIÓN con WM_Central: no admite riegos", "red")
        return ("DISPONIBLE", "green")

    def show_status_change(self):
        st = self.status()[0].split(" ·")[0]
        if st != self._last_status:
            self._last_status = st
            text, col = self.status()
            self.say(f"ESTADO → {text}", col)

    def unavailable_reason(self):
        if not self.monitor_up:
            return "WS averiada: sin conexión con su monitor"
        if self.ko:
            return "WS con fuga/avería (KO)"
        if self.blocked:
            return "WS fuera de servicio (Out of Service)"
        if not self.central_up:
            return "Imposible conectar con WM_Central"
        if self.session:
            return "la WS ya está regando"
        return None

    #  Kafka
    def kafka_init(self):
        """Conecta con Kafka y se posiciona al final de wm.ws.commands (reintenta hasta lograrlo)."""
        warned = False
        while True:
            try:
                bus = KafkaBus(self.kafka, "wm-ws-engine")
                bus.ensure_topics()
                self.cmd_consumer = bus.tail_consumer([T_CMD])
                self.bus = bus
                self.say(f"Conectado a Kafka ({self.kafka})", "green")
                return
            except Exception as e:
                if not warned:
                    self.say(f"Imposible conectar con Kafka ({self.kafka}): {e}. Reintentando...", "red")
                    warned = True
                time.sleep(3)

    def cmd_loop(self):
        while True:
            try:
                m, _raw = poll_json(self.cmd_consumer, 1.0)
                if m and m.get("ws") == self.ws_id:
                    self.handle_cmd(m)
            except Exception as e:
                self.say(f"Error leyendo órdenes de Kafka: {e}. Reconectando...", "red")
                try:
                    self.cmd_consumer = self.bus.tail_consumer([T_CMD])
                except Exception:
                    time.sleep(2)

    def handle_cmd(self, m):
        t = m.get("t")
        if t == "START":          # (el consumidor 'tail' empieza en el final: no hay órdenes caducadas)
            self.on_start(m)
        elif t == "STOP":
            with self.lock:
                if self.session and m.get("req_id") in (self.session["req_id"], "", None):
                    self.say(f"WM_Central ordena PARAR el riego ({m.get('reason')})", "yellow")
                    self.end_session(m.get("reason") or "CENTRAL")
        elif t == "BLOCK":
            with self.lock:
                self.blocked = True
                self.say("WM_Central ha BLOQUEADO la WS → OUT OF SERVICE", "orange")
                if self.session:
                    self.end_session("BLOQUEO")
            self.show_status_change()
        elif t == "ACTIVATE":
            with self.lock:
                self.blocked = False
            self.say("WM_Central ha ACTIVADO la WS", "green")
            self.show_status_change()
        elif t == "DENIED":
            if self.manual_req == m.get("req_id"):
                self.manual_req = None
                self.say(f"WM_Central DENEGÓ el riego manual: {m.get('reason')}", "red")

    def on_start(self, m):
        req_id = m.get("req_id")
        with self.lock:
            why = self.unavailable_reason()
            if why:
                self.say(f"START {req_id} rechazado: {why}", "red")
                self.bus.send_sync(T_EVT, {"t": "START_NACK", "ws": self.ws_id, "req_id": req_id, "reason": why},
                                   key=self.ws_id)
                return
            if not self.bus.send_sync(T_EVT, {"t": "START_ACK", "ws": self.ws_id, "req_id": req_id}, key=self.ws_id):
                self.say("No se pudo confirmar el inicio a Central (Kafka): no se riega", "red")
                return
            dur = int(m.get("duration", 30))
            self.session = {"req_id": req_id, "op": m.get("op", "?"), "duration": dur,
                            "ticks": 0, "volume": 0.0, "flow": 0.0}
            self.manual_req = None
            self.persist_session()
            self.say(f"Riego AUTORIZADO por Central (req {req_id}, operario {self.session['op']}, {dur} s): "
                     f"ELECTROVÁLVULA ABIERTA", "green")
        self.show_status_change()

    #  riego
    def session_file(self):
        return os.path.join(self.args.data_dir, f"session_{self.ws_id}.json")

    def persist_session(self):
        tmp = self.session_file() + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.session, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.session_file())

    def end_session(self, reason):
        with self.lock:
            s = self.session
            if not s:
                return
            self.session = None
            try:
                os.remove(self.session_file())
            except FileNotFoundError:
                pass
            ev = {"t": "END", "ws": self.ws_id, "req_id": s["req_id"], "op": s["op"],
                  "volume": round(s["volume"], 3), "duration": s["ticks"], "reason": reason}
            self.outbox.add(ev)           # persistente: se entrega aunque Kafka/Central estén caídos
            self.wake.set()
            self.say(f"RIEGO FINALIZADO ({REASON_TXT.get(reason, reason)}): {s['volume']:.2f} L en {s['ticks']} s. "
                     f"ELECTROVÁLVULA CERRADA", "cyan")
        self.show_status_change()

    def tick_loop(self):
        nxt = time.time() + 1
        while True:
            time.sleep(max(0, nxt - time.time()))
            nxt += 1
            with self.lock:
                s = self.session
                if not s:
                    continue
                s["ticks"] += 1
                s["flow"] = random.uniform(9.0, 13.0)          # caudalímetro simulado (L/min)
                s["volume"] += s["flow"] / 60.0
                self.persist_session()
                msg = {"t": "TELEMETRY", "ws": self.ws_id, "req_id": s["req_id"], "flow": round(s["flow"], 2),
                       "volume": round(s["volume"], 3), "elapsed": s["ticks"]}
                self.say(f"Regando: {s['flow']:.1f} L/min · {s['volume']:.2f} L · {s['ticks']}/{s['duration']} s", "green")
                try:
                    self.bus.send(T_EVT, msg, key=self.ws_id)
                except Exception:
                    pass
                if s["ticks"] >= s["duration"]:
                    self.end_session("TIEMPO")

    def outbox_loop(self):
        while True:
            self.wake.wait(1.0)
            self.wake.clear()
            if self.ko or self.bus is None or self.ws_id is None:
                continue
            for ev in self.outbox.pending():
                if self.bus.send_sync(T_EVT, ev, key=self.ws_id, timeout=5):
                    self.outbox.done(ev)
                    self.say(f"Resumen final del riego {ev['req_id']} entregado a Central "
                             f"({ev['volume']:.2f} L)", "cyan")
                else:
                    break

    # enlace con el Monitor
    def link_loop(self):
        warned = None
        while True:
            fc = None
            try:
                fc = connect(self.mhost, self.mport, timeout=3)
                while True:
                    r = split_msg(fc.request("HELLO", timeout=8))
                    if r[0] == "ID":
                        break
                    if r[0] == "DENIED":
                        self.say(f"El monitor fue DENEGADO por WM_Central: {r[1] if len(r) > 1 else ''}", "red")
                        time.sleep(1)
                        os._exit(2)
                    self.say("Esperando a que el monitor se autentique con WM_Central...", "yellow")
                    time.sleep(1)
                self.identify(r[1], r[2] if len(r) > 2 else "")
                if split_msg(fc.request("READY", timeout=8))[0] != "OK":
                    raise ProtocolError("el monitor no aceptó READY")
                self.monitor_up = True
                warned = None
                self.say(f"Conectado al monitor ({self.mhost}:{self.mport})", "green")
                self.show_status_change()
                while True:
                    m = split_msg(fc.recv(timeout=6))
                    if m[0] == "PING":
                        self.on_ping(m)
                        fc.send("KO" if self.ko else "OK", timeout=2)
            except (ConnectionClosed, ProtocolError, TimeoutError, OSError) as e:
                was = self.monitor_up
                self.monitor_up = False
                if was:
                    self.say(f"Monitor perdido ({e}): WS AVERIADA, no admite riegos", "red")
                    with self.lock:
                        if self.session:
                            self.end_session("MONITOR")
                    self.show_status_change()
                elif warned != str(e):
                    self.say(f"Esperando al monitor en {self.mhost}:{self.mport}... ({e})", "yellow")
                    warned = str(e)
                time.sleep(2)
            finally:
                if fc:
                    fc.close()

    def on_ping(self, m):
        changed = False
        with self.lock:
            c_up = m[1] == "1"
            if c_up != self.central_up:
                self.central_up, changed = c_up, True
                self.say("Conexión con WM_Central RESTABLECIDA" if c_up else
                         "Imposible conectar con WM_Central (el riego en curso continúa hasta finalizar)",
                         "green" if c_up else "red")
            if c_up and m[2] in ("0", "1"):
                b = m[2] == "1"
                if b != self.blocked:
                    self.blocked, changed = b, True
                    if b and self.session:
                        self.end_session("BLOQUEO")
        if changed:
            self.show_status_change()

    def identify(self, ws_id, location):
        if self.ws_id == ws_id:
            return
        self.ws_id, self.location = ws_id, location
        self.tag = f"{ws_id} ENGINE"
        self.say(f"ID asignado por el monitor: {ws_id} · {location}", "cyan")
        self.outbox.open(os.path.join(self.args.data_dir, f"outbox_{ws_id}.jsonl"))
        # ¿Hubo un riego a medias cuando el engine cayó? -> se informa a Central.
        sf = self.session_file()
        if os.path.exists(sf):
            try:
                with open(sf) as f:
                    s = json.load(f)
                self.outbox.add({"t": "END", "ws": ws_id, "req_id": s["req_id"], "op": s["op"],
                                 "volume": round(s["volume"], 3), "duration": s["ticks"],
                                 "reason": "INTERRUMPIDO"})
                os.remove(sf)
                self.say(f"Riego interrumpido recuperado del disco: {s['volume']:.2f} L "
                         f"(se enviará a Central)", "yellow")
            except Exception as e:
                self.say(f"No se pudo recuperar el riego interrumpido: {e}", "red")
        self.wake.set()

    #  teclado
    def menu(self):
        self.say("Menú:  [r] regar manualmente · [f] finalizar riego · [k] simular KO/recuperar · "
                 "[s] estado · [h] ayuda · [q] salir", "cyan")

    def keyboard_loop(self):
        self.menu()
        single_key = sys.stdin.isatty() and HAVE_TERMIOS
        while True:
            try:
                if single_key:
                    ch = read_key()
                    line = "" if ch == "" else ch
                else:
                    line = sys.stdin.readline()
            except Exception:
                return
            if line == "":             # stdin cerrado (docker sin -it): el engine sigue funcionando
                while True:
                    time.sleep(3600)
            parts = line.strip().lower().split()
            if not parts:
                continue
            c = parts[0]
            if c in ("r", "1"):
                dur = 30
                if len(parts) > 1 and parts[1].isdigit():
                    dur = int(parts[1])
                elif single_key:
                    try:
                        d = input("  Duración del riego en segundos [30]: ").strip()
                        dur = int(d) if d.isdigit() else 30
                    except EOFError:
                        pass
                self.manual_start(dur)
            elif c in ("f", "2"):
                if self.session:
                    self.end_session("MANUAL_WS")
                else:
                    self.say("No hay ningún riego en curso", "yellow")
            elif c in ("k", "3"):
                self.toggle_ko()
            elif c in ("s", "4"):
                t, col = self.status()
                self.say(f"ESTADO: {t}  (Central: {'ok' if self.central_up else 'caída'}, "
                         f"monitor: {'ok' if self.monitor_up else 'caído'})", col)
            elif c in ("q", "0"):
                self.say("Engine detenido.")
                os._exit(0)
            else:
                self.menu()

    def manual_start(self, duration):
        with self.lock:
            why = self.unavailable_reason()
        if why:
            self.say(f"No se puede regar: {why}", "red")
            return
        req_id = new_id("M")
        self.manual_req = req_id
        ok = self.bus and self.bus.send_sync(
            T_REQ, {"t": "REQ", "req_id": req_id, "op": f"WS:{self.ws_id}", "ws": self.ws_id,
                    "src": "WS", "duration": duration}, key=self.ws_id)
        if ok:
            self.say(f"Petición de riego manual ({duration} s) enviada a WM_Central. Esperando autorización...", "cyan")
        else:
            self.manual_req = None
            self.say("Imposible conectar con WM_Central (Kafka no responde)", "red")

    def toggle_ko(self):
        with self.lock:
            self.ko = not self.ko
            if self.ko:
                self.say("*** KO: se simula una FUGA/AVERÍA. Se reportará KO al monitor ***", "red")
                if self.session:
                    self.end_session("FUGA")
            else:
                self.say("*** Avería RESUELTA: se reporta OK al monitor ***", "green")
                self.wake.set()       # entrega los resúmenes retenidos
        self.show_status_change()

def main():
    ap = argparse.ArgumentParser(description="WM_WS_E - Engine de la Watering Station")
    ap.add_argument("--kafka", default=env("KAFKA_BOOTSTRAP", "localhost:9092"), help="host:puerto del broker de Kafka")
    ap.add_argument("--monitor", default=env("WM_MONITOR", "localhost:6000"), help="host:puerto del WM_WS_M")
    ap.add_argument("--data-dir", default=env("WM_DATA_DIR", "./data"), help="carpeta del estado persistente")
    args = ap.parse_args()
    print(color("=" * 64, "cyan"))
    print(color("  WM_WS_E · Engine de la Watering Station · SD 26/27", "bold"))
    print(color("=" * 64, "cyan"))
    e = Engine(args)
    e.kafka_init()
    for t in (e.cmd_loop, e.tick_loop, e.outbox_loop, e.link_loop):
        threading.Thread(target=t, daemon=True).start()
    try:
        e.keyboard_loop()
    except KeyboardInterrupt:
        print("\nEngine detenido.")

if __name__ == "__main__":
    main()