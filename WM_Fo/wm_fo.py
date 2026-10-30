import argparse
import sys
import threading
import time

from wm_common import (T_FO, T_REQ, T_STATUS, color, connect_kafka, env, log, new_id, poll_json)

CENTRAL_TIMEOUT = 3.5      # s sin latido de Central -> "Imposible conectar con WM_Central"
AUTH_TIMEOUT = 15          # s máximo esperando la autorización/denegación

class FO:
    def __init__(self, args):
        self.args = args
        self.op = args.id
        self.tag = f"FO {self.op}"
        self.bus = None
        self.status = []            # última lista de WS publicada por Central
        self.last_status = 0.0
        self.waiters = {}           # req_id -> {"event", "result", "authorized"}
        self.lock = threading.Lock()
        self.central_was_up = None

    def say(self, msg, col=None):
        log(self.tag, msg, col)

    def central_up(self):
        return time.time() - self.last_status < CENTRAL_TIMEOUT

    # ------------------------------------------------------------------ hilos Kafka
    def status_loop(self):
        c = self.bus.tail_consumer([T_STATUS])
        while True:
            m, _ = poll_json(c, 1.0)
            if m and m.get("t") == "STATUS":
                self.status = m.get("ws", [])
                self.last_status = time.time()
            up = self.central_up()
            if up != self.central_was_up:
                if up:
                    self.say("Conectado con WM_Central", "green")
                elif self.central_was_up is not None:
                    self.say("Imposible conectar con WM_Central", "red")
                self.central_was_up = up

    def notify_loop(self):
        # Grupo propio del operario: si la app se cierra, al volver recibe lo que se perdió
        c = self.bus.group_consumer([T_FO], f"wm-fo-{self.op}")
        while True:
            m, _ = poll_json(c, 1.0)
            if not m or m.get("op") != self.op:
                continue                       # solo mensajes de MIS riegos
            self.on_notify(m)

    def on_notify(self, m):
        t, req = m.get("t"), m.get("req_id")
        with self.lock:
            w = self.waiters.get(req)
        prefix = f"[{m.get('ws', '')} · {req}]" if w else f"[{m.get('ws', '')} · {req} · servicio anterior]"
        if t == "INFO":
            self.say(f"{prefix} {m['msg']}", "cyan")
        elif t == "AUTHORIZED":
            self.say(f"{prefix} {m['msg']}", "green")
            if w:
                w["authorized"] = True
        elif t == "TELEMETRY":
            if not w:
                return                         # telemetría antigua de un servicio anterior: no se repite
            self.say(f"{prefix} Regando: {m['flow']:.1f} L/min · {m['volume']:.2f} L · "
                     f"{m['elapsed']}/{m['duration']} s", "green")
        elif t == "INCIDENT":
            self.say(f"{prefix} {m['msg']}", "red")
        elif t in ("DENIED", "END"):
            self.say(f"{prefix} {m['msg']}", "red" if t == "DENIED" else "cyan")
            if w:
                w["result"] = t
                w["event"].set()

    # ------------------------------------------------------------------ peticiones
    def request(self, ws, duration):
        """Envía una petición y espera a que concluya. Devuelve 'END', 'DENIED' o 'ERROR'."""
        if not self.central_up():
            self.say(f"No se puede pedir riego en {ws}: Imposible conectar con WM_Central", "red")
            return "ERROR"
        req = new_id("F")
        w = {"event": threading.Event(), "result": None, "authorized": False}
        with self.lock:
            self.waiters[req] = w
        ok = self.bus.send_sync(T_REQ, {"t": "REQ", "req_id": req, "op": self.op, "ws": ws,
                                        "src": "FO", "duration": int(duration)}, key=ws)
        if not ok:
            self.say("Imposible enviar la petición (Kafka no responde)", "red")
            return "ERROR"
        self.say(f"Petición {req} enviada: riego de {duration} s en {ws}. Esperando a WM_Central...", "yellow")
        t0, warned = time.time(), False
        limit = AUTH_TIMEOUT
        while not w["event"].wait(1):
            if w["authorized"]:
                limit = int(duration) + 120      # ya está regando: esperamos al resumen final
            if not self.central_up() and not warned:
                self.say("Imposible conectar con WM_Central. La WS terminará el riego; "
                         "el resumen llegará cuando Central vuelva.", "red")
                warned = True
            if time.time() - t0 > limit:
                self.say(f"Sin respuesta de WM_Central para {req}. Se pasa a la siguiente.", "red")
                return "ERROR"
        return w["result"]

    def run_file(self, path):
        try:
            with open(path, encoding="utf-8") as f:
                lines = [l.strip() for l in f if l.strip() and not l.strip().startswith("#")]
        except FileNotFoundError:
            self.say(f"El fichero {path} no existe", "red")
            return
        self.say(f"Procesando {len(lines)} peticiones del fichero {path}", "cyan")
        while not self.central_up():            # espera inicial a que Central esté viva
            time.sleep(1)
        for i, line in enumerate(lines, 1):
            parts = [p.strip() for p in line.split(",")]
            ws, dur = parts[0], (parts[1] if len(parts) > 1 and parts[1].isdigit() else "10")
            self.say(f"--- Petición {i}/{len(lines)}: {ws}, {dur} s ---", "bold")
            self.request(ws, dur)
            if i < len(lines):
                self.say("Esperando 4 segundos...", "gray")
                time.sleep(4)
        self.say("Fichero completado", "green")

    def show_ws(self):
        if not self.central_up():
            self.say("Imposible conectar con WM_Central", "red")
            return
        cols = {"AVAILABLE": "green", "WATERING": "green", "LEAK": "red",
                "OUT_OF_SERVICE": "orange", "DISCONNECTED": "gray"}
        print(color(f"  {'ID':<12}{'UBICACIÓN':<20}ESTADO", "bold"))
        for w in self.status:
            print(color(f"  {w['id']:<12}{w['location'][:19]:<20}{w['label']}", cols.get(w["state"])))
        avail = [w["id"] for w in self.status if w["state"] == "AVAILABLE"]
        print(f"  Disponibles: {', '.join(avail) or 'ninguna'}")

    def menu_loop(self):
        if not sys.stdin or not sys.stdin.isatty():
            while True:                       # sin terminal: solo modo fichero
                time.sleep(3600)
        while True:
            print(color("\n  [1] Solicitar riego  [2] Ver WS  [3] Procesar fichero  [0] Salir", "cyan"))
            try:
                op = input("  Opción: ").strip()
            except EOFError:
                return
            if op == "1":
                ws = input("  ID de la WS: ").strip()
                d = input("  Duración en segundos [10]: ").strip()
                if ws:
                    self.request(ws, d if d.isdigit() else "10")
            elif op == "2":
                self.show_ws()
            elif op == "3":
                p = input(f"  Fichero [{self.args.file or 'solicitudes.txt'}]: ").strip()
                self.run_file(p or self.args.file or "solicitudes.txt")
            elif op == "0":
                return

def main():
    ap = argparse.ArgumentParser(description="WM_FO - Aplicación del operario")
    ap.add_argument("--kafka", default=env("KAFKA_BOOTSTRAP"), required=env("KAFKA_BOOTSTRAP") is None,
                    help="host:puerto del broker Kafka (o variable KAFKA_BOOTSTRAP)")
    ap.add_argument("--id", default=env("FO_ID"), required=env("FO_ID") is None, help="ID del operario")
    ap.add_argument("--file", default=env("FO_FILE"), help="fichero de peticiones a procesar al arrancar")
    args = ap.parse_args()
    print(color("=" * 64, "cyan"))
    print(color(f"  WM_FO · Operario {args.id} · SD 26/27", "bold"))
    print(color("=" * 64, "cyan"), flush=True)
    fo = FO(args)
    fo.bus = connect_kafka(args.kafka, f"wm-fo-{args.id}", fo.say)
    threading.Thread(target=fo.status_loop, daemon=True).start()
    threading.Thread(target=fo.notify_loop, daemon=True).start()
    try:
        if args.file:
            fo.run_file(args.file)
        fo.menu_loop()
    except KeyboardInterrupt:
        pass
    print("\nWM_FO detenido.")

if __name__ == "__main__":
    main()