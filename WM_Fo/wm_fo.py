#!/usr/bin/env python3
"""
WM_FO  --  Field Operator: aplicación del operario de campo (SD 26/27)

  * Solicita riegos a WM_Central por Kafka (topic wm.requests) y recibe, en
    tiempo real, SÓLO los mensajes de sus propias peticiones (topic wm.operators).
  * Ve el estado de TODAS las estaciones (topic wm.status).
  * Modo fichero: lee una lista de servicios; envía el primero y, cuando concluye
    (éxito o fracaso), espera 4 s y solicita el siguiente.
  * Si la aplicación se cierra durante un riego, el riego continúa; al volver a
    arrancar con el mismo ID el operario recibe lo que se perdió ("[pendiente]").
    (consumidor con grupo wm-fo-<ID> y offsets confirmados)

Parámetros (línea de comandos o variable de entorno):
  --kafka   / KAFKA_BOOTSTRAP   host:puerto del broker
  --id      / FO_ID             ID del operario (obligatorio, registrado en Central)
  --file    / FO_FILE           fichero de servicios (opcional)
  --duration / FO_DURATION      duración por defecto del riego en segundos (30)

Formato del fichero de servicios (una línea por servicio):
      WS-01#20        ->  estación WS-01, 20 segundos
      WS-02           ->  estación WS-02, duración por defecto
  Se aceptan  #  ,  ;  o espacios como separador y líneas de comentario con '#'.
"""
import argparse
import re
import sys
import threading
import time

from wm_common import (BusError, KafkaBus, T_OP, T_REQ, T_STATUS, color, commit, env, log, new_id,
                       poll_json)

KIND_COL = {"INFO": "cyan", "AUTH": "green", "DENIED": "red", "TELEMETRY": "green", "FINAL": "yellow"}
STATE_COL = {"AVAILABLE": "bg_green", "WATERING": "bg_green", "LEAK": "bg_red",
             "OUT_OF_SERVICE": "bg_orange", "DISCONNECTED": "bg_grey"}
CENTRAL_STALE = 6.0   # s sin "foto" de estado => WM_Central no disponible


class FO:
    def __init__(self, args):
        self.args = args
        self.op = args.id
        self.bus = None
        self.started = time.time()
        self.wss = []
        self.last_snap = 0.0
        self.done = {}               # req_id -> Event (concluido: FINAL o DENIED)
        self.outcome = {}            # req_id -> 'AUTH' | 'DENIED'
        self.lock = threading.Lock()
        self.tag = f"FO {self.op}"

    def say(self, msg, col=None):
        log(self.tag, msg, col)

    # ------------------------------------------------------------------ Kafka
    def connect(self):
        warned = False
        while True:
            try:
                self.bus = KafkaBus(self.args.kafka, f"wm-fo-{self.op}")
                self.bus.ensure_topics()
                self.say(f"Conectado a Kafka ({self.args.kafka})", "green")
                return
            except Exception as e:
                if not warned:
                    self.say(f"Imposible conectar con Kafka ({self.args.kafka}): {e}. Reintentando...", "red")
                    warned = True
                time.sleep(3)

    def notif_loop(self):
        """Mensajes de MIS peticiones. Grupo propio con commit => no se pierde ninguno."""
        while True:
            consumer = None
            try:
                consumer = self.bus.group_consumer(f"wm-fo-{self.op}", [T_OP], from_start=True)
                from confluent_kafka import TopicPartition
                _lo, backlog_end = consumer.get_watermark_offsets(TopicPartition(T_OP, 0), timeout=10)
                while True:
                    m, raw = poll_json(consumer, 1.0)
                    if m is None:
                        continue
                    if m.get("op") == self.op:
                        # offset anterior al final que había al arrancar = mensaje "pendiente"
                        # (ocurrió mientras el FO estaba cerrado). Sin comparar relojes.
                        self.show(m, late=raw.offset() < backlog_end)
                    commit(consumer, raw)
            except Exception as e:
                self.say(f"Error leyendo notificaciones: {e}. Reintentando...", "red")
                time.sleep(3)
            finally:
                try:
                    consumer and consumer.close()
                except Exception:
                    pass

    def show(self, m, late=False):
        kind, text, req = m.get("kind", "INFO"), m.get("msg", ""), m.get("req_id")
        if kind == "TELEMETRY" and late:
            return                       # la telemetría antigua no interesa
        self.say(("[pendiente] " if late else "") + text, KIND_COL.get(kind))
        if kind in ("AUTH", "DENIED"):
            self.outcome.setdefault(req, kind)
        if kind in ("FINAL", "DENIED"):
            ev = self.done.get(req)
            if ev:
                ev.set()

    def status_loop(self):
        while True:
            consumer = None
            try:
                consumer = self.bus.tail_consumer([T_STATUS])
                while True:
                    m, _ = poll_json(consumer, 1.0)
                    if m and m.get("wss") is not None:
                        self.wss, self.last_snap = m["wss"], time.time()
            except Exception:
                time.sleep(3)
            finally:
                try:
                    consumer and consumer.close()
                except Exception:
                    pass

    def central_alive(self):
        return time.time() - self.last_snap < CENTRAL_STALE

    # ------------------------------------------------------------------ acciones
    def show_stations(self):
        if not self.central_alive():
            self.say("Imposible conectar con WM_Central: no recibo el estado de las estaciones", "red")
            return
        print(color("  ID      UBICACIÓN                       ESTADO", "bold"))
        for w in self.wss:
            st = color(f" {w['label']:<34}", STATE_COL.get(w["state"], "grey"))
            extra = f"  ({w['flow']} L/min, {w['volume']} L)" if w["state"] == "WATERING" else ""
            print(f"  {w['id']:<7} {w['location']:<31} {st}{extra}", flush=True)
        free = [w["id"] for w in self.wss if w["state"] == "AVAILABLE"]
        self.say("Disponibles ahora: " + (", ".join(free) or "ninguna"), "green" if free else "yellow")

    def request(self, ws, duration, wait=True):
        """Envía una petición. Devuelve el req_id (o None si no se pudo enviar)."""
        deadline = time.time() + 4
        while not self.central_alive() and time.time() < deadline:   # primer snapshot tras arrancar
            time.sleep(0.3)
        if not self.central_alive():
            self.say("Imposible conectar con WM_Central: la petición NO se envía", "red")
            return None
        req = new_id("F")
        ev = threading.Event()
        self.done[req] = ev
        ok = self.bus.send_sync(T_REQ, {"t": "REQ", "req_id": req, "op": self.op, "ws": ws,
                                        "src": "FO", "duration": duration}, key=self.op)
        if not ok:
            self.say("Imposible conectar con Kafka: la petición NO se envía", "red")
            return None
        self.say(f"Petición {req} enviada: riego en {ws} durante {duration} s", "cyan")
        if wait:
            if not ev.wait(duration + 45):
                self.say("Sin respuesta de WM_Central (¿caída?). Compruebe el estado de la central.", "red")
        return req

    def run_file(self, path):
        try:
            lines = open(path, encoding="utf-8").read().splitlines()
        except OSError as e:
            self.say(f"No se puede leer el fichero '{path}': {e}", "red")
            return
        jobs = []
        for ln in lines:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            tok = [t for t in re.split(r"[#,;\s]+", ln) if t]
            dur = int(tok[1]) if len(tok) > 1 and tok[1].isdigit() else self.args.duration
            jobs.append((tok[0], dur))
        self.say(f"Fichero '{path}': {len(jobs)} servicios a solicitar", "cyan")
        results = []
        for i, (ws, dur) in enumerate(jobs, 1):
            self.say(f"--- Servicio {i}/{len(jobs)}: {ws} ({dur} s) ---", "bold")
            req = self.request(ws, dur)
            results.append(self.outcome.get(req, "ERROR") if req else "ERROR")
            if i < len(jobs):
                self.say("Esperando 4 s antes del siguiente servicio...", "grey")
                time.sleep(4)
        ok = results.count("AUTH")
        self.say(f"Fichero completado: {ok} riegos realizados, {len(results) - ok} denegados/fallidos", "yellow")

    def menu(self):
        print(color("\n  [1] Ver estaciones   [2] Solicitar riego   [3] Cargar fichero   [0] Salir\n", "cyan"), flush=True)

    def interactive(self):
        self.menu()
        while True:
            try:
                line = sys.stdin.readline()
            except Exception:
                return
            if line == "":
                while True:               # stdin cerrado: seguimos mostrando notificaciones
                    time.sleep(3600)
            p = line.strip().split()
            if not p:
                continue
            c = p[0].lower()
            if c in ("1", "s"):
                self.show_stations()
            elif c in ("2", "r"):
                ws = p[1] if len(p) > 1 else input("  ID de la estación: ").strip()
                if len(p) > 2 and p[2].isdigit():
                    dur = int(p[2])
                else:
                    d = input(f"  Duración en segundos [{self.args.duration}]: ").strip()
                    dur = int(d) if d.isdigit() else self.args.duration
                self.request(ws, dur, wait=False)
            elif c in ("3", "f"):
                path = p[1] if len(p) > 1 else input("  Fichero: ").strip()
                threading.Thread(target=self.run_file, args=(path,), daemon=True).start()
            elif c in ("0", "q"):
                os_exit()
            else:
                self.menu()


def os_exit():
    import os
    print("Hasta luego.")
    os._exit(0)


def main():
    ap = argparse.ArgumentParser(description="WM_FO - aplicación del operario de campo")
    ap.add_argument("--kafka", default=env("KAFKA_BOOTSTRAP", "localhost:9092"), help="host:puerto del broker de Kafka")
    ap.add_argument("--id", default=env("FO_ID"), required=env("FO_ID") is None, help="ID del operario")
    ap.add_argument("--file", default=env("FO_FILE"), help="fichero de servicios a solicitar")
    ap.add_argument("--duration", type=int, default=int(env("FO_DURATION", 30)), help="duración por defecto (s)")
    args = ap.parse_args()
    print(color("=" * 64, "cyan"))
    print(color(f"  WM_FO · Operario {args.id} · SD 26/27", "bold"))
    print(color("=" * 64, "cyan"))
    fo = FO(args)
    fo.connect()
    threading.Thread(target=fo.notif_loop, daemon=True).start()
    threading.Thread(target=fo.status_loop, daemon=True).start()
    try:
        if args.file:
            fo.run_file(args.file)
        fo.interactive()
    except KeyboardInterrupt:
        print("\nHasta luego.")


if __name__ == "__main__":
    main()
