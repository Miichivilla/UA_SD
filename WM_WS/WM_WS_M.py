import argparse
import os
import socket
import threading
import time

from wm_common import (ConnectionClosed, FramedConn, ProtocolError, color, connect, env, log, parse_hostport, split_msg)

class Monitor:
    def __init__(self, args):
        self.args = args
        self.chost, self.cport = parse_hostport(args.central)
        self.ws_id = args.id
        self.location = args.location or ""
        self.tag = f"{self.ws_id} MONITOR"
        self.lock = threading.Lock()
        self.authenticated = False     # Central ha validado esta WS al menos una vez
        self.denied = None             # motivo si Central la denegó
        self.central_up = False
        self.blocked = None            # lo que dice Central (None = desconocido)
        self.engine_conn = None        # conexión del engine activa
        self.engine_state = "KO"       # OK / KO
        self.engine_reason = "esperando al engine"

    def say(self, msg, col=None):
        log(self.tag, msg, col)

    def set_engine(self, state, reason=""):
        with self.lock:
            changed = (state, reason) != (self.engine_state, self.engine_reason)
            self.engine_state, self.engine_reason = state, reason
        if changed:
            if state == "OK":
                self.say("Engine OK → WS operativa", "green")
            else:
                self.say(f"*** AVERÍA: {reason}. Se notifica la FUGA a WM_Central ***", "red")

    # ------------------------------------------------------------------ enlace con Central
    def central_loop(self):
        warned = False
        while True:
            fc = None
            try:
                fc = connect(self.chost, self.cport, timeout=5)
                if self.location:
                    r = split_msg(fc.request(f"REGISTRO#{self.ws_id}#{self.location}"))
                    self.say(f"Registro en WM_Central: {r[1] if len(r) > 1 else r[0]}", "cyan")
                r = split_msg(fc.request(f"AUTH#{self.ws_id}"))
                if r[0] == "DENIED":
                    self.denied = r[1] if len(r) > 1 else "denegada"
                    self.say(f"WM_Central ha DENEGADO la WS: {self.denied}", "red")
                    time.sleep(3)          # deja tiempo a que el engine se entere
                    os._exit(1)
                if r[0] != "OK":
                    raise ProtocolError(f"respuesta inesperada: {r}")
                self.location = r[2] if len(r) > 2 else self.location
                self.blocked = len(r) > 3 and r[3] == "1"
                self.authenticated = True
                self.central_up = True
                warned = False
                self.say(f"Autenticada en WM_Central ({self.chost}:{self.cport}) · ubicación {self.location}", "green")
                # latido cada segundo con el estado de salud
                while True:
                    with self.lock:
                        st, why = self.engine_state, self.engine_reason
                    r = split_msg(fc.request(f"STATUS#{self.ws_id}#{st}#{why}", timeout=5))
                    if r[0] == "ACK" and len(r) > 1:
                        b = r[1] == "1"
                        if b != self.blocked:
                            self.say("WM_Central: WS BLOQUEADA (Out of Service)" if b else
                                     "WM_Central: WS ACTIVADA", "orange" if b else "green")
                        self.blocked = b
                    time.sleep(1)
            except (ConnectionClosed, ProtocolError, OSError) as e:
                if self.central_up or not warned:
                    self.say(f"Imposible conectar con WM_Central ({self.chost}:{self.cport}): {e}. "
                             f"Reintentando...", "red")
                    warned = True
                self.central_up = False
                time.sleep(2)
            finally:
                if fc:
                    fc.close()

    # ------------------------------------------------------------------ servidor para el Engine
    def engine_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.args.port))
        srv.listen(5)
        self.say(f"Esperando al engine en el puerto {self.args.port}", "cyan")
        while True:
            sock, addr = srv.accept()
            threading.Thread(target=self.handle_engine, args=(sock, addr), daemon=True).start()

    def handle_engine(self, sock, addr):
        fc = FramedConn(sock, addr)
        mine = False
        try:
            # 1) saludo: el engine pide su identidad (cuando Central ya ha validado la WS)
            while True:
                m = split_msg(fc.recv(timeout=15))
                if m[0] == "HELLO":
                    with self.lock:
                        busy = self.engine_conn is not None
                    if self.denied:
                        fc.send(f"DENIED#{self.denied}")
                        return
                    if busy:
                        fc.send("DENIED#ya hay un engine conectado a este monitor")
                        return
                    if not self.authenticated:
                        fc.send("WAIT")
                        continue
                    fc.send(f"ID#{self.ws_id}#{self.location}")
                elif m[0] == "READY":
                    with self.lock:
                        self.engine_conn, mine = fc, True
                    fc.send("OK")
                    break
            self.say(f"Engine conectado desde {addr[0]}:{addr[1]}", "green")
            self.set_engine("OK")
            # 2) comprobación de salud cada segundo
            while True:
                b = "-" if self.blocked is None else ("1" if self.blocked else "0")
                r = fc.request(f"PING#{1 if self.central_up else 0}#{b}", timeout=3)
                if r == "KO":
                    self.set_engine("KO", "el engine reporta KO (fuga/avería)")
                else:
                    self.set_engine("OK")
                time.sleep(1)
        except (ConnectionClosed, ProtocolError, OSError) as e:
            if mine:
                self.set_engine("KO", f"el engine no responde ({e.__class__.__name__})")
        finally:
            fc.close()
            if mine:
                with self.lock:
                    self.engine_conn = None

def main():
    ap = argparse.ArgumentParser(description="WM_WS_M - Monitor de la Watering Station")
    ap.add_argument("--port", type=int, default=int(env("WM_MONITOR_PORT", 6000)), help="puerto para el engine")
    ap.add_argument("--central", default=env("WM_CENTRAL"), required=env("WM_CENTRAL") is None,
                    help="host:puerto de WM_Central (o variable WM_CENTRAL)")
    ap.add_argument("--id", default=env("WS_ID"), required=env("WS_ID") is None, help="ID de la WS")
    ap.add_argument("--location", default=env("WS_LOCATION"), help="ubicación (si se da, registra la WS)")
    args = ap.parse_args()
    print(color("=" * 64, "cyan"))
    print(color(f"  WM_WS_M · Monitor de la WS {args.id} · SD 26/27", "bold"))
    print(color("=" * 64, "cyan"), flush=True)
    m = Monitor(args)
    threading.Thread(target=m.engine_server, daemon=True).start()
    threading.Thread(target=m.central_loop, daemon=True).start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nMonitor detenido.")

if __name__ == "__main__":
    main()