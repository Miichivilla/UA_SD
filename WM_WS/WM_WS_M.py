import argparse
import os
import socket
import threading
import time

from wm_common import (ConnectionClosed, FramedConn, ProtocolError, color, connect, env, log, parse_hostport,
                       split_msg)


class Monitor:
    def __init__(self, args):
        self.args = args
        self.central_host, self.central_port = parse_hostport(args.central)
        self.ws_id = args.id
        self.location = args.location or ""
        self.log_tag = f"{self.ws_id} MONITOR"
        self.lock = threading.Lock()
        self.authenticated = False       # Central ha validado esta WS al menos una vez
        self.denied_reason = None        # motivo si Central la denegó
        self.central_connected = False
        self.blocked = None              # lo que dice Central (None = desconocido)
        self.engine_connection = None    # conexión del engine activa
        self.engine_status = "KO"        # OK / KO
        self.engine_msg = "esperando al engine"

    def say(self, msg, col=None):
        log(self.log_tag, msg, col)

    def set_engine_status(self, status, reason=""):
        with self.lock:
            changed = (status, reason) != (self.engine_status, self.engine_msg)
            self.engine_status, self.engine_msg = status, reason
        if changed:
            if status == "OK":
                self.say("Engine OK → WS operativa", "green")
            else:
                self.say(f"*** AVERÍA: {reason}. Se notifica la FUGA a WM_Central ***", "red")

    # ------------------------------------------------------------------ enlace con Central
    def central_connection_loop(self):
        warned = False
        while True:
            central_conn = None
            try:
                central_conn = connect(self.central_host, self.central_port, timeout=5)
                if self.location:
                    response = split_msg(central_conn.request(f"REGISTRO#{self.ws_id}#{self.location}"))
                    self.say(f"Registro en WM_Central: {response[1] if len(response) > 1 else response[0]}",
                             "cyan")
                response = split_msg(central_conn.request(f"AUTH#{self.ws_id}"))
                if response[0] == "DENIED":
                    self.denied_reason = response[1] if len(response) > 1 else "denegada"
                    self.say(f"WM_Central ha DENEGADO la WS: {self.denied_reason}", "red")
                    time.sleep(3)          # deja tiempo a que el engine se entere
                    os._exit(1)
                if response[0] != "OK":
                    raise ProtocolError(f"respuesta inesperada: {response}")
                self.location = response[2] if len(response) > 2 else self.location
                self.blocked = len(response) > 3 and response[3] == "1"
                self.authenticated = True
                self.central_connected = True
                warned = False
                self.say(f"Autenticada en WM_Central ({self.central_host}:{self.central_port}) "
                         f"· ubicación {self.location}", "green")

                # latido cada segundo con el estado de salud
                while True:
                    with self.lock:
                        status, reason = self.engine_status, self.engine_msg
                    response = split_msg(central_conn.request(f"STATUS#{self.ws_id}#{status}#{reason}",
                                                              timeout=5))
                    if response[0] == "ACK" and len(response) > 1:
                        is_blocked = response[1] == "1"
                        if is_blocked != self.blocked:
                            self.say("WM_Central: WS BLOQUEADA (Out of Service)" if is_blocked else
                                     "WM_Central: WS ACTIVADA", "orange" if is_blocked else "green")
                        self.blocked = is_blocked
                    time.sleep(1)
            except (ConnectionClosed, ProtocolError, OSError) as e:
                if self.central_connected or not warned:
                    self.say(f"Imposible conectar con WM_Central ({self.central_host}:{self.central_port}): "
                             f"{e}. Reintentando...", "red")
                    warned = True
                self.central_connected = False
                time.sleep(2)
            finally:
                if central_conn:
                    central_conn.close()

    # ------------------------------------------------------------------ servidor para el Engine
    def engine_socket_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", self.args.port))
        server.listen(5)
        self.say(f"Esperando al engine en el puerto {self.args.port}", "cyan")
        while True:
            connection, address = server.accept()
            threading.Thread(target=self.attend_engine, args=(connection, address), daemon=True).start()

    def attend_engine(self, connection, address):
        engine_conn = FramedConn(connection, address)
        is_active_engine = False
        try:
            # 1) saludo: el engine pide su identidad (cuando Central ya ha validado la WS)
            while True:
                message = split_msg(engine_conn.recv(timeout=15))
                if message[0] == "HELLO":
                    with self.lock:
                        busy = self.engine_connection is not None
                    if self.denied_reason:
                        engine_conn.send(f"DENIED#{self.denied_reason}")
                        return
                    if busy:
                        engine_conn.send("DENIED#ya hay un engine conectado a este monitor")
                        return
                    if not self.authenticated:
                        engine_conn.send("WAIT")
                        continue
                    engine_conn.send(f"ID#{self.ws_id}#{self.location}")
                elif message[0] == "READY":
                    with self.lock:
                        self.engine_connection, is_active_engine = engine_conn, True
                    engine_conn.send("OK")
                    break
            self.say(f"Engine conectado desde {address[0]}:{address[1]}", "green")
            self.set_engine_status("OK")

            # 2) comprobación de salud cada segundo
            while True:
                blocked_flag = "-" if self.blocked is None else ("1" if self.blocked else "0")
                response = engine_conn.request(f"PING#{1 if self.central_connected else 0}#{blocked_flag}",
                                               timeout=3)
                if response == "KO":
                    self.set_engine_status("KO", "el engine reporta KO (fuga/avería)")
                else:
                    self.set_engine_status("OK")
                time.sleep(1)
        except (ConnectionClosed, ProtocolError, OSError) as e:
            if is_active_engine:
                self.set_engine_status("KO", f"el engine no responde ({e.__class__.__name__})")
        finally:
            engine_conn.close()
            if is_active_engine:
                with self.lock:
                    self.engine_connection = None


def main():
    parser = argparse.ArgumentParser(description="WM_WS_M - Monitor de la Watering Station")
    parser.add_argument("--port", type=int, default=int(env("WM_MONITOR_PORT", 6000)),
                        help="puerto para el engine")
    parser.add_argument("--central", default=env("WM_CENTRAL"), required=env("WM_CENTRAL") is None,
                        help="host:puerto de WM_Central (o variable WM_CENTRAL)")
    parser.add_argument("--id", default=env("WS_ID"), required=env("WS_ID") is None, help="ID de la WS")
    parser.add_argument("--location", default=env("WS_LOCATION"), help="ubicación (si se da, registra la WS)")
    args = parser.parse_args()

    print(color("=" * 64, "cyan"))
    print(color(f"  WM_WS_M · Monitor de la WS {args.id} · SD 26/27", "bold"))
    print(color("=" * 64, "cyan"), flush=True)

    monitor = Monitor(args)
    threading.Thread(target=monitor.engine_socket_server, daemon=True).start()
    threading.Thread(target=monitor.central_connection_loop, daemon=True).start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nMonitor detenido.")


if __name__ == "__main__":
    main()