"""Panel web de monitorización y control de WM_Central (sólo librería estándar)."""
import base64
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from wm_common import env, log

HERE = os.path.dirname(os.path.abspath(__file__))
USER = env("PANEL_USER")
PASSWORD = env("PANEL_PASS")


def start_web(central, port):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):  # silencia el log por petición
            pass

        # -- utilidades -------------------------------------------------------
        def _authorized(self):
            if not (USER and PASSWORD):
                return True
            h = self.headers.get("Authorization", "")
            if h.startswith("Basic "):
                try:
                    u, p = base64.b64decode(h[6:]).decode().split(":", 1)
                    return u == USER and p == PASSWORD
                except Exception:
                    return False
            return False

        def _send(self, code, body, ctype="application/json"):
            data = body if isinstance(body, bytes) else body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype + "; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _deny(self):
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="WM_Central"')
            self.send_header("Content-Length", "0")
            self.end_headers()

        # -- rutas ------------------------------------------------------------
        def do_GET(self):
            if self.path == "/health":          # sin auth: healthcheck de Railway
                return self._send(200, json.dumps({"status": "ok"}))
            if not self._authorized():
                return self._deny()
            if self.path in ("/", "/index.html"):
                with open(os.path.join(HERE, "panel.html"), "rb") as f:
                    return self._send(200, f.read(), "text/html")
            if self.path == "/api/state":
                rows = central.db.recent_sessions(12)
                return self._send(200, json.dumps({
                    "now": time.strftime("%H:%M:%S"),
                    "uptime": int(time.time() - central.started),
                    "kafka": central.kafka_ok,
                    "kafka_addr": central.args.kafka,
                    "socket_port": central.args.port,
                    "wss": central.snapshot(),
                    "events": list(central.events)[-60:][::-1],
                    "operators": central.db.operators(),
                    "history": rows,
                }))
            self._send(404, json.dumps({"error": "no encontrado"}))

        def do_POST(self):
            if not self._authorized():
                return self._deny()
            try:
                n = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                return self._send(400, json.dumps({"error": "JSON no válido"}))
            try:
                if self.path == "/api/cmd":
                    cmd, target = body.get("cmd"), str(body.get("ws", "ALL"))
                    if cmd == "start":
                        central.cmd_start(target, int(body.get("duration", 30)))
                    elif cmd == "block":
                        central.cmd_block(target)
                    elif cmd == "activate":
                        central.cmd_activate(target)
                    else:
                        raise ValueError("orden desconocida")
                elif self.path == "/api/ws":
                    central.add_ws(str(body.get("id", "")).strip(), str(body.get("location", "")).strip())
                elif self.path == "/api/operator":
                    central.add_operator(str(body.get("id", "")).strip(), str(body.get("name", "")).strip())
                else:
                    return self._send(404, json.dumps({"error": "no encontrado"}))
                self._send(200, json.dumps({"ok": True}))
            except Exception as e:
                self._send(400, json.dumps({"error": str(e)}))

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    srv.daemon_threads = True
    log("CENTRAL", f"Panel web en http://0.0.0.0:{port}/"
        + ("  (protegido con usuario/contraseña)" if USER and PASSWORD else "  (SIN contraseña: define PANEL_USER y PANEL_PASS)"),
        "green")
    srv.serve_forever()
