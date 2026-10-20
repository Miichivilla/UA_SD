import os
from http.server import BaseHTTPRequestHandler, HTTPServer

# Obtiene el puerto dinámico asignado por Railway mediante la variable PORT
port = int(os.environ.get("PORT", 3000))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Hello World desde Railway y UA_SD!, por parte de Imran y Michelle")

print(f"Escuchando en el puerto {port}")
HTTPServer(("0.0.0.0", port), Handler).serve_forever()