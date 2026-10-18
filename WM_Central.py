import socket
import sqlite3
import os
import threading
from https.server import BaseHTTPRequestHandler, HTTPServer
port = 5050

#Railway
port = int(os.environ.get("PORT", 3000))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Hello World desde Railway y UA_SD!, por parte de Imran y Michelle")

def lanzar_web():
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

# Lanzamos el servidor HTTP en segundo plano para que Railway detecte el puerto activo
threading.Thread(target=lanzar_web, daemon=True).start()


#Creamos la base de datos.

connection = sqlite3.conncect('wmcentral.db', check_same_thread = False)
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
MAX_CONEXIONES = 2
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
                print("[DESCONEXIÓN] La estación {addr} ha solicitado finalizar la conexión")
                break
            
            print(f"[MENSAJE] Mensaje recibido de la estación [{addr}]: {msg}")
            
            if REGISTRO in msg:
                splitted = msg.split('#')
                if len(splitted)>=3:
                    ws_id = partes[1]
                    location = partes[2]
                    cursor.execute("INSERT OT REPLACE INTO estaciones VALUES (?, ?, ?)", (ws_id, "Available" ,location))
                    conn.commit()
                    print(f"[BASE DE DATOS] Estación '{ws_id}' registrada en la ubicación '{location}'")
                    conn.send("[REGISTRADO] Estación {ws_id} registrada correctamente".encode(FORMAT))
                else:
                    conn.send("[ERR_REGISTRO] Formato inválido. Uso: REGISTRO#ID#UBICACION".encode(Format))
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


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

print("[INICIANDO] Servidor WM_Central inicializándose...")

start()




