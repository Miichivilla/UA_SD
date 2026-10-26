import sys
import socket
import threading
import time

HOST = "junction.proxy.rlwy.net"
HEADER = 64
PORT = 59727
FORMAT = 'utf-8'
FIN = "FIN"
WS_ID = "Station001"
LOCATION = "MainOffice"


def sendMessageToSocket(socket, message):
    #empaquetamos el mensaje
    msg = message.encode(FORMAT)
    send_length = str(len(msg))
    send_length = send_length.encode(FORMAT) + b' ' * (HEADER - len(send_length))
    socket.send(send_length)
    socket.send(msg)
    
def attendEngine(connection, address, socket_central, ws_id):
    print(f"MONITOR: Engine conectado desde {address}")
    
    while True:
        try:
            connection.settimeout(2.0)
            msg = connection.recv(1024).decode(FORMAT)
            
            if not msg:
                break
            
            if msg == "KO":
                print(f"[ALERTA] El motor ha reportado un fallo (KO). Avisando a central...")
                sendMessageToSocket(socket_central,f"LEAK#{ws_id}#Fuga detectada por Motor")
                break
            
        except socket.timeout:
            pass
        
        
        
if __name__ == "__main__":
    try:
        print(f"[CONECTANDO] Conectando con la central en {HOST}:{PORT}...")
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        
        print("[CONECTADO] Conexión establecida con éxito con la central.")
        
        #registro inicial a la central
        msg_reg = f"REGISTRO#{WS_ID}#{LOCATION}"
        sendMessageToSocket(sock, msg_reg)
        
        respuesta = sock.recv(2048).decode(FORMAT)
        print(f"[RESPUESTA CENTRAL]: {respuesta}")
        
        sendMessageToSocket(sock, FIN)
        sock.close()
        print("[DESCONEXIÓN] Conexión finalizada correctamente.")

    except Exception as e:
        print(f"[ERROR DE CONEXIÓN] No se pudo conectar a la central: {e}")