import sys
import socket
import threading
import time

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"

def sendMessageToSocket(socket, message):
    #empaquetamos el mensaje
    msg = message.encode(FORMAT)
    send_length = str(len(msg))
    send_length += b' ' * (HEADER - len(send_length))
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
                print(f"[ALERTA] El motor ha reportado un fallo (KO). Avisando a central...)
                enviarMensajeSocket(socket_central,f"LEAK#{ws_id}#Fuga detectada por Motor")
                break
            
        except socket.timeout: