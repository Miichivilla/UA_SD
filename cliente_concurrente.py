import socket
import sys

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"

def send(msg, clientSocket):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    clientSocket.send(send_length)
    clientSocket.send(message)
    
########## MAIN ##########


print("****** WATERING STATION  ****")

if  (len(sys.argv) == 5):
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    WS_ID = sys.argv[3]
    LOCATION = sys.argv[4]
    ADDR = (SERVER, PORT)
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

    msg=sys.argv[3]
    while msg != FIN :
        print("Envio a WM_Central: ", msg)
        send(msg)
        print("Recibo de WM_Central: ", client.recv(2048).decode(FORMAT))
        msg=input()

    print ("SE ACABO LO QUE SE DABA")
    print("Envio a WM_Central: ", FIN)
    send(FIN)
    client.close()
else:
    print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Texto Bienvenida>")
