import socket 
import threading
import os

HEADER = 64
PORT = 5050
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 10

estaciones = {}
db_lock = threading.Lock()
VERDE = "\033[42m\033[30m"
RESET = "\033[0m"

def actualizar_pantalla():

    os.system('cls' if os.name == 'nt' else 'clear')
    print("=== MONITORIZACION WS_CENTRAL ===")
    print("-" * 55)
    with db_lock:
        for ws_id, info in estaciones.items():
            if info["estado"] == "AVAILABLE":
                print(f"{VERDE} ID: {ws_id:<6} | Loc: {info['ubicacion']:<20} | Estado: {info['estado']} {RESET}")
    print("-" * 55)

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
				break
				
			try:
				data = json.loads(msg)
				
				# Si una estacion se conecta
				if data.get("origen") == "WS" and data.get("accion") == "REGISTRAR":
					ws_id = data["id"]
					with db_lock:
						estaciones[ws_id] = {
						"ubicacion": data["ubicacion"],
						"estado": "AVALIABLE",
						"conn": conn
						}
					actualizar_pantalla()
					conn.send("REGISTRO_OK".encode(FORMAT))
			except json.JSONDecodeError:
				printf(f"Error: mensaje invalido por parte de {addr}")
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    
        

def start():
    server.listen()
    print(f"[LISTENING] WM_Central a la escucha en {SERVER}")
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
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1
        

######################### MAIN ##########################


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

print("[STARTING] WM_Central inicializándose...")

start()

