import time
import json
from kafka import KafkaProducer
from kafka.serializer import Serializer

# Configuración del Broker Kafka
# Cambia por la URL/puerto de Railway o localhost:9092
KAFKA_BROKER = 'caboose.proxy.rlwy.net:51518' 
TOPIC_PETICIONES = 'peticiones_riego'

def crear_productor():
    """Crea y devuelve la instancia del productor de Kafka."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000  # Tiempo de espera máximo de conexión
        )
        print(f"[WM_FO] Conectado a Kafka Broker en: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"[ERROR] No se pudo conectar con Kafka: {e}")
        return None

def enviar_solicitud_riego(producer, id_operador, id_estacion, duracion):
    """Publica un mensaje de petición de riego en el tópico de Kafka."""
    mensaje = {
        "operador_id": id_operador,
        "estacion_id": id_estacion,
        "duracion_minutos": int(duracion),
        "timestamp": time.time()
    }
    
    # Envío del mensaje al tópico
    producer.send(TOPIC_PETICIONES, value=mensaje)
    producer.flush() # Asegura el envío inmediato
    
    print(f"[WM_FO] Solicitud enviada a Kafka -> Tópico '{TOPIC_PETICIONES}': {mensaje}")

def procesar_fichero_solicitudes(ruta_archivo, id_operador="FO_01"):
    """Lee el fichero y envía solicitudes con una pausa de 4 segundos."""
    producer = crear_productor()
    if not producer:
        return

    print(f"[WM_FO] Procesando archivo de peticiones: {ruta_archivo}...")
    
    try:
        with open(ruta_archivo, 'r') as f:
            for linea in f:
                linea = linea.strip()
                if not linea or linea.startswith("#"):
                    continue

                partes = linea.split(",")
                if len(partes) == 2:
                    id_estacion = partes[0].strip()
                    duracion = partes[1].strip()

                    # Enviar la solicitud mediante Kafka
                    enviar_solicitud_riego(producer, id_operador, id_estacion, duracion)

                    # Requisito obligatorio: Pausa de 4 segundos entre solicitudes
                    print("[WM_FO] Esperando 4 segundos...")
                    time.sleep(4)

    except FileNotFoundError:
        print(f"[ERROR] El archivo {ruta_archivo} no existe.")
    finally:
        producer.close()

if __name__ == "__main__":
    # Ejecución de prueba
    procesar_fichero_solicitudes("solicitudes.txt", id_operador="FO_01")