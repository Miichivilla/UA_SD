from kafka import KafkaProducer, KafkaConsumer

# Sustituye este valor por tu endpoint real del TCP Proxy de Railway
BOOTSTRAP = "caboose.proxy.rlwy.net:51518"

# Productor: envía un mensaje de prueba al topic
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
producer.send("topic-prueba", b"Hola desde el repositorio UA_SD local")
producer.flush()
print("Mensaje enviado correctamente a Kafka.")

# Consumidor: lee el mensaje enviado
consumer = KafkaConsumer(
    "topic-prueba",
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000
)

for msg in consumer:
    print("Mensaje recibido:", msg.value.decode())