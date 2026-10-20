#!/bin/sh
# Compone la configuración de Kafka a partir de variables de entorno y arranca el broker.
#
# Variables a definir en Railway (servicio Kafka):
#   KAFKA_PUBLIC_HOST   host del TCP Proxy        (p. ej. xxxxx.proxy.rlwy.net)
#   KAFKA_PUBLIC_PORT   puerto público del proxy  (p. ej. 19097)
# Opcionales:
#   KAFKA_INTERNAL_HOST (por defecto RAILWAY_PRIVATE_DOMAIN, o "broker" en local)
#   KAFKA_HEAP_OPTS     (por defecto -Xms256m -Xmx512m)
set -eu

INTERNAL_HOST="${KAFKA_INTERNAL_HOST:-${RAILWAY_PRIVATE_DOMAIN:-broker}}"
PUBLIC_HOST="${KAFKA_PUBLIC_HOST:-localhost}"
PUBLIC_PORT="${KAFKA_PUBLIC_PORT:-9092}"

export CLUSTER_ID="${CLUSTER_ID:-gTBb85_WRXu6oQgq-brTQw}"
export KAFKA_NODE_ID=1
export KAFKA_PROCESS_ROLES=broker,controller
export KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:29093
export KAFKA_LISTENERS=INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:29093
export KAFKA_ADVERTISED_LISTENERS="INTERNAL://${INTERNAL_HOST}:29092,EXTERNAL://${PUBLIC_HOST}:${PUBLIC_PORT}"
export KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
export KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
export KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
export KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
export KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
export KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
export KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
# Datos en el volumen de Railway (Mount Path = /var/lib/kafka/data): sobreviven a los redespliegues
export KAFKA_LOG_DIRS="${KAFKA_LOG_DIRS:-/var/lib/kafka/data}"
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}"

echo "[wm-kafka] interno : ${INTERNAL_HOST}:29092"
echo "[wm-kafka] externo : ${PUBLIC_HOST}:${PUBLIC_PORT}"

if [ -x /__cacert_entrypoint.sh ]; then
  exec /__cacert_entrypoint.sh /etc/kafka/docker/run
fi
exec /etc/kafka/docker/run
