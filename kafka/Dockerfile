# =============================================================
# Kafka (KRaft, un solo nodo) para Railway
#   - Listener INTERNAL (29092): lo usa WM_Central por la red privada de Railway
#   - Listener EXTERNAL (9092) : lo usan WM_WS_E y WM_FO desde los PCs del laboratorio
#                                (Railway -> Settings -> Networking -> TCP Proxy -> 9092)
# Alternativa documentada: plantilla "apache/kafka" de Railway (ver docs/DESPLIEGUE.md).
# =============================================================
FROM apache/kafka:4.3.1

# Los volúmenes de Railway se montan como root: así el broker puede escribir en ellos.
USER root
COPY --chmod=755 entrypoint.sh /wm-entrypoint.sh
ENTRYPOINT ["/wm-entrypoint.sh"]
