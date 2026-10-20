# =============================================================
# WM_Central  ·  servidor de sockets (monitores) + Kafka + SQLite + panel web
# Despliegue en Railway: "Deploy from GitHub repo" con Root Directory = WM_Central
# =============================================================
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    WM_PORT=5050 \
    WM_DB=/data/wm_central.db

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /data

# 5050 = sockets de los monitores (en Railway: TCP Proxy)
# 8080 = panel web (en Railway se usa la variable PORT que inyecta la plataforma)
EXPOSE 5050 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=3 \
  CMD python -c "import os,urllib.request;urllib.request.urlopen('http://127.0.0.1:%s/health' % os.environ.get('PORT','8080'),timeout=2)" || exit 1

# Parámetros por variables de entorno (o argumentos):
#   KAFKA_BOOTSTRAP  host:puerto del broker       WM_PORT  puerto de sockets
#   PORT             puerto del panel web         WM_DB    fichero SQLite (montar volumen en /data)
#   PANEL_USER / PANEL_PASS   protegen el panel web
CMD ["python", "WM_Central.py"]
