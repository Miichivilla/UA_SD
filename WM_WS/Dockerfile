# =============================================================
# WM_WS  ·  Watering Station: Monitor (WM_WS_M.py) y Engine (WM_WS_E.py)
# Una misma imagen; se elige el módulo con el comando:
#   docker run ... wm-ws python WM_WS_M.py --id WS-01 --central HOST:PUERTO
#   docker run ... wm-ws python WM_WS_E.py --kafka HOST:PUERTO --monitor wm-ws-01-m:6000
# =============================================================
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    WM_DATA_DIR=/data

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /data

EXPOSE 6000
CMD ["python", "WM_WS_M.py"]
