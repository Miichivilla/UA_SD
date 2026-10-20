# =============================================================
# WM_FO  ·  Field Operator (aplicación del operario)
#   docker run -it --rm wm-fo python WM_FO.py --kafka HOST:PUERTO --id OP-01
# =============================================================
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "WM_FO.py"]
