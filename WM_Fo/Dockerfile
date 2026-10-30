FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
#   python wm_fo.py --kafka HOST:PUERTO --id FO_01 [--file solicitudes.txt]
CMD ["python", "wm_fo.py", "--help"]
