FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
# La misma imagen sirve para el Monitor y el Engine:
#   python WM_WS_M.py --port 6000 --central HOST:PUERTO --id Station001 --location "Parque"
#   python WM_WS_E.py --kafka HOST:PUERTO --monitor HOST:6000
CMD ["python", "WM_WS_M.py", "--help"]
