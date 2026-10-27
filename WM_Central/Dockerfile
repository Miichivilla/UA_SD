FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Puerto en el que la Central escuchará conexiones TCP Sockets
EXPOSE 5000 3000

CMD ["python", "WM_Central.py"]