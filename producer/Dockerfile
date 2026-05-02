
FROM python:3.11-slim

WORKDIR /app

# Optimisation : cache des dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code
COPY . .

# Healthcheck : test TCP sur Kafka 9092 (Kafka n'est pas HTTP)
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD python -c "import socket; s=socket.create_connection(('kafka', 9092), timeout=5); s.close()" || exit 1

CMD ["python", "producer.py"]
