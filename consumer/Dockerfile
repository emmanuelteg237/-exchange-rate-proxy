
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Le command est défini dans docker-compose.yml
# (consumer_es.py ou consumer_alert.py)
