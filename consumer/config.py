
"""
Configuration centralisée des consumers.
"""
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration commune aux consumers."""

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
    )
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "exchange-rates")
    KAFKA_TOPIC_INPUT = os.getenv("KAFKA_TOPIC_INPUT", "exchange-rates")
    KAFKA_TOPIC_OUTPUT = os.getenv("KAFKA_TOPIC_OUTPUT", "exchange-alerts")

    # Elasticsearch
    ELASTICSEARCH_HOST = os.getenv(
        "ELASTICSEARCH_HOST", "http://localhost:9200"
    )
    ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "exchange-rates")
    ELASTICSEARCH_INDEX_ALERTS = os.getenv(
        "ELASTICSEARCH_INDEX_ALERTS", "exchange-alerts"
    )

    # Alert detection
    ALERT_THRESHOLD_PERCENT = float(
        os.getenv("ALERT_THRESHOLD_PERCENT", "2.0")
    )

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
