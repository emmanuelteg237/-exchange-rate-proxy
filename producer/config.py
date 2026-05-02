
"""
Configuration centralisée du producer.
Toutes les variables d'environnement sont lues ici.
"""
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration du producer."""

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
    )
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "exchange-rates")

    # API externe
    API_BASE_URL = os.getenv(
        "API_BASE_URL", "https://api.exchangerate-api.com/v4/latest"
    )
    API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "10"))

    # Devises de base à monitorer
    BASE_CURRENCIES = os.getenv(
        "BASE_CURRENCIES", "USD,EUR,GBP,JPY,CHF"
    ).split(",")

    # Devises cibles (les taux qu'on garde dans chaque message)
    TARGET_CURRENCIES = os.getenv(
        "TARGET_CURRENCIES",
        "USD,EUR,GBP,JPY,CHF,CAD,AUD,CNY,INR,BRL"
    ).split(",")

    # Fréquence des appels API
    FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "30"))

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def display(cls):
        """Affiche la config au démarrage."""
        print("=" * 60)
        print("CONFIGURATION DU PRODUCER")
        print("=" * 60)
        print(f"Kafka servers : {cls.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Kafka topic : {cls.KAFKA_TOPIC}")
        print(f"API URL : {cls.API_BASE_URL}")
        print(f"Base currencies : {cls.BASE_CURRENCIES}")
        print(f"Target currencies : {cls.TARGET_CURRENCIES}")
        print(f"Fetch interval : {cls.FETCH_INTERVAL_SECONDS}s")
        print("=" * 60)
