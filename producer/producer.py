
"""
Producer Kafka qui récupère les taux de change depuis exchangerate-api
et les publie sur un topic Kafka.
Architecture:
 API externe → Producer → Kafka topic 'exchange-rates'
Run:
 python producer.py
"""
import json
import logging
import signal
import sys
import time
from typing import Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from tenacity import retry, stop_after_attempt, wait_exponential, \
    retry_if_exception_type

from config import Config
from schemas import ExchangeRateMessage

# ============================================
# Logging
# ============================================
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("producer")

# ============================================
# Métriques (compteurs simples)
# ============================================
class Metrics:
    """Compteurs pour suivre l'activité du producer."""
    api_calls_total = 0
    api_errors_total = 0
    messages_sent_total = 0
    messages_failed_total = 0

    @classmethod
    def display(cls):
        logger.info(
            f"📊 Métriques: "
            f"API calls={cls.api_calls_total}, "
            f"API errors={cls.api_errors_total}, "
            f"Messages sent={cls.messages_sent_total}, "
            f"Messages failed={cls.messages_failed_total}"
        )

# ============================================
# Producer Kafka
# ============================================
class ExchangeRateProducer:
    """Producer qui publie les taux de change sur Kafka."""
    def __init__(self):
        self.kafka_producer: Optional[KafkaProducer] = None
        self.running = True
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Permet d'arrêter proprement avec Ctrl+C."""
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        """Arrêt propre du producer."""
        logger.info("🛑 Signal d'arrêt reçu, fermeture en cours...")
        self.running = False

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(NoBrokersAvailable),
        reraise=True
    )
    def _connect_kafka(self) -> KafkaProducer:
        """Connexion à Kafka avec retry exponentiel."""
        logger.info(f"🔌 Connexion à Kafka : {Config.KAFKA_BOOTSTRAP_SERVERS}")
        producer = KafkaProducer(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Attente ACK de tous les replicas
            retries=3,
            max_in_flight_requests_per_connection=1,  # Garantit l'ordre
            compression_type='gzip',
        )
        logger.info("✅ Connecté à Kafka")
        return producer

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True
    )
    def fetch_rates(self, base_currency: str) -> Optional[dict]:
        """
        Récupère les taux pour une devise de base.

        Returns:
            Le JSON retourné par l'API, ou None en cas d'échec
        """
        url = f"{Config.API_BASE_URL}/{base_currency}"
        logger.debug(f"📡 GET {url}")
        Metrics.api_calls_total += 1

        try:
            response = requests.get(url, timeout=Config.API_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()
            logger.info(
                f"✅ API OK pour {base_currency} "
                f"({len(data.get('rates', {}))} devises)"
            )
            return data
        except requests.exceptions.RequestException as e:
            Metrics.api_errors_total += 1
            logger.error(f"❌ Erreur API pour {base_currency}: {e}")
            raise

    def publish_rates(self, api_data: dict, base_currency: str) -> int:
        """
        Publie chaque taux comme un message Kafka séparé.

        Returns:
            Le nombre de messages publiés avec succès
        """
        if "rates" not in api_data:
            logger.warning(f"⚠️ Pas de 'rates' dans la réponse pour {base_currency}")
            return 0

        rates = api_data["rates"]
        api_timestamp = api_data.get("time_last_updated", int(time.time()))
        success_count = 0

        for target_currency, rate in rates.items():
            # On filtre pour ne garder que les devises qui nous intéressent
            if target_currency not in Config.TARGET_CURRENCIES:
                continue

            # Skip self-conversion (USD/USD = 1)
            if target_currency == base_currency:
                continue

            try:
                message = ExchangeRateMessage.build(
                    base_currency=base_currency,
                    target_currency=target_currency,
                    rate=rate,
                    api_timestamp=api_timestamp
                )

                if not ExchangeRateMessage.validate(message):
                    logger.warning(f"⚠️ Message invalide: {message}")
                    Metrics.messages_failed_total += 1
                    continue

                # Clé = pair de devises (pour partitioning cohérent)
                key = f"{base_currency}-{target_currency}"

                self.kafka_producer.send(
                    Config.KAFKA_TOPIC,
                    key=key,
                    value=message
                )

                success_count += 1
                Metrics.messages_sent_total += 1

            except KafkaError as e:
                logger.error(f"❌ Erreur Kafka pour {target_currency}: {e}")
                Metrics.messages_failed_total += 1

        # Force l'envoi de tous les messages bufferisés
        self.kafka_producer.flush()

        logger.info(
            f"📤 {success_count} taux publiés pour {base_currency} "
            f"sur topic '{Config.KAFKA_TOPIC}'"
        )
        return success_count

    def run(self):
        """Boucle principale du producer."""
        Config.display()
        logger.info("🚀 Démarrage du producer...")

        # Connexion à Kafka
        try:
            self.kafka_producer = self._connect_kafka()
        except Exception as e:
            logger.error(f"❌ Impossible de se connecter à Kafka: {e}")
            sys.exit(1)

        iteration = 0

        while self.running:
            iteration += 1
            logger.info(f"🔄 === Itération #{iteration} ===")

            for base_currency in Config.BASE_CURRENCIES:
                if not self.running:
                    break

                try:
                    api_data = self.fetch_rates(base_currency)
                    if api_data:
                        self.publish_rates(api_data, base_currency)

                except Exception as e:
                    logger.error(
                        f"❌ Erreur sur {base_currency} "
                        f"après tous les retries: {e}"
                    )

            # Affiche les métriques toutes les 5 itérations
            if iteration % 5 == 0:
                Metrics.display()

            # Attente avant la prochaine itération
            if self.running:
                logger.info(f"⏳ Attente {Config.FETCH_INTERVAL_SECONDS}s...")
                for _ in range(Config.FETCH_INTERVAL_SECONDS):
                    if not self.running:
                        break
                    time.sleep(1)

        # Cleanup
        if self.kafka_producer:
            logger.info("🧹 Fermeture du producer Kafka...")
            self.kafka_producer.close()

        Metrics.display()
        logger.info("👋 Producer arrêté proprement")

# ============================================
# Entry point
# ============================================
if __name__ == "__main__":
    producer = ExchangeRateProducer()
    producer.run()
