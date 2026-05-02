
"""
Consumer Kafka qui détecte les variations anormales des taux de change
et publie des alertes sur un topic dédié.
Architecture:
 Kafka 'exchange-rates' → Consumer Alert → Kafka 'exchange-alerts'
 → Elasticsearch (pour visu Kibana)
"""
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

from elasticsearch import Elasticsearch
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from tenacity import retry, stop_after_attempt, wait_exponential, \
    retry_if_exception_type

from config import Config

# ============================================
# Logging
# ============================================
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("consumer-alert")

# ============================================
# Métriques
# ============================================
class Metrics:
    messages_processed = 0
    alerts_triggered = 0

    @classmethod
    def display(cls):
        logger.info(
            f"📊 Métriques: "
            f"Messages traités={cls.messages_processed}, "
            f"Alertes déclenchées={cls.alerts_triggered}"
        )

# ============================================
# Mapping Elasticsearch pour les alertes
# ============================================
ALERT_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "alert_type": {"type": "keyword"},
            "severity": {"type": "keyword"},
            "pair": {"type": "keyword"},
            "base_currency": {"type": "keyword"},
            "target_currency": {"type": "keyword"},
            "previous_rate": {"type": "double"},
            "current_rate": {"type": "double"},
            "variation_percent": {"type": "double"},
            "direction": {"type": "keyword"},
            "message": {"type": "text"}
        }
    }
}

class AlertConsumer:
    """Consumer qui détecte les variations anormales."""

    def __init__(self):
        self.kafka_consumer: Optional[KafkaConsumer] = None
        self.kafka_producer: Optional[KafkaProducer] = None
        self.es_client: Optional[Elasticsearch] = None
        self.running = True

        # Cache des derniers taux par paire
        self.last_rates = {}  # {"USD/EUR": 0.92, ...}

        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info("🛑 Signal d'arrêt reçu...")
        self.running = False

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(NoBrokersAvailable),
        reraise=True
    )
    def _connect_kafka_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            Config.KAFKA_TOPIC_INPUT,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id="alert-detector-group",
            auto_offset_reset="latest",  # On veut juste les nouveaux taux
            enable_auto_commit=True,
        )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(NoBrokersAvailable),
        reraise=True
    )
    def _connect_kafka_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
        )

    def _connect_elasticsearch(self) -> Elasticsearch:
        client = Elasticsearch(Config.ELASTICSEARCH_HOST, request_timeout=10)

        # Crée l'index alertes
        if not client.indices.exists(index=Config.ELASTICSEARCH_INDEX_ALERTS):
            logger.info(
                f"📁 Création de l'index alertes "
                f"'{Config.ELASTICSEARCH_INDEX_ALERTS}'"
            )
            client.indices.create(
                index=Config.ELASTICSEARCH_INDEX_ALERTS,
                body=ALERT_INDEX_MAPPING
            )
        return client

    def _detect_alert(self, message: dict) -> Optional[dict]:
        """
        Détecte si une variation est anormale.
        Returns:
            Le dict de l'alerte si détectée, None sinon
        """
        pair = message.get("pair")
        current_rate = message.get("rate")

        if not pair or current_rate is None:
            return None

        # Premier taux pour cette paire : on enregistre, pas d'alerte
        if pair not in self.last_rates:
            self.last_rates[pair] = current_rate
            return None

        previous_rate = self.last_rates[pair]
        if previous_rate == 0:
            return None

        # Calcul de la variation en %
        variation = ((current_rate - previous_rate) / previous_rate) * 100

        # Mise à jour du cache (toujours)
        self.last_rates[pair] = current_rate

        # Si variation supérieure au seuil → alerte
        if abs(variation) >= Config.ALERT_THRESHOLD_PERCENT:
            severity = "high" if abs(variation) >= 5.0 else "medium"
            direction = "up" if variation > 0 else "down"

            alert = {
                "@timestamp": datetime.now(timezone.utc).isoformat(),
                "alert_type": "rate_variation",
                "severity": severity,
                "pair": pair,
                "base_currency": message.get("base_currency"),
                "target_currency": message.get("target_currency"),
                "previous_rate": previous_rate,
                "current_rate": current_rate,
                "variation_percent": round(variation, 4),
                "direction": direction,
                "message": (
                    f"⚠️ Variation anormale détectée pour {pair}: "
                    f"{previous_rate:.4f} → {current_rate:.4f} "
                    f"({variation:+.2f}%)"
                )
            }
            return alert

        return None

    def _publish_alert(self, alert: dict):
        """Publie l'alerte sur Kafka ET dans Elasticsearch."""

        # 1. Publish to Kafka topic
        try:
            self.kafka_producer.send(
                Config.KAFKA_TOPIC_OUTPUT,
                value=alert
            )
            self.kafka_producer.flush()
        except Exception as e:
            logger.error(f"❌ Erreur publication Kafka: {e}")

        # 2. Index in Elasticsearch
        try:
            self.es_client.index(
                index=Config.ELASTICSEARCH_INDEX_ALERTS,
                document=alert
            )
        except Exception as e:
            logger.error(f"❌ Erreur indexation alerte ES: {e}")

    def run(self):
        """Boucle principale."""
        logger.info("🚀 Démarrage du détecteur d'alertes...")
        logger.info(f"⚙️ Seuil d'alerte : {Config.ALERT_THRESHOLD_PERCENT}%")

        try:
            self.kafka_consumer = self._connect_kafka_consumer()
            self.kafka_producer = self._connect_kafka_producer()
            self.es_client = self._connect_elasticsearch()
        except Exception as e:
            logger.error(f"❌ Erreur de connexion: {e}")
            sys.exit(1)

        logger.info(f"👂 Écoute du topic '{Config.KAFKA_TOPIC_INPUT}'...")

        try:
            for record in self.kafka_consumer:
                if not self.running:
                    break

                Metrics.messages_processed += 1
                message = record.value

                # Détection
                alert = self._detect_alert(message)
                if alert:
                    Metrics.alerts_triggered += 1
                    logger.warning(alert["message"])
                    self._publish_alert(alert)

                if Metrics.messages_processed % 100 == 0:
                    Metrics.display()

        except KeyboardInterrupt:
            logger.info("🛑 Interruption clavier")
        finally:
            if self.kafka_consumer:
                self.kafka_consumer.close()
            if self.kafka_producer:
                self.kafka_producer.close()

            Metrics.display()
            logger.info("👋 Consumer arrêté proprement")

if __name__ == "__main__":
    consumer = AlertConsumer()
    consumer.run()
