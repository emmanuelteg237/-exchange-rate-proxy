
"""
Consumer Kafka qui lit les taux de change et les indexe dans Elasticsearch.
Architecture:
 Kafka topic 'exchange-rates' → Consumer ES → Elasticsearch index 'exchange-rates'
"""
import json
import logging
import signal
import sys
from typing import Optional

from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError
from kafka import KafkaConsumer
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
logger = logging.getLogger("consumer-es")

# ============================================
# Métriques
# ============================================
class Metrics:
    messages_consumed = 0
    messages_indexed = 0
    indexing_errors = 0

    @classmethod
    def display(cls):
        logger.info(
            f"📊 Métriques: "
            f"Messages consommés={cls.messages_consumed}, "
            f"Indexés={cls.messages_indexed}, "
            f"Erreurs={cls.indexing_errors}"
        )

# ============================================
# Mapping Elasticsearch
# ============================================
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "ingested_at": {"type": "date"},
            "api_date": {"type": "date"},
            "api_timestamp": {"type": "long"},
            "base_currency": {"type": "keyword"},
            "target_currency": {"type": "keyword"},
            "pair": {"type": "keyword"},
            "rate": {"type": "double"},
            "source": {"type": "keyword"}
        }
    }
}

class ElasticsearchConsumer:
    """Consumer qui lit Kafka et indexe dans Elasticsearch."""

    def __init__(self):
        self.kafka_consumer: Optional[KafkaConsumer] = None
        self.es_client: Optional[Elasticsearch] = None
        self.running = True
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info("🛑 Signal d'arrêt reçu...")
        self.running = False

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(ESConnectionError),
        reraise=True
    )
    def _connect_elasticsearch(self) -> Elasticsearch:
        """Connexion à Elasticsearch avec retry."""
        logger.info(f"🔌 Connexion à Elasticsearch : {Config.ELASTICSEARCH_HOST}")
        client = Elasticsearch(
            Config.ELASTICSEARCH_HOST,
            request_timeout=10,
            max_retries=3,
            retry_on_timeout=True
        )
        if not client.ping():
            raise ESConnectionError("Elasticsearch ne répond pas")
        logger.info("✅ Connecté à Elasticsearch")
        return client

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(NoBrokersAvailable),
        reraise=True
    )
    def _connect_kafka(self) -> KafkaConsumer:
        """Connexion à Kafka avec retry."""
        logger.info(f"🔌 Connexion à Kafka : {Config.KAFKA_BOOTSTRAP_SERVERS}")
        consumer = KafkaConsumer(
            Config.KAFKA_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            group_id="elasticsearch-indexer-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
        )
        logger.info("✅ Connecté à Kafka")
        return consumer

    def _ensure_index_exists(self):
        """Crée l'index Elasticsearch s'il n'existe pas."""
        if not self.es_client.indices.exists(index=Config.ELASTICSEARCH_INDEX):
            logger.info(f"📁 Création de l'index '{Config.ELASTICSEARCH_INDEX}'")
            self.es_client.indices.create(
                index=Config.ELASTICSEARCH_INDEX,
                body=INDEX_MAPPING
            )
            logger.info("✅ Index créé")
        else:
            logger.info(f"✅ Index '{Config.ELASTICSEARCH_INDEX}' existe déjà")

    def _index_message(self, message: dict) -> bool:
        """Indexe un message dans Elasticsearch."""
        try:
            self.es_client.index(
                index=Config.ELASTICSEARCH_INDEX,
                document=message
            )
            Metrics.messages_indexed += 1
            return True
        except Exception as e:
            logger.error(f"❌ Erreur indexation: {e}")
            Metrics.indexing_errors += 1
            return False

    def run(self):
        """Boucle principale."""
        logger.info("🚀 Démarrage du consumer Elasticsearch...")

        # Connexions
        try:
            self.es_client = self._connect_elasticsearch()
            self._ensure_index_exists()
            self.kafka_consumer = self._connect_kafka()
        except Exception as e:
            logger.error(f"❌ Erreur de connexion: {e}")
            sys.exit(1)

        logger.info(
            f"👂 Écoute du topic '{Config.KAFKA_TOPIC}'... "
            f"(Ctrl+C pour arrêter)"
        )

        try:
            for record in self.kafka_consumer:
                if not self.running:
                    break
                Metrics.messages_consumed += 1
                message = record.value
                logger.info(
                    f"📥 Reçu: {message.get('pair')} = {message.get('rate'):.4f}"
                )
                self._index_message(message)

                if Metrics.messages_consumed % 50 == 0:
                    Metrics.display()

        except KeyboardInterrupt:
            logger.info("🛑 Interruption clavier")
        except Exception as e:
            logger.error(f"❌ Erreur dans la boucle de consommation: {e}")
        finally:
            if self.kafka_consumer:
                self.kafka_consumer.close()
            Metrics.display()
            logger.info("👋 Consumer arrêté proprement")

if __name__ == "__main__":
    consumer = ElasticsearchConsumer()
    consumer.run()
