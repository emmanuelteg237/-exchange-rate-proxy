
"""
Schémas et validation des messages Kafka.
"""
from datetime import datetime, timezone


class ExchangeRateMessage:
    """Représente un message de taux de change pour Kafka."""

    @staticmethod
    def build(base_currency: str, target_currency: str, rate: float,
              api_timestamp: int) -> dict:
        """
        Construit un message structuré.

        Args:
            base_currency: La devise de base (ex: USD)
            target_currency: La devise cible (ex: EUR)
            rate: Le taux de change
            api_timestamp: Le timestamp Unix retourné par l'API

        Returns:
            Un dict prêt à être sérialisé en JSON
        """
        # Conversion timestamp Unix → ISO 8601
        api_date = datetime.fromtimestamp(
            api_timestamp, tz=timezone.utc
        ).isoformat()

        return {
            "base_currency": base_currency,
            "target_currency": target_currency,
            "rate": float(rate),
            "pair": f"{base_currency}/{target_currency}",
            "api_timestamp": api_timestamp,
            "api_date": api_date,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "exchangerate-api.com"
        }

    @staticmethod
    def validate(message: dict) -> bool:
        """
        Valide qu'un message contient les champs requis.

        Returns:
            True si valide, False sinon
        """
        required_fields = [
            "base_currency", "target_currency", "rate",
            "pair", "ingested_at", "@timestamp"
        ]
        return all(field in message for field in required_fields)
