
"""
Tests pour la détection d'alertes.
"""
import pytest
from unittest.mock import patch


@pytest.fixture
def alert_consumer():
    """Crée une instance du consumer alertes pour les tests."""
    # On patch les imports pour éviter les connexions réelles
    with patch('consumer_alert.KafkaConsumer'), \
         patch('consumer_alert.KafkaProducer'), \
         patch('consumer_alert.Elasticsearch'):
        from consumer_alert import AlertConsumer
        consumer = AlertConsumer()
        return consumer


class TestAlertDetection:
    """Tests pour la logique de détection d'alertes."""

    def test_first_message_for_pair_no_alert(self, alert_consumer):
        """Le premier message d'une paire ne doit jamais déclencher d'alerte."""
        message = {
            "pair": "USD/EUR",
            "rate": 0.92,
            "base_currency": "USD",
            "target_currency": "EUR"
        }
        alert = alert_consumer._detect_alert(message)
        assert alert is None
        assert alert_consumer.last_rates["USD/EUR"] == 0.92

    def test_no_alert_for_small_variation(self, alert_consumer):
        """Une variation < seuil ne doit pas déclencher d'alerte."""
        # Premier taux
        alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 0.92,
            "base_currency": "USD",
            "target_currency": "EUR"
        })

        # Variation de 0.5% (sous le seuil de 2%)
        alert = alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 0.9246,
            "base_currency": "USD",
            "target_currency": "EUR"
        })
        assert alert is None

    def test_alert_triggered_on_large_variation(self, alert_consumer):
        """Une variation > seuil doit déclencher une alerte."""
        # Premier taux
        alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 0.92,
            "base_currency": "USD",
            "target_currency": "EUR"
        })

        # Variation de +5%
        alert = alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 0.966,
            "base_currency": "USD",
            "target_currency": "EUR"
        })
        assert alert is not None
        assert alert["pair"] == "USD/EUR"
        assert alert["alert_type"] == "rate_variation"
        assert alert["direction"] == "up"
        assert alert["variation_percent"] == pytest.approx(5.0, abs=0.01)

    def test_alert_high_severity_for_huge_variation(self, alert_consumer):
        """Variation >= 5% doit avoir severity 'high'."""
        alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 1.0,
            "base_currency": "USD",
            "target_currency": "EUR"
        })
        alert = alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 1.1,  # +10%
            "base_currency": "USD",
            "target_currency": "EUR"
        })
        assert alert["severity"] == "high"

    def test_alert_medium_severity_for_moderate_variation(self, alert_consumer):
        """Variation entre seuil et 5% doit avoir severity 'medium'."""
        alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 1.0,
            "base_currency": "USD",
            "target_currency": "EUR"
        })
        alert = alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 1.03,  # +3%
            "base_currency": "USD",
            "target_currency": "EUR"
        })
        assert alert["severity"] == "medium"

    def test_negative_variation_direction_down(self, alert_consumer):
        """Une baisse doit donner direction='down'."""
        alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 1.0,
            "base_currency": "USD",
            "target_currency": "EUR"
        })
        alert = alert_consumer._detect_alert({
            "pair": "USD/EUR",
            "rate": 0.95,  # -5%
            "base_currency": "USD",
            "target_currency": "EUR"
        })
        assert alert["direction"] == "down"

    def test_missing_fields_returns_none(self, alert_consumer):
        """Un message sans rate ou pair doit retourner None."""
        assert alert_consumer._detect_alert({"pair": "USD/EUR"}) is None
        assert alert_consumer._detect_alert({"rate": 0.92}) is None
        assert alert_consumer._detect_alert({}) is None
