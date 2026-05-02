
"""
Tests unitaires pour le producer.
"""
import pytest
from schemas import ExchangeRateMessage


class TestExchangeRateMessage:
    """Tests pour la construction et validation des messages."""

    def test_build_creates_valid_message(self):
        """Un message correctement construit doit avoir tous les champs."""
        message = ExchangeRateMessage.build(
            base_currency="USD",
            target_currency="EUR",
            rate=0.92,
            api_timestamp=1730476800
        )
        assert message["base_currency"] == "USD"
        assert message["target_currency"] == "EUR"
        assert message["rate"] == 0.92
        assert message["pair"] == "USD/EUR"
        assert message["api_timestamp"] == 1730476800
        assert "ingested_at" in message
        assert "@timestamp" in message
        assert message["source"] == "exchangerate-api.com"

    def test_build_converts_rate_to_float(self):
        """Le rate doit être un float même si on passe un int."""
        message = ExchangeRateMessage.build(
            base_currency="USD",
            target_currency="JPY",
            rate=153,  # int
            api_timestamp=1730476800
        )
        assert isinstance(message["rate"], float)
        assert message["rate"] == 153.0

    def test_validate_returns_true_for_valid_message(self, sample_message):
        """Un message valide doit passer la validation."""
        assert ExchangeRateMessage.validate(sample_message) is True

    def test_validate_returns_false_when_field_missing(self, sample_message):
        """Un message sans champ requis doit échouer."""
        del sample_message["rate"]
        assert ExchangeRateMessage.validate(sample_message) is False

    def test_validate_returns_false_for_empty_dict(self):
        """Un dict vide doit échouer la validation."""
        assert ExchangeRateMessage.validate({}) is False

    def test_pair_format(self):
        """Le format de la paire doit être BASE/TARGET."""
        message = ExchangeRateMessage.build(
            base_currency="GBP",
            target_currency="JPY",
            rate=192.5,
            api_timestamp=1730476800
        )
        assert message["pair"] == "GBP/JPY"

    def test_api_date_is_iso_format(self):
        """api_date doit être au format ISO 8601."""
        message = ExchangeRateMessage.build(
            base_currency="USD",
            target_currency="EUR",
            rate=0.92,
            api_timestamp=1730476800
        )
        # ISO 8601 contient 'T' et un fuseau horaire
        assert "T" in message["api_date"]
        assert "+" in message["api_date"] or "Z" in message["api_date"]
