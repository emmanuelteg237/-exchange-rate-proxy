
"""
Fixtures pytest partagées entre les tests.
"""
import sys
from pathlib import Path

# Ajoute les modules producer et consumer au PYTHONPATH
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "producer"))
sys.path.insert(0, str(ROOT / "consumer"))

import pytest

@pytest.fixture
def sample_api_response():
    """Réponse type de l'API exchangerate-api."""
    return {
        "provider": "https://www.exchangerate-api.com",
        "base": "USD",
        "date": "2024-11-01",
        "time_last_updated": 1730476800,
        "rates": {
            "USD": 1.0,
            "EUR": 0.92,
            "GBP": 0.78,
            "JPY": 153.45,
            "CHF": 0.88
        }
    }

@pytest.fixture
def sample_message():
    """Message Kafka type."""
    return {
        "base_currency": "USD",
        "target_currency": "EUR",
        "rate": 0.92,
        "pair": "USD/EUR",
        "api_timestamp": 1730476800,
        "api_date": "2024-11-01T12:00:00+00:00",
        "ingested_at": "2024-11-01T12:00:30.000+00:00",
        "@timestamp": "2024-11-01T12:00:30.000+00:00",
        "source": "exchangerate-api.com"
    }
