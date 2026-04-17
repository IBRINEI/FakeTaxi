import pytest
from unittest.mock import patch, MagicMock

# Create a mock producer before importing the app
mock_producer_instance = MagicMock()

with patch('kafka.KafkaProducer', return_value=mock_producer_instance):
    from api_server import app
    from fastapi.testclient import TestClient

client = TestClient(app)


def test_create_ride_success():
    mock_producer_instance.send.reset_mock()

    valid_payload = {
        "ride_id": "test-uuid-123",
        "driver_id": 42,
        "client_id": 99,
        "status": "completed",
        "fare_amount": 150.50,
        "event_timestamp": "2026-04-14T10:00:00"
    }

    with TestClient(app) as client:
        response = client.post("/api/v1/rides", json=valid_payload)

    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert mock_producer_instance.send.call_count == 1

    called_args = mock_producer_instance.send.call_args[0]
    assert called_args[0] == 'realtime_rides'


def test_create_ride_validation_error():
    mock_producer_instance.send.reset_mock()

    invalid_payload = {
        "ride_id": "test-uuid-123",
        "driver_id": 42,
        "client_id": 99,
        "status": "completed",
        "fare_amount": "INVALID_PRICE",
        "event_timestamp": "2026-04-14T10:00:00"
    }

    with TestClient(app) as client:
        response = client.post("/api/v1/rides", json=invalid_payload)

    assert response.status_code == 422
    assert mock_producer_instance.send.call_count == 0