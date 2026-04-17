import pytest
import requests_mock
import requests
from taxi_emulator import generate_single_event, API_URL


def test_generate_ride_contract():
    ride = generate_single_event()

    expected_keys = {"ride_id", "driver_id", "client_id", "status", "fare_amount", "event_timestamp"}

    assert set(ride.keys()) == expected_keys, "Generated dictionary lacks required keys"
    assert isinstance(ride["fare_amount"], float), "Fare amount must be a float"
    assert isinstance(ride["driver_id"], int), "Driver ID must be an integer"
    assert ride["status"] in ['requested', 'en_route', 'completed', 'cancelled'], "Unknown ride status generated"


def test_api_unreachable_handling():
    ride_data = generate_single_event()

    with requests_mock.Mocker() as m:
        m.post(API_URL, exc=requests.exceptions.ConnectionError("Connection refused"))

        try:
            response = requests.post(API_URL, json=ride_data)
        except requests.exceptions.RequestException:
            pass
        except Exception as e:
            pytest.fail(f"Unexpected exception raised: {e}")