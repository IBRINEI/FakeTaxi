from unittest.mock import patch, Mock, ANY
from taxi_emulator import generate_single_event, get_producer

def test_generate_single_event():
    given_generated_event = generate_single_event()
    assert given_generated_event is not None
    assert isinstance(given_generated_event, dict)
    assert set(given_generated_event.keys()) == {'ride_id', 'driver_id', 'user_id', 'status', 'fare_amount',
                                                 'event_timestamp'}
    assert given_generated_event['status'] in ['requested', 'en_route', 'completed', 'cancelled']

@patch('taxi_emulator.KafkaProducer')
def test_start_sending_events(mock_producer_class):
    given_producer = get_producer()
    mock_producer_class.assert_called_once_with(bootstrap_servers='localhost:9092',
                                                value_serializer=ANY)
    assert given_producer == mock_producer_class.return_value