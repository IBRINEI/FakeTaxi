import json
import logging
import uuid
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import time
import requests
from faker import Faker
import random

logging.basicConfig(
    filename='../pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

API_URL = "http://localhost:8000/api/v1/rides"


def get_producer():
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer

def generate_single_event() -> dict:
    try:
        fake_event = {
            'ride_id': str(uuid.uuid4()),
            'driver_id': random.randint(1, 1000),
            'user_id': random.randint(1, 10000),
            'status': random.choice(['requested', 'en_route', 'completed', 'cancelled']),
            'fare_amount': round(random.uniform(5.0, 1000.0), 2) *
                           random.choices([-1, 1], weights=[0.01, 0.99])[0],
            'event_timestamp': datetime.now().isoformat()
        }
        logging.info('Event generated successfully.')
    except Exception as e:
        logging.error(f'Error in generating event: {e}')
        raise

    return fake_event


def start_sending_events():
    while True:
        ride_data = generate_single_event()
        try:
            response = requests.post(API_URL, json=ride_data)
            if response.status_code == 200:
                logging.info(f"Event sent successfully {ride_data['ride_id']} -> Backend -> Kafka")
            else:
                logging.warning(f"API validation error: {response.text}")
        except Exception as e:
            logging.error(f'Error in sending event: {e}')

        time.sleep(random.randint(1, 5))


if __name__ == '__main__':
    start_sending_events()
