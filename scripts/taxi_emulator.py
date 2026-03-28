import json
import logging
import uuid
from kafka import KafkaConsumer, KafkaProducer
import random
import time
from datetime import datetime

logging.basicConfig(
    filename='../pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


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
    producer = get_producer()
    try:
        while True:
            generated_event = generate_single_event()
            producer.send('rides_topic', generated_event)
            time.sleep(random.uniform(1, 10))
    except KeyboardInterrupt:
        logging.info('Stopped producer.')
    except Exception as e:
        logging.error(f'Error in producing event: {e}')
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    start_sending_events()
