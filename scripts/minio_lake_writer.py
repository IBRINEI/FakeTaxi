import os
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logging.basicConfig(
    filename='../pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_PASSWORD', 'minioadmin')
BUCKET_NAME = 'taxi-raw-data'

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'rides_topic'

BATCH_SIZE = 50


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )


def consume_and_save_to_lake():
    s3 = get_s3_client()

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    logging.info(f"Listening to Kafka and writing to MinIO (Bucket: {BUCKET_NAME})...")

    batch = []

    for message in consumer:
        ride_data = message.value
        batch.append(ride_data)

        if len(batch) >= BATCH_SIZE:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"raw_rides_{timestamp}.jsonl"

            jsonl_data = "\n".join([json.dumps(record) for record in batch])

            try:
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=file_name,
                    Body=jsonl_data.encode('utf-8'),
                    ContentType='application/jsonlines'
                )
                print(f"Successfully saved batch of length {BATCH_SIZE} -> {file_name}")
            except ClientError as e:
                print(f"Error while writing to MinIO: {e}")

            batch = []


if __name__ == "__main__":
    consume_and_save_to_lake()