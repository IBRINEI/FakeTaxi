import pytest
import json
from unittest.mock import patch, MagicMock
import os, sys


TESTS_FOLDER = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_FOLDER = os.path.dirname(TESTS_FOLDER)

if SCRIPTS_FOLDER not in sys.path:
    sys.path.insert(0, SCRIPTS_FOLDER)

from minio_lake_writer import consume_and_save_to_lake, BATCH_SIZE


@patch('minio_lake_writer.get_s3_client')
@patch('minio_lake_writer.KafkaConsumer')
def test_consume_and_save_to_lake_batching(mock_kafka_class, mock_s3_client_func):
    mock_s3 = MagicMock()
    mock_s3_client_func.return_value = mock_s3

    mock_consumer = MagicMock()
    mock_kafka_class.return_value = mock_consumer

    fake_messages = []
    for i in range(BATCH_SIZE):
        msg = MagicMock()
        msg.value = {"ride_id": f"ride_{i}", "driver_id": 100, "status": "completed"}
        fake_messages.append(msg)

    mock_consumer.__iter__.return_value = fake_messages

    consume_and_save_to_lake()


    assert mock_s3.put_object.call_count == 1, "Only one batch must be sent!"


    called_args = mock_s3.put_object.call_args[1]

    assert called_args['Bucket'] == 'taxi-raw-data'
    assert called_args['ContentType'] == 'application/jsonlines'

    body_bytes = called_args['Body']
    body_string = body_bytes.decode('utf-8')
    lines = body_string.split('\n')

    assert len(lines) == BATCH_SIZE, f"There should be exactly {BATCH_SIZE} lines in the file"

    first_record = json.loads(lines[0])
    assert first_record['ride_id'] == "ride_0"
    assert first_record['driver_id'] == 100