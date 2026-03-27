import pytest
import json
from pyspark.sql import SparkSession
import sys
import os

from scripts.spark_streaming import parse_and_clean_df

if os.name == 'nt':
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ['PATH']
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

@pytest.fixture(scope="session")
def spark():
    spark_session = (SparkSession.builder
                     .appName("TestTaxiStreaming")
                     .config("spark.driver.bindAddress", "127.0.0.1")
                     .config("spark.driver.host", "127.0.0.1")
                     .master("local[1]")
                     .getOrCreate())
    yield spark_session
    spark_session.stop()

def test_parse_and_clean_data(spark):
    good_json = json.dumps(
        {"ride_id": "1", "driver_id": 10, "client_id": 100, "status": "completed", "fare_amount": 25.5,
         "event_timestamp": "2026-03-27T10:00:00"})
    bad_json = json.dumps(
        {"ride_id": "2", "driver_id": 11, "client_id": 101, "status": "cancelled", "fare_amount": -10.0,
         "event_timestamp": "2026-03-27T10:01:00"})

    test_data = [{"value": good_json}, {"value": bad_json}]

    raw_df = spark.createDataFrame(test_data)

    result_df = parse_and_clean_df(raw_df)

    result_data = result_df.collect()

    assert len(result_data) == 1
    assert result_data[0].ride_id == "1"
    assert result_data[0].fare_amount == 25.5