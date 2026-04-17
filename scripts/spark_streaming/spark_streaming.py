import pyspark
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
import logging
import os


logging.basicConfig(
    filename='../../pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'realtime_rides')

CH_HOST = os.getenv('CH_HOST', 'clickhouse')
CH_PORT = int(os.getenv('CH_PORT', 8123))
CH_USER = os.getenv('CH_USER', 'default')
CH_PASSWORD = os.getenv('CH_PASSWORD', '')

if os.name == 'nt':
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ['PATH']

def streaming():
    logging.info("Initializing Spark Session...")
    spark = (SparkSession.builder
             .appName("TaxiStreamingApp")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
             .config("spark.driver.bindAddress", "0.0.0.0")
             .config("spark.driver.host", "0.0.0.0")
             .config("spark.driver.memory", "512m")
             .config("spark.executor.memory", "512m")
             .master("local[*]")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    logging.info('Sark Session Launched!')

    cleaned_df = get_final_df(spark)

    start_query(spark, cleaned_df)

def start_query(spark, cleaned_df):
    logging.info(f"Starting write stream to ClickHouse at {CH_HOST}...")
    query = (cleaned_df
             .writeStream
             .option("checkpointLocation", "/tmp/checkpoints/realtime_rides")
             .foreachBatch(write_to_clickhouse)
             .start())
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logging.info('Interrupt signal received, shutting down')
    except Exception as e:
        logging.error(f'Error in query: {e}')
        raise e
    finally:
        query.stop()
        spark.stop()
        logging.info('Spark shut down')

def get_final_df(spark_session):
    kafka_df = read_df_from_kafka(spark_session)
    cleaned_df = parse_and_clean_df(kafka_df)
    return cleaned_df

def parse_and_clean_df(df):
    parsed_df = parse_df(df)
    cleaned_df = clean_df(parsed_df)
    return cleaned_df

def read_df_from_kafka(spark_session):
    return (spark_session
            .readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', KAFKA_BROKER)
            .option('subscribe', KAFKA_TOPIC)
            .option('startingOffsets', 'latest')
            .load())

def parse_df(loaded_df):
    taxi_struct = get_data_struct()
    string_df = loaded_df.selectExpr("CAST(value AS STRING)")
    parsed_df = string_df.select(from_json(col("value"), taxi_struct).alias("data")).select("data.*")
    return parsed_df

def clean_df(df):
    cleaned_df = df.filter(col('fare_amount') > 0)
    return cleaned_df

def get_data_struct():
    taxi_struct = StructType([
        StructField('ride_id', StringType(), True),
        StructField('driver_id', IntegerType(), True),
        StructField('client_id', IntegerType(), True),
        StructField('status', StringType(), True),
        StructField('fare_amount', FloatType(), True),
        StructField('event_timestamp', StringType(), True),
    ])
    return taxi_struct


def write_to_clickhouse(batch_df, batch_id):
    ch_creds = {
        'host': CH_HOST,
        'port': CH_PORT,
        'user': CH_USER,
        'password': CH_PASSWORD
    }
    batch_df.foreachPartition(lambda partition: send_partition_to_clickhouse(partition, ch_creds))


def send_partition_to_clickhouse(partition, creds):
    import clickhouse_connect

    data = []

    for row in partition:
        data.append([
            row.ride_id,
            row.driver_id,
            row.client_id,
            row.status,
            row.fare_amount,
            row.event_timestamp
        ])

    if not data:
        return

    try:
        client = clickhouse_connect.get_client(
            host=creds['host'],
            port=creds['port'],
            username=creds['user'],
            password=creds['password']
        )

        client.insert('realtime_rides', data, column_names=[
            'ride_id', 'driver_id', 'client_id', 'status', 'fare_amount', 'event_timestamp'
        ])
    except Exception as e:
        print(f"Error while writing partition to ClickHouse: {e}")


if __name__ == '__main__':
    streaming()

