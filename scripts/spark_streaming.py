from dotenv import load_dotenv
from scripts.ch_migrator import ClickHouseMigrator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pandas as pd
import logging
import os

logging.basicConfig(
    filename='../pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

if os.name == 'nt':
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ['PATH']

def streaming():
    spark = (SparkSession.builder
             .appName("TaxiStreamingApp")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.host", "127.0.0.1")
             .master("local[*]")
             .getOrCreate())

    logging.info('Sark Session Launched!')

    cleaned_df = get_final_df(spark)

    start_query(spark, cleaned_df)

def start_query(spark, cleaned_df):
    query = (cleaned_df
             .writeStream
             .foreachBatch(write_to_clickhouse)
             .start())
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logging.info('Interrupt signal received, shutting down')
    except Exception as e:
        logging.error(f'Error in query: {e}')
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
    try:
        kafka_df = (spark_session
                    .readStream
                    .format('kafka')
                    .option('kafka.bootstrap.servers', 'localhost:9092')
                    .option('subscribe', 'rides_topic')
                    .load())
        return kafka_df
    except Exception as e:
        logging.error(f'Error in reading from kafka, returning empty df: {e}')
        return SQLContext.createDataFrame(spark_session.sparkContext.emptyRDD(), get_data_struct())

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
    df = batch_df.toPandas()

    if df.empty:
        return

    logging.info(f'Writing batch {batch_id} to ClickHouse. Length: {len(df)}')

    load_dotenv()
    migrator = ClickHouseMigrator(host='localhost',
                                  port=8123,
                                  user=os.getenv('CH_USER'),
                                  password=os.getenv('CH_PASSWORD'))
    clickhouse_client = migrator.client
    try:
        clickhouse_client.insert_df('realtime_rides', df)
        logging.info('Successfully inserted data to ClickHouse.')
    except Exception as e:
        logging.error(f'Error inserting batch into ClickHouse: {e}')


if __name__ == '__main__':
    streaming()

