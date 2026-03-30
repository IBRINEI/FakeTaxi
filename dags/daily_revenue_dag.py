from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

from sqlalchemy.testing.plugin.plugin_base import logging

DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT = os.path.dirname(DAGS_FOLDER)

SCRIPTS_FOLDER = os.path.join(PROJECT_ROOT, 'scripts')

if SCRIPTS_FOLDER not in sys.path:
    sys.path.insert(0, SCRIPTS_FOLDER)

from postgres_migrator import PostgresMigrator, PG_SCHEMA
from ch_migrator import ClickHouseMigrator

import clickhouse_connect
import psycopg2
from psycopg2.extras import execute_values

import logging
logging.basicConfig(
    filename='../pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def init_postgres_db():
    migrator = PostgresMigrator(
        host='postgres',
        port=5432,
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        database='taxi_dwh'
    )
    migrator.apply_schema(PG_SCHEMA)

def calculate_daily_revenue(**kwargs):
    exec_date = kwargs['ds']
    logging.info(f'Starting calculation of daily revenue for {exec_date}')

    ch_migrator = ClickHouseMigrator(host='clickhouse',
                                  port=8123,
                                  user=os.getenv('CH_USER'),
                                  password=os.getenv('CH_PASSWORD'))
    ch_client = ch_migrator.client
    query = f"""
        SELECT 
            driver_id, 
            toInt32(count(ride_id)) as total_rides, 
            toFloat32(sum(fare_amount)) as total_revenue, 
            toDate('{exec_date}') as calc_date
        FROM realtime_rides 
        WHERE status = 'completed' 
          AND toDate(event_timestamp) = toDate('{exec_date}')
        GROUP BY driver_id
    """
    try:
        result = ch_client.query(query)
    except Exception as e:
        logging.error(f"Error when querying: {e}")
        raise e

    if not result.result_rows:
        logging.warning(f"No data for {exec_date}")
        return

    pg_migrator = PostgresMigrator(
        host='postgres',
        port=5432,
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        database='taxi_dwh'
    )
    pg_connection = pg_migrator.client

    data_tuples = result.result_rows
    insert_query = """
            INSERT INTO driver_daily_revenue (driver_id, total_rides, total_revenue, calc_date)
            VALUES %s
            ON CONFLICT (driver_id, calc_date) 
            DO UPDATE SET 
                total_rides = EXCLUDED.total_rides,
                total_revenue = EXCLUDED.total_revenue;
        """

    try:
        with pg_connection.cursor() as cursor:
            execute_values(cursor, insert_query, data_tuples)
    except Exception as e:
        logging.error(f"Error when inserting data: {e}")
        raise e

    pg_connection.commit()
    pg_connection.close()
    logging.info(f'Finished calculation of daily revenue for {exec_date}. Inserted {len(data_tuples)} rows')


default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='driver_daily_revenue_etl',
    default_args=default_args,
    description='Driver daily revenue calculation',
    schedule='0 2 * * *', # 02:00 daily
    start_date=datetime(2026, 3, 27),
    catchup=False,
) as dag:

    # Задача 1
    create_table_task = PythonOperator(
        task_id='init_postgres_schema',
        python_callable=init_postgres_db
    )

    # Задача 2
    calculate_revenue_task = PythonOperator(
        task_id='calculate_revenue_and_load',
        python_callable=calculate_daily_revenue,
    )

    create_table_task >> calculate_revenue_task



