import logging
import psycopg2
import os
from dotenv import load_dotenv

logging.basicConfig(
    filename='../pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS driver_daily_revenue (
    driver_id INTEGER,
    total_rides INTEGER,
    total_revenue DECIMAL,
    calc_date DATE,
    PRIMARY KEY (driver_id, calc_date)
) 
"""


class PostgresMigrator:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.client = self.init_connection()

    def init_connection(self):
        try:
            client = psycopg2.connect(host=self.host,
                                                   port=self.port,
                                                   user=self.user,
                                                   password=self.password,
                                                   database=self.database)
        except Exception as e:
            logging.error(f'Unable to get client: {e}')
            raise e
        return client

    def apply_schema(self, schema):
        try:
            with self.client.cursor() as cursor:
                cursor.execute(schema)
            self.client.commit()
            logging.info('Successfully applied schema')
        except Exception as e:
            self.client.rollback()
            logging.error(f'Unable to apply schema: {e}')
            raise e
        finally:
            if self.client:
                self.client.close()
                logging.info('PostgreSQL connection closed')

if __name__ == '__main__':
    load_dotenv()
    migrator = PostgresMigrator(
        host='localhost',
        port=5432,
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        database='taxi_dwh'
    )
    migrator.apply_schema(PG_SCHEMA)