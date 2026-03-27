import clickhouse_connect
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(
    filename='../pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

schema = """
CREATE TABLE IF NOT EXISTS realtime_rides (
    ride_id String,
    driver_id Int32,
    client_id Int32,
    status String,
    fare_amount Float32,
    event_timestamp String
) ENGINE = MergeTree() ORDER BY event_timestamp;
"""

class ClickHouseMigrator:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.client = self.init_connection()

    def init_connection(self):
        try:
            client = clickhouse_connect.get_client(host=self.host,
                                                   port=self.port,
                                                   username=self.user,
                                                   password=self.password)
        except Exception as e:
            logging.error(f'Unable to get client: {e}')
            client = None
        return client

    def apply_schema(self, schema):
        try:
            self.client.command(schema)
            logging.info('Successfully applied schema')
        except Exception as e:
            logging.error(f'Unable to apply schema: {e}')


if __name__ == '__main__':
    load_dotenv()
    migrator = ClickHouseMigrator(host='localhost',
                                  port=8123,
                                  user=os.getenv('CH_USER'),
                                  password=os.getenv('CH_PASSWORD'))
    migrator.apply_schema(schema)