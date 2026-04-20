import os
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from ch_migrator import ClickHouseMigrator, schema
from postgres_migrator import PostgresMigrator, PG_SCHEMA

def main():
    logging.info("Starting infrastructure initialization...")

    try:
        logging.info("Initializing ClickHouse schema...")
        ch_migrator = ClickHouseMigrator(
            host=os.getenv('CH_HOST', 'clickhouse'),
            port=int(os.getenv('CH_PORT', 8123)),
            user=os.getenv('CH_USER', 'default'),
            password=os.getenv('CH_PASSWORD', '')
        )
        ch_migrator.apply_schema(schema)
        logging.info("ClickHouse schema verified.")
    except Exception as e:
        logging.error(f"Failed to initialize ClickHouse: {e}")
        sys.exit(1)

    try:
        logging.info("Initializing Postgres schema...")
        pg_migrator = PostgresMigrator(
            host=os.getenv('PG_HOST', 'postgres'),
            port=int(os.getenv('PG_PORT', 5432)),
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD'),
            database='taxi_dwh'
        )
        pg_migrator.apply_schema(PG_SCHEMA)
        logging.info("Postgres schema verified.")
    except Exception as e:
        logging.error(f"Failed to initialize Postgres: {e}")
        sys.exit(1)

    logging.info("Infrastructure initialization completed successfully.")

if __name__ == "__main__":
    main()