from unittest.mock import patch
from scripts.postgres_migrator import PostgresMigrator, PG_SCHEMA
import pytest

def init_migrator():
    migrator = PostgresMigrator(
        host='localhost',
        port=5432,
        user='PG_USER',
        password='PG_PASSWORD',
        database='taxi_dwh'
    )
    return migrator

@patch('scripts.postgres_migrator.psycopg2.connect')
def test_postgres_migrator_init(mock_get_client):
    migrator = init_migrator()
    mock_get_client.assert_called_once_with(
        host='localhost',
        port=5432,
        user='PG_USER',
        password='PG_PASSWORD',
        database='taxi_dwh'
    )
    assert migrator.client == mock_get_client.return_value

@patch('scripts.postgres_migrator.psycopg2.connect')
def test_postgres_migrator_apply_schema_success(mock_get_client):
    given_mock_db_client = mock_get_client.return_value
    mock_cursor = given_mock_db_client.cursor.return_value.__enter__.return_value

    migrator = init_migrator()
    migrator.apply_schema(PG_SCHEMA)

    given_mock_db_client.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with(PG_SCHEMA)
    given_mock_db_client.commit.assert_called_once()
    given_mock_db_client.rollback.assert_not_called()
    given_mock_db_client.close.assert_called_once()

@patch('scripts.postgres_migrator.psycopg2.connect')
def test_postgres_migrator_apply_schema_failure(mock_get_client):
    given_mock_db_client = mock_get_client.return_value
    mock_cursor = given_mock_db_client.cursor.return_value.__enter__.return_value

    mock_cursor.execute.side_effect = Exception('Test exception')
    migrator = init_migrator()
    with pytest.raises(Exception, match='Test exception'):
        migrator.apply_schema(PG_SCHEMA)

    mock_cursor.execute.assert_called_once_with(PG_SCHEMA)
    given_mock_db_client.commit.assert_not_called()
    given_mock_db_client.rollback.assert_called_once()
    given_mock_db_client.close.assert_called_once()