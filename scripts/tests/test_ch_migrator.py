from unittest.mock import patch
from scripts.ch_migrator import ClickHouseMigrator, schema

def init_migrator():
    migrator = ClickHouseMigrator(
        host='localhost',
        port=8123,
        user='default',
        password='fake_password'
    )
    return migrator

@patch('ch_migrator.clickhouse_connect.get_client')
def test_clickhouse_migrator_init(mock_get_client):
    migrator = init_migrator()
    mock_get_client.assert_called_once_with(
        host='localhost',
        port=8123,
        username='default',
        password='fake_password'
    )

@patch('ch_migrator.clickhouse_connect.get_client')
def test_clickhouse_migrator_get_client(mock_get_client):
    given_mock_db_client = mock_get_client.return_value
    migrator = init_migrator()
    migrator.apply_schema(schema)
    given_mock_db_client.command.assert_called_once_with(schema)