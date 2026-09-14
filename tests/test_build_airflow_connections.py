import json
import unittest

from scripts.build_airflow_connections import (
    build_airflow_environment,
    build_connection_json,
    build_metadata_uri,
    parse_connection_ids,
)


def metadata_environment():
    return {
        'AIRFLOW_DATABASE_HOST': 'postgres.example.internal',
        'AIRFLOW_DATABASE_PORT': '5432',
        'AIRFLOW_DATABASE_NAME': 'airflow_metadata',
        'AIRFLOW_DATABASE_USER': 'airflow_meta',
        'AIRFLOW_DATABASE_PASSWORD': 'metadata-secret',
    }


class BuildAirflowConnectionsTest(unittest.TestCase):
    def test_metadata_uri_encodes_credentials(self):
        environment = {
            'AIRFLOW_DATABASE_HOST': 'postgres.example.internal',
            'AIRFLOW_DATABASE_PORT': '5432',
            'AIRFLOW_DATABASE_NAME': 'airflow metadata',
            'AIRFLOW_DATABASE_USER': 'air@flow',
            'AIRFLOW_DATABASE_PASSWORD': 'p@ss:/?#[]',
        }

        uri = build_metadata_uri(environment)

        self.assertEqual(
            uri,
            'postgresql+psycopg2://air%40flow:p%40ss%3A%2F%3F%23%5B%5D'
            '@postgres.example.internal:5432/airflow%20metadata',
        )

    def test_business_connection_preserves_raw_special_characters(self):
        environment = {
            'AIRFLOW_CONNECTION_SOURCE_OLIST_TYPE': 'mysql',
            'AIRFLOW_CONNECTION_SOURCE_OLIST_HOST': 'mysql.example.internal',
            'AIRFLOW_CONNECTION_SOURCE_OLIST_PORT': '3306',
            'AIRFLOW_CONNECTION_SOURCE_OLIST_DATABASE': 'olist',
            'AIRFLOW_CONNECTION_SOURCE_OLIST_USER': 'etl-user',
            'AIRFLOW_CONNECTION_SOURCE_OLIST_PASSWORD': ' quotes" and slash\\ and @ ',
        }

        connection = json.loads(build_connection_json(environment, 'source_olist'))

        self.assertEqual(
            connection['password'],
            environment['AIRFLOW_CONNECTION_SOURCE_OLIST_PASSWORD'],
        )
        self.assertEqual(connection['port'], 3306)
        self.assertEqual(connection['conn_type'], 'mysql')

    def test_multiple_connections_generate_native_airflow_variables(self):
        environment = {
            **metadata_environment(),
            'AIRFLOW_CONNECTION_IDS': 'mysql,warehouse_postgres',
            'AIRFLOW_CONNECTION_MYSQL_TYPE': 'mysql',
            'AIRFLOW_CONNECTION_MYSQL_HOST': 'mysql.example.internal',
            'AIRFLOW_CONNECTION_MYSQL_PORT': '3306',
            'AIRFLOW_CONNECTION_MYSQL_DATABASE': 'olist',
            'AIRFLOW_CONNECTION_MYSQL_USER': 'source',
            'AIRFLOW_CONNECTION_MYSQL_PASSWORD': 'mysql-secret',
            'AIRFLOW_CONNECTION_WAREHOUSE_POSTGRES_TYPE': 'postgres',
            'AIRFLOW_CONNECTION_WAREHOUSE_POSTGRES_HOST': 'pg.example.internal',
            'AIRFLOW_CONNECTION_WAREHOUSE_POSTGRES_PORT': '5432',
            'AIRFLOW_CONNECTION_WAREHOUSE_POSTGRES_DATABASE': 'warehouse',
            'AIRFLOW_CONNECTION_WAREHOUSE_POSTGRES_USER': 'loader',
            'AIRFLOW_CONNECTION_WAREHOUSE_POSTGRES_PASSWORD': 'postgres-secret',
        }

        generated = build_airflow_environment(environment)

        self.assertEqual(
            json.loads(generated['AIRFLOW_CONN_MYSQL'])['schema'],
            'olist',
        )
        self.assertEqual(
            json.loads(generated['AIRFLOW_CONN_WAREHOUSE_POSTGRES'])['conn_type'],
            'postgres',
        )
        self.assertTrue(
            generated['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'].startswith(
                'postgresql+psycopg2://'
            )
        )

    def test_invalid_port_is_rejected(self):
        environment = {
            'AIRFLOW_CONNECTION_POSTGRES_TYPE': 'postgres',
            'AIRFLOW_CONNECTION_POSTGRES_HOST': 'postgres.example.internal',
            'AIRFLOW_CONNECTION_POSTGRES_PORT': 'invalid',
            'AIRFLOW_CONNECTION_POSTGRES_DATABASE': 'ecommerce_dw',
            'AIRFLOW_CONNECTION_POSTGRES_USER': 'warehouse',
            'AIRFLOW_CONNECTION_POSTGRES_PASSWORD': 'secret',
        }

        with self.assertRaisesRegex(ValueError, 'must be an integer'):
            build_connection_json(environment, 'postgres')

    def test_invalid_connection_id_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'lowercase snake_case'):
            parse_connection_ids({'AIRFLOW_CONNECTION_IDS': 'mysql,CRM-Database'})

    def test_duplicate_connection_id_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            parse_connection_ids({'AIRFLOW_CONNECTION_IDS': 'mysql,mysql'})


if __name__ == '__main__':
    unittest.main()
