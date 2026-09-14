from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Mapping
from urllib.parse import quote

CONNECTION_ID_PATTERN = re.compile(r'^[a-z][a-z0-9_]*$')
CONNECTION_TYPE_PATTERN = re.compile(r'^[a-z][a-z0-9_]*$')


def _required(
    environment: Mapping[str, str],
    name: str,
    *,
    preserve_whitespace: bool = False,
) -> str:
    raw_value = environment.get(name, '')
    if not raw_value.strip():
        raise ValueError(f'Required environment variable is missing: {name}')
    return raw_value if preserve_whitespace else raw_value.strip()


def _port(environment: Mapping[str, str], name: str) -> int:
    raw_port = _required(environment, name)
    try:
        port = int(raw_port)
    except ValueError as error:
        raise ValueError(f'{name} must be an integer') from error
    if not 1 <= port <= 65535:
        raise ValueError(f'{name} must be between 1 and 65535')
    return port


def _uri_host(host: str) -> str:
    if ':' in host and not host.startswith('['):
        return f'[{host}]'
    return host


def build_metadata_uri(environment: Mapping[str, str]) -> str:
    host = _uri_host(_required(environment, 'AIRFLOW_DATABASE_HOST'))
    port = _port(environment, 'AIRFLOW_DATABASE_PORT')
    database = quote(_required(environment, 'AIRFLOW_DATABASE_NAME'), safe='')
    username = quote(_required(environment, 'AIRFLOW_DATABASE_USER'), safe='')
    password = quote(
        _required(
            environment,
            'AIRFLOW_DATABASE_PASSWORD',
            preserve_whitespace=True,
        ),
        safe='',
    )
    return f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'


def parse_connection_ids(environment: Mapping[str, str]) -> list[str]:
    raw_ids = _required(environment, 'AIRFLOW_CONNECTION_IDS')
    connection_ids = [value.strip() for value in raw_ids.split(',') if value.strip()]
    if not connection_ids:
        raise ValueError('AIRFLOW_CONNECTION_IDS must contain at least one connection ID')

    invalid_ids = [
        connection_id
        for connection_id in connection_ids
        if not CONNECTION_ID_PATTERN.fullmatch(connection_id)
    ]
    if invalid_ids:
        raise ValueError(
            'Connection IDs must use lowercase snake_case and start with a letter: '
            + ', '.join(invalid_ids)
        )
    if len(connection_ids) != len(set(connection_ids)):
        raise ValueError('AIRFLOW_CONNECTION_IDS contains duplicate connection IDs')
    return connection_ids


def build_connection_json(environment: Mapping[str, str], connection_id: str) -> str:
    prefix = f'AIRFLOW_CONNECTION_{connection_id.upper()}'
    connection_type = _required(environment, f'{prefix}_TYPE').lower()
    if not CONNECTION_TYPE_PATTERN.fullmatch(connection_type):
        raise ValueError(f'{prefix}_TYPE is not a valid Airflow connection type')

    connection = {
        'conn_type': connection_type,
        'host': _required(environment, f'{prefix}_HOST'),
        'port': _port(environment, f'{prefix}_PORT'),
        'login': _required(environment, f'{prefix}_USER'),
        'password': _required(
            environment,
            f'{prefix}_PASSWORD',
            preserve_whitespace=True,
        ),
        'schema': _required(environment, f'{prefix}_DATABASE'),
    }
    return json.dumps(connection, ensure_ascii=False, separators=(',', ':'))


def build_airflow_environment(environment: Mapping[str, str]) -> dict[str, str]:
    generated_environment = dict(environment)
    generated_environment['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'] = (
        build_metadata_uri(environment)
    )
    for connection_id in parse_connection_ids(environment):
        variable_name = f'AIRFLOW_CONN_{connection_id.upper()}'
        generated_environment[variable_name] = build_connection_json(
            environment,
            connection_id,
        )
    return generated_environment


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in {'exec', 'validate'}:
        print(
            'Usage: build_airflow_connections.py {validate|exec COMMAND [ARG ...]}',
            file=sys.stderr,
        )
        return 2
    try:
        generated_environment = build_airflow_environment(os.environ)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1

    if sys.argv[1] == 'validate':
        connection_count = len(parse_connection_ids(os.environ))
        print(f'Validated metadata and {connection_count} external connections')
        return 0

    if len(sys.argv) < 3:
        print('The exec command requires a command to run', file=sys.stderr)
        return 2
    os.execve('/entrypoint', ['/entrypoint', *sys.argv[2:]], generated_environment)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
