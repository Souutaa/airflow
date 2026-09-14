#!/usr/bin/env bash
set -euo pipefail

readonly connection_builder='/opt/airflow/scripts/build_airflow_connections.py'

exec python "${connection_builder}" exec "$@"
