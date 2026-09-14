FROM apache/airflow:3.3.1-python3.11

COPY requirements.txt /requirements.txt
COPY --chown=airflow:root scripts/airflow-entrypoint.sh scripts/build_airflow_connections.py /opt/airflow/scripts/

# Keep the Airflow core version fixed while installing project dependencies.
RUN pip install --no-cache-dir -r /requirements.txt
