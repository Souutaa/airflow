# Unified development and production deployment

The repository has one `docker-compose.yaml`. Airflow always connects to real
PostgreSQL and MySQL hosts; Compose does not start database containers.

Environment selection is the only deployment difference:

```text
Development  -> .env.dev
Production   -> .env.prod
```

Both files must contain exactly the same keys. Only their values may differ.
They are ignored by Git; `.env.example` is the committed canonical schema.

## Architecture

```text
Airflow containers
    +-- PostgreSQL: airflow_metadata (Airflow internal state)
    +-- PostgreSQL: ecommerce_dw    (staging and warehouse)
    `-- MySQL:      olist           (source/business data)
```

Metadata and business data must use separate databases and users even when
they share the same PostgreSQL server.

## Database preparation

Create the PostgreSQL databases/users and MySQL source user using the templates:

```bash
sudo -u postgres psql -f deployment/server/postgres-bootstrap.sql.example
mysql -u root -p < deployment/server/mysql-bootstrap.sql.example
```

Replace every placeholder first. Configure PostgreSQL `listen_addresses`,
`pg_hba.conf`, MySQL `bind-address`, and the network firewall so only the
Airflow server can reach ports 5432 and 3306.

For PostgreSQL, add rules equivalent to the following, using the actual Airflow
source IP/CIDR:

```text
host  airflow_metadata  airflow_meta       AIRFLOW_SOURCE_CIDR  scram-sha-256
host  ecommerce_dw      airflow_warehouse  AIRFLOW_SOURCE_CIDR  scram-sha-256
```

For MySQL, replace `REPLACE_WITH_ALLOWED_SOURCE_HOST` in the bootstrap template
with the Airflow server address as observed by MySQL.

## Environment files

`.env.dev` and `.env.prod` already contain the same key set with safe
placeholders. Replace values locally; never commit either file.

Important rules:

- `AIRFLOW_ENV_FILE` must name the file being used.
- Use real private IP addresses or DNS names for database hosts.
- Enter database usernames and passwords as separate values; do not URL-encode
  or build JSON manually. The image safely generates Airflow's metadata URI and
  Connection JSON at startup. In an env file, wrap a value containing `$`, `#`,
  whitespace, or other env-file syntax in single quotes.
- Preserve the Fernet key when restoring existing Airflow metadata.
- Set `AIRFLOW_CREATE_ADMIN_USER=true` only for the first initialization of an
  empty metadata database; otherwise use `false`.

Database values are grouped as follows:

```text
AIRFLOW_DATABASE_*          -> PostgreSQL metadata required to start Airflow
AIRFLOW_CONNECTION_IDS     -> comma-separated external connection IDs
AIRFLOW_CONNECTION_<ID>_*  -> component fields for each external connection
```

Connection IDs must use lowercase `snake_case`. Each ID requires `TYPE`, `HOST`,
`PORT`, `DATABASE`, `USER`, and `PASSWORD` fields. The existing IDs remain
`mysql` and `postgres`, so current DAG code does not need to change.

The container entrypoint converts these fields into Airflow's native variables.
For example, `mysql` becomes `AIRFLOW_CONN_MYSQL`, while `source_crm` becomes
`AIRFLOW_CONN_SOURCE_CRM`. It also generates
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` for metadata. Do not add any of those
generated variables to the env files.

To add another database, add its ID and component fields to both environment
files; `docker-compose.yaml` does not need to change:

```env
AIRFLOW_CONNECTION_IDS=mysql,postgres,source_crm

AIRFLOW_CONNECTION_SOURCE_CRM_TYPE=mysql
AIRFLOW_CONNECTION_SOURCE_CRM_HOST=crm-db.example.internal
AIRFLOW_CONNECTION_SOURCE_CRM_PORT=3306
AIRFLOW_CONNECTION_SOURCE_CRM_DATABASE=crm
AIRFLOW_CONNECTION_SOURCE_CRM_USER=airflow_reader
AIRFLOW_CONNECTION_SOURCE_CRM_PASSWORD='replace-with-password'
```

The corresponding DAG uses `mysql_conn_id='source_crm'`. Keep the same set of
keys in `.env.dev` and `.env.prod`; only the values should differ.

Prepare the production log directory:

```bash
sudo install -d -m 0770 -o 50000 -g 0 /data/airflow/logs
```

## Restore existing databases

At cutover, stop the old scheduler and take fresh consistent backups. Restore
only into empty target databases:

```bash
pg_restore --no-owner --no-acl \
  --dbname=postgresql://airflow_meta@POSTGRES_HOST/airflow_metadata \
  airflow-metadata.dump

pg_restore --no-owner --no-acl \
  --dbname=postgresql://airflow_warehouse@POSTGRES_HOST/ecommerce_dw \
  business-warehouse.dump

mysql -h MYSQL_HOST -u root -p olist < mysql-business.sql
```

Do not point an older Airflow version at metadata migrated by a newer version.

## Development commands

```bash
docker compose --env-file .env.dev config --quiet
docker compose --env-file .env.dev build
docker compose --env-file .env.dev up airflow-init
docker compose --env-file .env.dev up -d --wait
```

Equivalent Make commands:

```bash
make config ENV_FILE=.env.dev
make build ENV_FILE=.env.dev
make init ENV_FILE=.env.dev
make up ENV_FILE=.env.dev
```

## Production commands

```bash
docker compose --env-file .env.prod config --quiet
docker compose --env-file .env.prod build
docker compose --env-file .env.prod up airflow-init
docker compose --env-file .env.prod up -d --wait
```

Equivalent Make commands:

```bash
make config ENV_FILE=.env.prod
make build ENV_FILE=.env.prod
make init ENV_FILE=.env.prod
make up ENV_FILE=.env.prod
```

## Verification

Use the same environment file for every command:

```bash
docker compose --env-file .env.prod ps

docker compose --env-file .env.prod exec -T airflow-scheduler \
  /bin/bash /opt/airflow/scripts/airflow-entrypoint.sh airflow version
docker compose --env-file .env.prod exec -T airflow-scheduler \
  /bin/bash /opt/airflow/scripts/airflow-entrypoint.sh airflow config lint
docker compose --env-file .env.prod exec -T airflow-scheduler \
  /bin/bash /opt/airflow/scripts/airflow-entrypoint.sh airflow db check
docker compose --env-file .env.prod exec -T airflow-scheduler \
  /bin/bash /opt/airflow/scripts/airflow-entrypoint.sh \
  airflow dags list-import-errors --output json

docker compose --env-file .env.prod exec -T airflow-scheduler \
  /bin/bash /opt/airflow/scripts/airflow-entrypoint.sh python -c \
  "from airflow.providers.mysql.hooks.mysql import MySqlHook; print(MySqlHook(mysql_conn_id='mysql').get_first('SELECT 1'))"

docker compose --env-file .env.prod exec -T airflow-scheduler \
  /bin/bash /opt/airflow/scripts/airflow-entrypoint.sh python -c \
  "from airflow.providers.postgres.hooks.postgres import PostgresHook; print(PostgresHook(postgres_conn_id='postgres').get_first('SELECT 1'))"
```

The wrapper is included in `exec` commands because a process started by
`docker compose exec` does not inherit variables exported by the container's
main process. It rebuilds the three generated Airflow connection variables
from the same simple component fields before running the requested command.

Keep business DAGs paused until row counts, connection targets, and catchup
behavior have been reviewed.

## Backup and rollback

Back up `airflow_metadata`, `ecommerce_dw`, `olist`, `/data/airflow/logs`, the
repository revision, and the active env file independently. To roll back,
restore the matching application revision, env file, and metadata backup.
