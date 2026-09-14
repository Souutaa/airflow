# Airflow 3.3.1 migration runbook

> Historical execution record: this documents the completed container-database
> migration. The current deployment now uses one Airflow-only Compose file and
> external database hosts. See `deployment/server/README.md` for current
> development and production commands.

## Scope

This migration upgrades orchestration from Airflow 2.11.2 to Airflow 3.3.1 on
Python 3.11 with `LocalExecutor`. PostgreSQL 16 remains the Airflow metadata
database. MySQL 8 remains the source database.

The existing `de_psql` service is retained temporarily because the current ETL
stores all `staging` and `warehouse` tables there. Moving these schemas to MySQL
is a separate business-data migration and must not be combined with the Airflow
upgrade.

## Pre-upgrade backup

Stop all Airflow components before the final metadata dump. Back up:

- the Airflow PostgreSQL database with `pg_dump --format=custom`;
- the MySQL database with `mysqldump --single-transaction --no-tablespaces`;
- the warehouse PostgreSQL database with `pg_dump --format=custom`;
- `.env`, `docker-compose.yaml`, `Dockerfile`, `requirements.txt`, `dags/`,
  `plugins/`, and `.ai/`.

Validate PostgreSQL dumps with `pg_restore --list`. Confirm that the MySQL dump
ends with its completion marker. Store checksums with the backup.

Do not delete the old named volumes. They are the rollback source until the
bind-mounted deployment and ETL smoke test have both passed.

## Host directories

Create the absolute directories configured in `.env`:

```text
AIRFLOW_POSTGRES_DATA_DIR
MYSQL_DATA_DIR
WAREHOUSE_POSTGRES_DATA_DIR
AIRFLOW_LOG_DIR
```

On Linux, a suitable layout is:

```text
/data/airflow/postgres
/data/airflow/mysql
/data/airflow/warehouse-postgres
/data/airflow/logs
```

## Restore before framework migration

1. Start only `postgres`, `mysql`, and `de_psql` with empty bind mounts.
2. Restore the Airflow 2.11.2 metadata dump into the metadata PostgreSQL.
3. Restore MySQL and warehouse PostgreSQL dumps into their respective services.
4. Verify row/table counts and stop all Airflow components.
5. Build the Airflow 3.3.1 image.
6. Run `docker compose up airflow-init`; the image entrypoint executes the
   non-destructive `airflow db migrate` command.
7. Confirm that Alembic is at the Airflow 3.3.1 head before starting the API
   server, scheduler, and DAG processor.

## Verification

```bash
docker compose config
docker compose build
docker compose up -d
docker compose ps
docker compose run --rm airflow-cli airflow version
docker compose run --rm airflow-cli python --version
docker compose run --rm airflow-cli airflow config get-value core executor
docker compose run --rm airflow-cli airflow dags list
docker compose run --rm airflow-cli airflow dags list-import-errors
```

Run the `hello_airflow` DAG as the smoke test. Then recreate each database
container without deleting its host directory and re-check the pre-recorded
database marker/table counts.

## Execution record (2026-09-14)

Validated backups are stored outside version control under
`.migration-backups/20260914-pre-airflow-3.3.1/`. Their SHA-256 checksums are:

```text
airflow-metadata.dump          6142FD7359DC386BAD46317CAB6108ED7BCB7A5C952FB0B93B9F802541D7D580
business-warehouse.dump        A88A3D0A26DED652EE8D165C1DA49CCCEA563698683EFCF0DF018ED222111AF9
mysql-business.sql             81CFF8526E2F9F939E506BC58B48838AD5F0EA34A1446A0546E4D4A84EAACCA5
repository-config-and-code.zip 55A210FC1F661F0E07ADCDEAD4C16E6D140CFE896B3A4E3B3B9CF19F00BBFFA6
```

The completed verification established:

- Airflow `3.3.1`, Python `3.11.15`, `LocalExecutor`, and metadata revision
  `d2f4e1b3c5a7`;
- API server, scheduler, DAG processor, metadata PostgreSQL, MySQL, and
  warehouse PostgreSQL health checks all passing;
- three DAGs parsed with zero import errors;
- the `hello_airflow` smoke DagRun completed successfully;
- Airflow provider hooks connected successfully to both configured business
  databases;
- MySQL contains 9 tables and 99,441 `orders` rows;
- warehouse PostgreSQL contains 9 staging tables, 7 warehouse tables, and
  118,434 `warehouse.fact_orders` rows;
- all recorded values survived forced recreation of the three database
  containers.

The restored `e_commerce_dw_etl` DAG is deliberately left paused. Its preserved
`catchup=True` behavior exposed an outstanding historical backlog through
2026-04-20 during verification. All 16 remaining active historical runs were
marked failed without deleting their history; there are no running, queued, or
scheduled task instances. All nine staging row counts were also checked against
MySQL. Review the historical backlog and source load policy before unpausing the
DAG.

## Rollback

Stop the Airflow 3 components. Point the prior Compose configuration back to the
unchanged named volumes and restore the pre-upgrade `.env`/source snapshot.
Never attempt to run Airflow 2 against metadata already migrated to Airflow 3;
use the pre-upgrade metadata dump or unchanged old volume instead.
