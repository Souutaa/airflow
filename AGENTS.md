# AGENTS.md

## 1. Project Purpose

This repository is an Apache Airflow based ETL/reporting project.

Primary responsibilities:

- Extract data from multiple source databases.
- Transform, clean, normalize, and map source data.
- Load processed data into the reporting database.
- Maintain ETL checkpoints.
- Maintain ETL job execution history.
- Refresh or expose SQL views used by reporting applications.
- Execute scheduled pipelines, primarily daily jobs.

Airflow is responsible for orchestration, scheduling, retries, dependencies, monitoring, and task execution state.

Airflow metadata must not be treated as the source of truth for business ETL state.

---

# 2. Target Technology Stack

Current target stack:

```text
Python              3.11.x
Apache Airflow      3.3.1
Executor            LocalExecutor

Airflow metadata:
PostgreSQL          16

Source / Business:
MySQL               8.x

Staging / Warehouse:
PostgreSQL          16

Deployment:
Airflow             Docker Compose
Databases           External/native services

Production OS:
Linux
```

Do not change these versions or architecture decisions unless explicitly requested.

Do not use:

```text
CeleryExecutor
Redis
KubernetesExecutor
```

unless explicitly requested.

---

# 3. Repository Source of Truth

Before making changes:

1. Inspect the repository.
2. Read this `AGENTS.md`.
3. Read relevant files under `.ai/`.
4. Inspect current implementation.
5. Inspect Docker configuration.
6. Inspect DAGs and dependencies.
7. Check Git working tree.

Do not assume repository structure.

Existing code and actual configuration are the implementation source of truth.

Documentation under `.ai/` provides architectural intent and task-specific instructions.

If documentation conflicts with actual code, report the conflict before making destructive changes.

---

# 4. `.ai` Documentation

Use `.ai/` for project documentation and task instructions.

Recommended structure:

```text
.ai/
├── architecture/
├── plans/
├── prompts/
└── archive/
```

Examples:

```text
.ai/prompts/airflow-2.9.2-to-3.3.1-migration.md
.ai/architecture/etl-architecture.md
.ai/architecture/checkpoint-strategy.md
```

`AGENTS.md` contains permanent repository rules.

Task-specific implementation instructions belong under:

```text
.ai/prompts/
```

Do not put temporary migration plans or one-time task instructions into `AGENTS.md`.

---

# 5. Git Safety

Before modifying files, always inspect:

```bash
git status
```

Never use destructive Git commands unless explicitly requested.

Do not run:

```bash
git reset --hard
git clean -fd
git checkout .
```

without explicit permission.

Do not overwrite unrelated user changes.

If the working tree is dirty:

- identify existing changes;
- preserve them;
- modify only files required for the current task.

Do not create commits unless explicitly requested.

---

# 6. Database Safety

Database data is critical.

Never:

- delete databases;
- reset databases;
- delete Docker volumes;
- drop schemas;
- drop tables;
- truncate production/business tables;
- recreate databases destructively;

unless explicitly requested.

Do not use destructive commands such as:

```bash
docker compose down -v
```

without explicit permission.

Do not use:

```bash
airflow db reset
```

unless explicitly requested.

Always prefer migration and backward-compatible changes over destructive recreation.

---

# 7. Database Persistence

Database lifecycle must be independent from the Airflow Docker container
lifecycle.

The current deployment uses PostgreSQL and MySQL services outside the Airflow
Compose project:

```text
Airflow containers
    ↓
real database host/IP/DNS
    ↓
native PostgreSQL/MySQL storage and backup lifecycle
```

Airflow containers may be recreated without affecting database storage. The
database servers must have their own persistent storage, backup, monitoring,
and restore procedures. Airflow logs may still use a host bind mount configured
through `AIRFLOW_LOG_DIR`.

If a database is containerized again in the future, use an explicit host bind
mount for critical data and never use an anonymous Docker volume.

Do not move or replace an existing database data directory without first determining whether it contains data that must be preserved.

---

# 8. Airflow Metadata Database

PostgreSQL is reserved for Airflow metadata unless otherwise documented.

Airflow metadata includes operational state such as:

- DAG runs;
- task instances;
- scheduling state;
- retry state;
- XCom;
- Airflow connections;
- variables;
- Airflow configuration-related state.

Airflow metadata is not the business data store.

Do not store business ETL checkpoints exclusively in Airflow metadata.

---

# 9. Business ETL State

Business ETL state must remain independent from Airflow's internal state.

Use dedicated business/report tables where appropriate, such as:

```text
etl_checkpoint
etl_job_run
```

Typical checkpoint information:

```text
pipeline_name
source_name
entity_name
checkpoint_value
last_success_at
updated_at
```

A checkpoint represents:

```text
how far business data has successfully been processed
```

It does NOT represent:

```text
whether an Airflow task happened to execute
```

Only advance a checkpoint after the corresponding business data has been successfully committed.

Never advance checkpoints before successful load/commit.

---

# 10. ETL Pipeline Principles

Preferred logical pipeline:

```text
Source
  ↓
Extract
  ↓
Normalize
  ↓
Validate
  ↓
Transform
  ↓
Mapping
  ↓
Load
  ↓
Refresh / expose report view
  ↓
Update checkpoint
```

When useful, use staging:

```text
Source
  ↓
Staging
  ↓
Transform
  ↓
Report tables
  ↓
Views
```

Keep extract, transform, load, validation, and checkpoint responsibilities clearly separated.

Avoid creating one large task that performs the complete ETL pipeline unless the pipeline is genuinely trivial.

---

# 11. Idempotency

ETL jobs should be safe to retry whenever reasonably possible.

Design tasks to be idempotent.

Avoid behavior where rerunning the same Airflow task causes:

- duplicate report records;
- corrupted checkpoints;
- duplicated business events;
- inconsistent state.

Prefer techniques such as:

```text
UPSERT
MERGE
unique constraints
business keys
batch IDs
transaction boundaries
```

where appropriate.

---

# 12. Transaction Safety

Checkpoint updates and business data commits must be ordered safely.

Do not implement:

```text
extract
→ update checkpoint
→ load
```

Prefer:

```text
extract
→ transform
→ validate
→ load
→ commit
→ update checkpoint
```

If a transaction can safely include both business load and checkpoint update, consider doing so.

Do not claim exactly-once processing unless implementation actually guarantees it.

---

# 13. Airflow Responsibilities

Airflow should handle:

```text
scheduling
orchestration
dependencies
retry
monitoring
task execution state
```

Business logic should not become unnecessarily coupled to Airflow internals.

Where practical, ETL business functions should remain independently testable Python code.

Avoid putting all transformation logic directly inside DAG definitions.

DAG files should primarily describe orchestration.

---

# 14. DAG Design

Prefer small and understandable DAGs.

A typical daily pipeline may resemble:

```text
start
  ↓
read_checkpoint
  ↓
extract
  ↓
transform
  ↓
validate
  ↓
load_report
  ↓
refresh_view
  ↓
save_checkpoint
  ↓
finish
```

Independent extracts may run in parallel when safe.

Example:

```text
                ┌─ extract_source_a ─┐
read_checkpoint ├─ extract_source_b ─┼─ transform
                └─ extract_source_c ─┘
```

Do not parallelize tasks merely for appearance.

Consider source database load and transaction isolation before increasing concurrency.

---

# 15. XCom Rules

Do not use Airflow XCom as a data transport layer for large datasets.

Do not pass large:

```text
pandas DataFrame
CSV contents
JSON datasets
binary data
```

through XCom.

XCom should contain small metadata such as:

```text
job_id
batch_id
checkpoint
row_count
staging_table
file_path
business_date
```

Large data should be persisted in an appropriate medium such as:

```text
database staging tables
files
Parquet
object storage
```

depending on the system architecture.

---

# 16. SQL and Database Access

Never build SQL using unsafe string concatenation when values originate outside trusted static code.

Use:

- parameterized SQL;
- SQLAlchemy;
- Airflow database hooks/providers;
- maintained database drivers.

Database credentials must not be hardcoded inside DAGs.

Prefer Airflow Connections or environment/secret based configuration.

Connection IDs should have clear names, for example:

```text
source_mysql
report_mysql
transaction_mssql
```

---

# 17. Secrets

Never commit real secrets.

Do not hardcode:

```text
passwords
database credentials
API keys
Fernet keys
secret keys
tokens
```

Use environment variables or an approved secret mechanism.

`.env`, `.env.dev`, and `.env.prod` must not be committed.

Maintain:

```text
.env.example
```

with safe placeholders.

Ensure `.gitignore` contains:

```text
.env
.env.dev
.env.prod
```

Never replace an existing persistent Fernet key without determining its impact on encrypted Airflow data.

---

# 18. Docker Rules

Use pinned versions.

Do not use:

```dockerfile
FROM apache/airflow:latest
```

Use an explicit version, currently:

```dockerfile
FROM apache/airflow:3.3.1-python3.11
```

Do not install production dependencies every time containers start.

Avoid relying on:

```text
_PIP_ADDITIONAL_REQUIREMENTS
```

for the final environment.

Install required packages during image build.

Keep Docker Compose understandable and minimal.

Do not introduce Redis, Celery, Kubernetes, or additional databases without a demonstrated requirement.

---

# 19. Dependencies

Only add dependencies that are required.

Before adding a package:

1. Check whether the standard library already solves the problem.
2. Check whether an existing project dependency already solves it.
3. Confirm compatibility with Python 3.11 and Airflow 3.3.1.
4. Prefer maintained packages.

For Airflow integrations, prefer official Airflow providers where appropriate.

Examples:

```text
apache-airflow-providers-common-sql
apache-airflow-providers-mysql
apache-airflow-providers-postgres
apache-airflow-providers-microsoft-mssql
```

Do not install providers that are not required.

---

# 20. Python Code Quality

Target Python:

```text
Python 3.11.x
```

Use:

- type hints where they improve clarity;
- small focused functions;
- clear naming;
- explicit error handling;
- context managers for resources;
- structured logging.

Avoid:

- giant functions;
- hidden global state;
- duplicated mapping logic;
- broad `except Exception` without meaningful handling;
- silent failures.

Do not swallow database or ETL errors merely to make a DAG appear successful.

---

# 21. Pandas Usage

Use Pandas only where it provides clear value.

Do not load very large tables entirely into memory without considering volume.

For large datasets prefer:

```text
chunked reads
batch processing
database-side transformation
staging tables
bulk inserts
```

Do not assume Pandas is always the correct ETL engine.

Push transformations to SQL when SQL is clearer, more efficient, and maintains correct semantics.

---

# 22. Logging

Use logging instead of `print()` for production ETL logic.

Logs should provide useful operational context such as:

```text
pipeline
task
source
batch
checkpoint
row counts
duration
error category
```

Never log:

```text
passwords
tokens
connection strings containing credentials
sensitive data
```

Avoid logging entire large datasets.

---

# 23. Error Handling

Failures must be visible.

If extraction, validation, mapping, loading, or checkpoint operations fail:

- raise an appropriate error;
- allow Airflow to mark the task as failed;
- preserve enough context for diagnosis.

Do not convert a real ETL failure into `SUCCESS`.

Retries should be used for transient errors, not to hide deterministic bugs.

---

# 24. Validation

Before marking work complete, run relevant checks.

For Docker-related changes, use when applicable:

```bash
docker compose config
docker compose build
docker compose up
docker compose ps
```

For Airflow-related changes, verify:

```text
Airflow starts
scheduler is healthy
API server is healthy
DAG processor is healthy
metadata DB is reachable
DAG parsing errors = 0
```

For DAG changes:

- confirm the DAG imports;
- confirm dependencies are correct;
- run relevant unit/smoke tests;
- test representative task execution where practical.

For persistence changes:

- verify data survives container recreation.

Do not declare success only because code compiles.

---

# 25. Testing

New ETL business logic should be testable outside Airflow where practical.

Prefer unit tests for:

```text
mapping
normalization
validation
checkpoint calculations
business transformations
```

Integration tests should cover database boundaries where valuable.

Bug fixes should include regression tests when practical.

---

# 26. Scope Control

Do not rewrite unrelated code while working on a specific task.

Prefer:

```text
small
reviewable
reversible
well-scoped
```

changes.

Infrastructure migration and ETL refactoring should generally be separate operations.

For example:

```text
Airflow 2 → Airflow 3 migration
```

should not automatically trigger:

```text
complete rewrite of all ETL business logic
```

unless compatibility requires it.

---

# 27. Existing Behavior

Preserve working business behavior unless the task explicitly changes requirements.

When refactoring:

- identify existing behavior;
- preserve contracts;
- document intentional behavior changes.

Do not infer that old code is wrong merely because its design can be improved.

---

# 28. Destructive Changes

Before any destructive or potentially irreversible operation:

1. Explain what will be changed.
2. Identify affected data.
3. Determine whether backup is required.
4. Prefer a reversible alternative.

Examples requiring special caution:

```text
database migration
storage path changes
volume migration
schema drops
major Airflow upgrades
Fernet key changes
database driver changes
```

---

# 29. Upgrade Strategy

For framework or infrastructure upgrades:

```text
inspect
→ identify compatibility issues
→ backup
→ migrate infrastructure
→ fix incompatible code
→ verify
→ report
```

Do not perform blind version bumps.

For major Airflow upgrades, inspect:

```text
DAG API
configuration
CLI
authentication
metadata migrations
providers
plugins
Docker services
```

before implementation.

---

# 30. Reporting Changes

After substantial work, provide a concise report containing:

```text
Findings
Files changed
Architecture changes
Commands/tests run
Verification results
Remaining risks
Manual actions required
```

Clearly distinguish:

```text
verified
not verified
assumed
```

Never claim something was tested if it was not actually tested.

---

# 31. Current Architectural Decisions

Unless explicitly changed, treat these as project decisions:

```text
Python                3.11.x
Airflow               3.3.1
Executor              LocalExecutor

Airflow metadata DB   PostgreSQL 16
Source/business DB    MySQL 8.x
Staging/warehouse DB  PostgreSQL 16

Airflow deployment    Docker Compose
Database deployment   External/native services
Environment selection .env.dev / .env.prod

Redis                 Not required
CeleryExecutor        Not required
Kubernetes            Not required
```

Business ETL checkpoint state must remain separate from Airflow metadata state.

---

# 32. Agent Working Method

For non-trivial tasks:

1. Read `AGENTS.md`.
2. Read task-specific `.ai` documentation.
3. Inspect relevant repository files.
4. Check Git state.
5. Produce findings.
6. Implement the smallest safe change.
7. Run verification.
8. Report results.

Do not blindly follow stale documentation when repository evidence proves otherwise.

Do not silently change architecture decisions.

If a blocker is discovered, report:

```text
Finding
Evidence
Impact
Recommended action
```

before attempting a risky workaround.
