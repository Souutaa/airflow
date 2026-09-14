ENV_FILE ?= .env.dev
COMPOSE := docker compose --env-file $(ENV_FILE)
AIRFLOW_EXEC := $(COMPOSE) exec -T airflow-scheduler /bin/bash /opt/airflow/scripts/airflow-entrypoint.sh

.PHONY: config build init up down ps logs verify

config:
	$(COMPOSE) config --quiet

build:
	$(COMPOSE) build

init:
	$(COMPOSE) up airflow-init

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --tail 200

verify:
	$(AIRFLOW_EXEC) airflow version
	$(AIRFLOW_EXEC) airflow config lint
	$(AIRFLOW_EXEC) airflow db check
	$(AIRFLOW_EXEC) airflow dags list-import-errors --output json
