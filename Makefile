PYTHON := .venv/bin/python
DBT := .venv/bin/dbt
DBT_PROJECT_DIR := dbt
DBT_PROFILES_DIR := dbt

EVENT ?= order_created
ORDER_ID ?= ord_1001
VERSION ?=
SOURCE_CHANNEL ?=
SERIALIZATION ?= json
SCHEMA_REGISTRY_URL ?= http://localhost:18081

POSTGRES_CONTAINER := contract_dwh_postgres
POSTGRES_USER := dwh
POSTGRES_DB := dwh

INTEGRATION_POSTGRES_ADMIN_DSN ?= \
	postgresql://dwh:dwh@localhost:55432/postgres
ANALYTICS_POSTGRES_DSN ?= \
	postgresql://dwh:dwh@localhost:55432/$(POSTGRES_DB)
ANALYTICS_RUN_INTERVAL_SECONDS ?= 300
ANALYTICS_TRIGGER ?= manual
ANALYTICS_FAIL_ON_WARNING ?= 0
ALERT_WEBHOOK_URL ?=
GRAFANA_PORT ?= 3000
GRAFANA_ADMIN_USER ?= admin
GRAFANA_ADMIN_PASSWORD ?= admin
GRAFANA_DB_USER ?= grafana_reader
GRAFANA_DB_PASSWORD ?= grafana
AIRFLOW_PORT ?= 8080
AIRFLOW_DAG_ID ?= contract_dwh_analytics
AIRFLOW_ANALYTICS_SCHEDULE ?= */5 * * * *


.PHONY: \
	up \
	bootstrap \
	down \
	reset \
	status \
	logs \
	logs-consumer \
	logs-redpanda \
	logs-topic-init \
	psql \
	check-contracts \
	generate-avro \
	check-avro-schemas \
	generate-ddl \
	init-raw \
	init-db \
	create-topics \
	wait-schema-registry \
	register-schemas \
	schema-registry-check \
	produce \
	produce-avro \
	produce-created \
	produce-paid \
	produce-cancelled \
	consume \
	consume-once \
	build-dds \
	build-mart \
	build-legacy-analytics \
	build-analytics \
	dbt-debug \
	dbt-parse \
	dbt-build \
	dbt-source-freshness \
	dbt-parity \
	verify-dbt-parity \
	dbt-docs \
	validate-monitoring-config \
	validate-airflow-config \
	analytics-run \
	analytics-run-strict \
	analytics-history \
	orchestration-up \
	orchestration-down \
	orchestration-logs \
	airflow-up \
	airflow-down \
	airflow-logs \
	airflow-status \
	airflow-health \
	airflow-dag-check \
	airflow-trigger \
	airflow-runs \
	airflow-e2e \
	init-monitoring-reader \
	monitoring-up \
	monitoring-airflow-up \
	monitoring-down \
	monitoring-logs \
	monitoring-status \
	monitoring-health \
	monitoring-check \
	rebuild-from-kafka \
	test \
	test-unit \
	test-integration \
	postgres-up \
	wait-postgres \
	ci \
	ci-logs \
	demo \
	e2e \
	e2e-avro \
	migrate


up:
	docker compose up -d --build


postgres-up:
	docker compose up -d postgres
	$(MAKE) wait-postgres


wait-postgres:
	@set -e; \
	attempt=1; \
	while ! docker compose exec -T postgres \
		pg_isready \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		>/dev/null 2>&1; do \
		if [ "$$attempt" -ge 30 ]; then \
			echo "PostgreSQL did not become ready"; \
			exit 1; \
		fi; \
		sleep 1; \
		attempt=$$((attempt + 1)); \
	done


bootstrap:
	docker compose up -d --build redpanda postgres
	$(MAKE) wait-postgres
	$(MAKE) wait-schema-registry
	$(MAKE) generate-avro
	$(MAKE) init-db
	docker compose up -d --build topic-init schema-init consumer
	@set -e; \
	attempt=1; \
	while ! docker compose logs --no-color consumer \
		2>/dev/null \
		| grep -q "Consumer assigned partitions"; do \
		if [ "$$attempt" -ge 60 ]; then \
			echo "Consumer did not receive a partition assignment"; \
			docker compose logs --tail=100 consumer; \
			exit 1; \
		fi; \
		sleep 1; \
		attempt=$$((attempt + 1)); \
	done


down:
	docker compose down


reset:
	docker compose down -v


status:
	docker compose ps -a


logs:
	docker compose logs -f


logs-consumer:
	docker compose logs -f consumer


logs-redpanda:
	docker compose logs -f redpanda


logs-topic-init:
	docker compose logs topic-init


psql:
	docker exec -it $(POSTGRES_CONTAINER) \
		psql \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB)


check-contracts:
	CONTRACTS_DIR=contracts \
	$(PYTHON) src/compatibility_checker.py


generate-avro: check-contracts
	CONTRACTS_DIR=contracts \
	$(PYTHON) src/avro_schema.py \
		--output-directory schemas/avro


check-avro-schemas: check-contracts
	CONTRACTS_DIR=contracts \
	$(PYTHON) src/avro_schema.py \
		--output-directory schemas/avro \
		--check


generate-ddl: check-contracts
	CONTRACTS_DIR=contracts \
	$(PYTHON) src/ddl_generator.py


init-raw: generate-ddl
	@set -e; \
	for file in sql/raw/*.sql; do \
		if [ -f "$$file" ]; then \
			echo "Applying $$file"; \
			docker exec -i $(POSTGRES_CONTAINER) \
				psql \
				-U $(POSTGRES_USER) \
				-d $(POSTGRES_DB) \
				-v ON_ERROR_STOP=1 \
				< "$$file"; \
		fi; \
	done


init-db: migrate


create-topics:
	KAFKA_BOOTSTRAP_SERVERS=localhost:19092 \
	CONTRACTS_DIR=contracts \
	$(PYTHON) src/topic_manager.py


wait-schema-registry:
	@set -e; \
	attempt=1; \
	while ! curl --fail --silent --show-error \
		$(SCHEMA_REGISTRY_URL)/subjects \
		>/dev/null 2>&1; do \
		if [ "$$attempt" -ge 30 ]; then \
			echo "Schema Registry did not become ready"; \
			exit 1; \
		fi; \
		sleep 1; \
		attempt=$$((attempt + 1)); \
	done


register-schemas: generate-avro wait-schema-registry
	CONTRACTS_DIR=contracts \
	SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	$(PYTHON) src/schema_registry_manager.py


schema-registry-check: wait-schema-registry
	CONTRACTS_DIR=contracts \
	SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	$(PYTHON) src/schema_registry_manager.py --check


produce:
	SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	$(PYTHON) src/producer.py \
		--event $(EVENT) \
		--order-id $(ORDER_ID) \
		--serialization $(SERIALIZATION) \
		$(if $(strip $(VERSION)),--version $(VERSION),) \
		$(if $(strip $(SOURCE_CHANNEL)),--source-channel $(SOURCE_CHANNEL),)


produce-avro:
	$(MAKE) produce SERIALIZATION=avro


produce-created:
	$(MAKE) produce \
		EVENT=order_created \
		ORDER_ID=$(ORDER_ID) \
		VERSION=$(VERSION) \
		SOURCE_CHANNEL=$(SOURCE_CHANNEL)


produce-paid:
	$(MAKE) produce \
		EVENT=order_paid \
		ORDER_ID=$(ORDER_ID) \
		VERSION=$(VERSION)


produce-cancelled:
	$(MAKE) produce \
		EVENT=order_cancelled \
		ORDER_ID=$(ORDER_ID) \
		VERSION=$(VERSION)


consume:
	SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	$(PYTHON) src/consumer.py


consume-once:
	SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	$(PYTHON) src/consumer.py --once


build-dds: migrate
	docker exec -i $(POSTGRES_CONTAINER) \
		psql \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		-v ON_ERROR_STOP=1 \
		< sql/dds/orders.sql


build-mart: migrate
	docker exec -i $(POSTGRES_CONTAINER) \
		psql \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		-v ON_ERROR_STOP=1 \
		< sql/mart/daily_orders.sql


build-legacy-analytics:
	$(MAKE) build-dds
	$(MAKE) build-mart


dbt-debug: postgres-up
	DBT_DATABASE=$(POSTGRES_DB) \
	$(DBT) debug \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)


dbt-parse:
	$(DBT) parse \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)


dbt-build: migrate
	DBT_DATABASE=$(POSTGRES_DB) \
	$(DBT) build \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR) \
		--exclude tag:parity


dbt-source-freshness: migrate
	DBT_DATABASE=$(POSTGRES_DB) \
	$(DBT) source freshness \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)


dbt-parity:
	DBT_DATABASE=$(POSTGRES_DB) \
	$(DBT) test \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR) \
		--select tag:parity


verify-dbt-parity:
	$(MAKE) build-legacy-analytics
	$(MAKE) dbt-build
	$(MAKE) dbt-parity


dbt-docs: postgres-up
	DBT_DATABASE=$(POSTGRES_DB) \
	$(DBT) docs generate \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)


validate-monitoring-config:
	$(PYTHON) src/monitoring_config_checker.py


validate-airflow-config:
	$(PYTHON) src/airflow_config_checker.py


analytics-run: migrate
	POSTGRES_DSN=$(ANALYTICS_POSTGRES_DSN) \
	DBT_DATABASE=$(POSTGRES_DB) \
	ALERT_WEBHOOK_URL=$(ALERT_WEBHOOK_URL) \
	$(PYTHON) src/analytics_runner.py \
		--dbt-executable $(DBT) \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR) \
		--trigger $(ANALYTICS_TRIGGER) $(if $(filter 1 true yes,$(ANALYTICS_FAIL_ON_WARNING)),--fail-on-warning,)


analytics-run-strict:
	$(MAKE) analytics-run ANALYTICS_FAIL_ON_WARNING=1


analytics-history: migrate
	docker exec -i $(POSTGRES_CONTAINER) \
		psql \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		-v ON_ERROR_STOP=1 \
		-c "SELECT \
			run_id, \
			trigger_type, \
			status, \
			freshness_status, \
			build_status, \
			total_nodes, \
			duration_seconds, \
			started_at \
		FROM analytics_run_history \
		ORDER BY started_at DESC \
		LIMIT 20;"


orchestration-up: bootstrap
	ANALYTICS_RUN_INTERVAL_SECONDS=$(ANALYTICS_RUN_INTERVAL_SECONDS) \
	ALERT_WEBHOOK_URL=$(ALERT_WEBHOOK_URL) \
	docker compose \
		--profile orchestration \
		up -d --build analytics-runner


orchestration-down:
	docker compose \
		--profile orchestration \
		stop analytics-runner


orchestration-logs:
	docker compose \
		--profile orchestration \
		logs -f analytics-runner


airflow-up: bootstrap validate-airflow-config
	@docker compose \
		--profile orchestration \
		stop analytics-runner >/dev/null 2>&1 || true
	@AIRFLOW_PORT=$(AIRFLOW_PORT) \
	AIRFLOW_ANALYTICS_SCHEDULE="$(AIRFLOW_ANALYTICS_SCHEDULE)" \
	ALERT_WEBHOOK_URL=$(ALERT_WEBHOOK_URL) \
	docker compose --profile airflow build airflow-init
	@AIRFLOW_PORT=$(AIRFLOW_PORT) \
	AIRFLOW_ANALYTICS_SCHEDULE="$(AIRFLOW_ANALYTICS_SCHEDULE)" \
	ALERT_WEBHOOK_URL=$(ALERT_WEBHOOK_URL) \
	docker compose --profile airflow up -d \
		airflow-api-server \
		airflow-scheduler \
		airflow-dag-processor
	@set -e; \
	attempt=1; \
	while ! curl --fail --silent --show-error \
		http://localhost:$(AIRFLOW_PORT)/api/v2/monitor/health \
		>/dev/null 2>&1; do \
		if [ "$$attempt" -ge 60 ]; then \
			echo "Airflow API server did not become ready"; \
			docker compose --profile airflow logs --tail=100 airflow-api-server; \
			exit 1; \
		fi; \
		sleep 2; \
		attempt=$$((attempt + 1)); \
	done
	@set -e; \
	attempt=1; \
	while ! docker compose exec -T airflow-scheduler \
		airflow dags list 2>/dev/null \
		| grep -q "$(AIRFLOW_DAG_ID)"; do \
		if [ "$$attempt" -ge 60 ]; then \
			echo "Airflow DAG was not discovered"; \
			docker compose --profile airflow logs --tail=100 airflow-dag-processor; \
			exit 1; \
		fi; \
		sleep 2; \
		attempt=$$((attempt + 1)); \
	done
	$(MAKE) airflow-dag-check
	@echo "Airflow UI: http://localhost:$(AIRFLOW_PORT)"


airflow-down:
	docker compose --profile airflow stop \
		airflow-dag-processor \
		airflow-scheduler \
		airflow-api-server \
		airflow-postgres


airflow-logs:
	docker compose --profile airflow logs -f \
		airflow-api-server \
		airflow-scheduler \
		airflow-dag-processor


airflow-status:
	docker compose --profile airflow ps
	$(MAKE) airflow-runs


airflow-health:
	@curl --fail --silent --show-error \
		http://localhost:$(AIRFLOW_PORT)/api/v2/monitor/health
	@echo


airflow-dag-check:
	docker compose exec -T airflow-scheduler \
		airflow dags list-import-errors
	@docker compose exec -T airflow-scheduler \
		airflow dags list \
		| grep "$(AIRFLOW_DAG_ID)"


airflow-trigger:
	docker compose exec -T airflow-scheduler \
		airflow dags trigger $(AIRFLOW_DAG_ID)


airflow-runs:
	docker compose exec -T airflow-scheduler \
		airflow dags list-runs $(AIRFLOW_DAG_ID) \
		--limit 10


airflow-e2e: airflow-up
	AIRFLOW_DAG_ID=$(AIRFLOW_DAG_ID) \
	AIRFLOW_PORT=$(AIRFLOW_PORT) \
	./scripts/airflow_e2e.sh


init-monitoring-reader:
	@docker exec \
		-e GRAFANA_READER_USER="$(GRAFANA_DB_USER)" \
		-e GRAFANA_READER_PASSWORD="$(GRAFANA_DB_PASSWORD)" \
		-i $(POSTGRES_CONTAINER) \
		psql \
		-X \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		-v ON_ERROR_STOP=1 \
		< monitoring/grafana/init_reader.sql


monitoring-up:
	$(MAKE) bootstrap
	$(MAKE) analytics-run
	$(MAKE) init-monitoring-reader
	@GRAFANA_PORT=$(GRAFANA_PORT) \
	GRAFANA_ADMIN_USER=$(GRAFANA_ADMIN_USER) \
	GRAFANA_ADMIN_PASSWORD=$(GRAFANA_ADMIN_PASSWORD) \
	GRAFANA_DB_USER=$(GRAFANA_DB_USER) \
	GRAFANA_DB_PASSWORD=$(GRAFANA_DB_PASSWORD) \
	ANALYTICS_RUN_INTERVAL_SECONDS=$(ANALYTICS_RUN_INTERVAL_SECONDS) \
	ALERT_WEBHOOK_URL=$(ALERT_WEBHOOK_URL) \
	docker compose \
		--profile orchestration \
		--profile monitoring \
		up -d --build analytics-runner grafana
	@echo "Grafana dashboard: http://localhost:$(GRAFANA_PORT)/d/contract-dwh-operations"


monitoring-airflow-up:
	$(MAKE) airflow-up
	$(MAKE) init-monitoring-reader
	@GRAFANA_PORT=$(GRAFANA_PORT) \
	GRAFANA_ADMIN_USER=$(GRAFANA_ADMIN_USER) \
	GRAFANA_ADMIN_PASSWORD=$(GRAFANA_ADMIN_PASSWORD) \
	GRAFANA_DB_USER=$(GRAFANA_DB_USER) \
	GRAFANA_DB_PASSWORD=$(GRAFANA_DB_PASSWORD) \
	docker compose \
		--profile monitoring \
		up -d --build grafana
	@echo "Airflow UI: http://localhost:$(AIRFLOW_PORT)"
	@echo "Grafana dashboard: http://localhost:$(GRAFANA_PORT)/d/contract-dwh-operations"


monitoring-down:
	docker compose \
		--profile orchestration \
		--profile monitoring \
		stop grafana analytics-runner


monitoring-logs:
	docker compose \
		--profile orchestration \
		--profile monitoring \
		logs -f grafana analytics-runner


monitoring-status:
	docker compose \
		--profile orchestration \
		--profile monitoring \
		ps


monitoring-health:
	docker exec -i $(POSTGRES_CONTAINER) \
		psql \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		-v ON_ERROR_STOP=1 \
		-c "SELECT \
			overall_health, \
			health_reason, \
			freshness_status, \
			build_status, \
			build_success_rate_24h, \
			dead_letter_count_24h, \
			dq_affected_orders, \
			orphan_order_count \
		FROM dbt_monitoring.monitor_pipeline_health;"


monitoring-check:
	@curl --fail --silent --show-error \
		http://localhost:$(GRAFANA_PORT)/api/health
	@echo
	$(MAKE) monitoring-health


build-analytics:
	$(MAKE) analytics-run


rebuild-from-kafka:
	@set -e; \
	echo "Stopping permanent consumer"; \
	docker compose stop consumer; \
	trap 'docker compose start consumer >/dev/null' EXIT; \
	echo "Truncating DWH tables and dbt schema"; \
	docker exec -i $(POSTGRES_CONTAINER) \
		psql \
		-U $(POSTGRES_USER) \
		-d $(POSTGRES_DB) \
		-v ON_ERROR_STOP=1 \
		-c "TRUNCATE \
			mart_daily_orders, \
			mart_watermarks, \
			dds_orders_changes, \
			dq_orphan_order_events, \
			dds_orders, \
			etl_watermarks, \
			raw_order_created, \
			raw_order_paid, \
			raw_order_cancelled, \
			dead_letter_events \
			RESTART IDENTITY; \
			ALTER SEQUENCE dds_orders_change_id_seq \
			RESTART WITH 1; \
			DROP SCHEMA IF EXISTS dbt CASCADE;"; \
	echo "Replaying Kafka topics"; \
	CONSUMER_GROUP_ID=contract-dwh-replay-$$(date +%s) \
	KAFKA_BOOTSTRAP_SERVERS=localhost:19092 \
	SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	POSTGRES_DSN=postgresql://dwh:dwh@localhost:55432/dwh \
	CONTRACTS_DIR=contracts \
	$(PYTHON) src/consumer.py --idle-timeout 5; \
	echo "Building DDS and MART"; \
	$(MAKE) build-analytics


test: test-unit


test-unit:
	$(PYTHON) -m pytest -v -m "not integration"


test-integration: postgres-up
	RUN_INTEGRATION_TESTS=1 \
	INTEGRATION_POSTGRES_ADMIN_DSN=$(INTEGRATION_POSTGRES_ADMIN_DSN) \
	$(PYTHON) -m pytest -v -m integration tests/integration


ci:
	$(MAKE) check-contracts
	$(MAKE) check-avro-schemas
	$(MAKE) validate-monitoring-config
	$(MAKE) validate-airflow-config
	$(MAKE) test-unit
	$(MAKE) test-integration
	$(MAKE) bootstrap
	$(MAKE) e2e
	$(MAKE) e2e-avro
	$(MAKE) airflow-e2e


ci-logs:
	docker compose ps -a
	docker compose logs --no-color --tail=300


demo:
	$(MAKE) produce-created \
		ORDER_ID=ord_demo_1 \
		VERSION=2 \
		SOURCE_CHANNEL=mobile_app

	$(MAKE) produce-paid \
		ORDER_ID=ord_demo_1

	$(MAKE) produce-created \
		ORDER_ID=ord_demo_2 \
		VERSION=1

	$(MAKE) produce-cancelled \
		ORDER_ID=ord_demo_2


e2e:
	./scripts/e2e_smoke.sh


e2e-avro: schema-registry-check
	EVENT_SERIALIZATION=avro \
	./scripts/e2e_smoke.sh

migrate: init-raw
	POSTGRES_CONTAINER=$(POSTGRES_CONTAINER) \
	POSTGRES_USER=$(POSTGRES_USER) \
	POSTGRES_DB=$(POSTGRES_DB) \
	./scripts/migrate.sh
