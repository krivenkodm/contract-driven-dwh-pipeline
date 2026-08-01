PYTHON := .venv/bin/python
DBT := .venv/bin/dbt
DBT_PROJECT_DIR := dbt
DBT_PROFILES_DIR := dbt

EVENT ?= order_created
ORDER_ID ?= ord_1001
VERSION ?=
SOURCE_CHANNEL ?=

POSTGRES_CONTAINER := contract_dwh_postgres
POSTGRES_USER := dwh
POSTGRES_DB := dwh

INTEGRATION_POSTGRES_ADMIN_DSN ?= \
	postgresql://dwh:dwh@localhost:55432/postgres


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
	generate-ddl \
	init-raw \
	init-db \
	create-topics \
	produce \
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
	dbt-parity \
	dbt-docs \
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
	$(MAKE) init-db
	docker compose up -d --build topic-init consumer
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


produce:
	$(PYTHON) src/producer.py \
		--event $(EVENT) \
		--order-id $(ORDER_ID) \
		$(if $(strip $(VERSION)),--version $(VERSION),) \
		$(if $(strip $(SOURCE_CHANNEL)),--source-channel $(SOURCE_CHANNEL),)


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
	$(PYTHON) src/consumer.py


consume-once:
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


dbt-parity:
	DBT_DATABASE=$(POSTGRES_DB) \
	$(DBT) test \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR) \
		--select tag:parity


dbt-docs: postgres-up
	DBT_DATABASE=$(POSTGRES_DB) \
	$(DBT) docs generate \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)


build-analytics:
	$(MAKE) build-legacy-analytics
	$(MAKE) dbt-build
	$(MAKE) dbt-parity


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
	$(MAKE) test-unit
	$(MAKE) test-integration
	$(MAKE) bootstrap
	$(MAKE) e2e


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

migrate: init-raw
	POSTGRES_CONTAINER=$(POSTGRES_CONTAINER) \
	POSTGRES_USER=$(POSTGRES_USER) \
	POSTGRES_DB=$(POSTGRES_DB) \
	./scripts/migrate.sh
