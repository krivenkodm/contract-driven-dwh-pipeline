PYTHON := .venv/bin/python

TOPICS := \
	ecommerce.order_created.v1 \
	ecommerce.order_paid.v1 \
	ecommerce.order_cancelled.v1

EVENT ?= order_created
ORDER_ID ?= ord_1001

.PHONY: \
	up \
	down \
	status \
	logs \
	generate-ddl \
	init-db \
	create-topics \
	produce \
	produce-invalid \
	produce-created \
	produce-paid \
	produce-cancelled \
	consume \
	consume-once \
	psql

up:
	docker compose up -d

down:
	docker compose down

status:
	docker compose ps

logs:
	docker compose logs --tail=100

generate-ddl:
	$(PYTHON) src/ddl_generator.py

init-db: generate-ddl
	@for file in sql/raw/generated_*.sql; do \
		echo "Applying $$file"; \
		docker exec -i contract_dwh_postgres \
			psql -U dwh -d dwh < $$file; \
	done
	docker exec -i contract_dwh_postgres \
		psql -U dwh -d dwh \
		< sql/raw/dead_letter_events.sql

create-topics:
	@for topic in $(TOPICS); do \
		echo "Creating topic $$topic"; \
		docker exec contract_redpanda \
			rpk topic create $$topic \
			--brokers localhost:9092 || true; \
	done

produce:
	$(PYTHON) src/producer.py \
		--event $(EVENT) \
		--order-id $(ORDER_ID)

produce-invalid:
	$(PYTHON) src/producer.py \
		--event $(EVENT) \
		--order-id $(ORDER_ID) \
		--invalid

produce-created:
	$(MAKE) produce \
		EVENT=order_created \
		ORDER_ID=ord_1001

produce-paid:
	$(MAKE) produce \
		EVENT=order_paid \
		ORDER_ID=ord_1001

produce-cancelled:
	$(MAKE) produce \
		EVENT=order_cancelled \
		ORDER_ID=ord_1002

consume:
	$(PYTHON) src/consumer.py

consume-once:
	$(PYTHON) src/consumer.py --once

psql:
	docker exec -it contract_dwh_postgres \
		psql -U dwh -d dwh

build-dds:
	docker exec -i contract_dwh_postgres \
		psql -U dwh -d dwh \
		< sql/dds/orders.sql

build-mart:
	docker exec -i contract_dwh_postgres \
		psql -U dwh -d dwh \
		< sql/mart/daily_orders.sql

build-analytics:
	$(MAKE) build-dds
	$(MAKE) build-mart