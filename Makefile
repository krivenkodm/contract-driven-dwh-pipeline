PYTHON := .venv/bin/python
TOPIC := ecommerce.order_created.v1

.PHONY: \
	up \
	down \
	status \
	logs \
	generate-ddl \
	init-db \
	create-topic \
	produce \
	produce-invalid \
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
	docker exec -i contract_dwh_postgres \
		psql -U dwh -d dwh \
		< sql/raw/generated_order_created.sql
	docker exec -i contract_dwh_postgres \
		psql -U dwh -d dwh \
		< sql/raw/dead_letter_events.sql

create-topic:
	docker exec contract_redpanda \
		rpk topic create $(TOPIC) \
		--brokers localhost:9092

produce:
	$(PYTHON) src/producer.py

produce-invalid:
	$(PYTHON) src/producer.py --invalid

consume:
	$(PYTHON) src/consumer.py

consume-once:
	$(PYTHON) src/consumer.py --once

psql:
	docker exec -it contract_dwh_postgres \
		psql -U dwh -d dwh