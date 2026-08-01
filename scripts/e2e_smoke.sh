#!/usr/bin/env bash

set -Eeuo pipefail


POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-contract_dwh_postgres}"
POSTGRES_USER="${POSTGRES_USER:-dwh}"
POSTGRES_DB="${POSTGRES_DB:-dwh}"

WAIT_ATTEMPTS="${WAIT_ATTEMPTS:-30}"
WAIT_DELAY_SECONDS="${WAIT_DELAY_SECONDS:-1}"

ORDER_ID="${ORDER_ID:-ord_e2e_$(date +%Y%m%d%H%M%S)_$$}"
INVALID_ORDER_ID="${ORDER_ID}_invalid"
E2E_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
EVENT_SERIALIZATION="${EVENT_SERIALIZATION:-json}"

if [[ ! "${ORDER_ID}" =~ ^[a-zA-Z0-9_-]+$ ]]; then
    echo "ERROR: invalid ORDER_ID: ${ORDER_ID}"
    exit 1
fi

if [[ "${EVENT_SERIALIZATION}" != "json" \
    && "${EVENT_SERIALIZATION}" != "avro" ]]; then
    echo "ERROR: unsupported EVENT_SERIALIZATION: ${EVENT_SERIALIZATION}"
    exit 1
fi

psql_scalar() {
    docker exec -i "${POSTGRES_CONTAINER}" \
        psql \
        -U "${POSTGRES_USER}" \
        -d "${POSTGRES_DB}" \
        -v ON_ERROR_STOP=1 \
        -Atq \
        -c "$1" \
        | tr -d '[:space:]'
}


require_running_service() {
    local service="$1"

    if ! docker compose ps \
        --services \
        --status running \
        | grep -qx "${service}"; then

        echo "ERROR: service is not running: ${service}"
        docker compose ps -a
        exit 1
    fi
}


wait_for_raw_event() {
    local table="$1"
    local attempt
    local rows_count

    for ((attempt = 1; attempt <= WAIT_ATTEMPTS; attempt++)); do
        rows_count="$(
            psql_scalar "
                SELECT COUNT(*)
                FROM ${table}
                WHERE order_id = '${ORDER_ID}';
            "
        )"

        if [[ "${rows_count}" -ge 1 ]]; then
            echo "Found ${ORDER_ID} in ${table}"
            return
        fi

        echo \
            "Waiting for ${ORDER_ID} in ${table}" \
            "(${attempt}/${WAIT_ATTEMPTS})..."

        sleep "${WAIT_DELAY_SECONDS}"
    done

    echo "ERROR: event did not reach ${table}"
    echo
    echo "Consumer logs:"
    docker compose logs --tail=100 consumer
    exit 1
}


wait_for_dlq_event() {
    local attempt
    local rows_count

    for ((attempt = 1; attempt <= WAIT_ATTEMPTS; attempt++)); do
        if [[ "${EVENT_SERIALIZATION}" == "avro" ]]; then
            rows_count="$(
                psql_scalar "
                    SELECT COUNT(*)
                    FROM dead_letter_events
                    WHERE kafka_topic = 'ecommerce.order_created.v2'
                      AND event_payload IS NULL
                      AND error_message LIKE
                            'Message is not valid Avro:%'
                      AND load_dttm >=
                            '${E2E_STARTED_AT}'::timestamptz;
                "
            )"
        else
            rows_count="$(
                psql_scalar "
                    SELECT COUNT(*)
                    FROM dead_letter_events
                    WHERE event_payload ->> 'order_id'
                        = '${INVALID_ORDER_ID}';
                "
            )"
        fi

        if [[ "${rows_count}" -ge 1 ]]; then
            echo "Found ${INVALID_ORDER_ID} in dead_letter_events"
            return
        fi

        echo \
            "Waiting for ${INVALID_ORDER_ID} in dead_letter_events" \
            "(${attempt}/${WAIT_ATTEMPTS})..."

        sleep "${WAIT_DELAY_SECONDS}"
    done

    echo "ERROR: invalid event did not reach dead_letter_events"
    docker compose logs --tail=100 consumer
    exit 1
}


echo "========================================"
echo "E2E smoke test"
echo "Order ID: ${ORDER_ID}"
echo "Serialization: ${EVENT_SERIALIZATION}"
echo "========================================"

echo
echo "[1/8] Checking infrastructure"

require_running_service redpanda
require_running_service postgres
require_running_service consumer

echo "Infrastructure is running"

echo
echo "[2/8] Producing order_created"

make produce-created \
    ORDER_ID="${ORDER_ID}" \
    SERIALIZATION="${EVENT_SERIALIZATION}"

echo
echo "[3/8] Producing order_paid"

make produce-paid \
    ORDER_ID="${ORDER_ID}" \
    SERIALIZATION="${EVENT_SERIALIZATION}"

echo
echo "[4/8] Producing invalid order_created"

if [[ "${EVENT_SERIALIZATION}" == "avro" ]]; then
    .venv/bin/python src/producer.py \
        --event order_created \
        --order-id "${INVALID_ORDER_ID}" \
        --serialization avro \
        --corrupt-avro
else
    .venv/bin/python src/producer.py \
        --event order_created \
        --order-id "${INVALID_ORDER_ID}" \
        --serialization json \
        --invalid
fi

echo
echo "[5/8] Waiting for RAW and DLQ events"

wait_for_raw_event raw_order_created
wait_for_raw_event raw_order_paid
wait_for_dlq_event

echo
echo "[6/8] Checking contract provenance and payload retention"

valid_raw_provenance_count="$(
    psql_scalar "
        SELECT COUNT(*)
        FROM raw_order_created
        WHERE order_id = '${ORDER_ID}'
          AND contract_name = 'order_created'
          AND contract_version = 2
          AND original_payload ->> 'order_id' = '${ORDER_ID}';
    "
)"

if [[ "${valid_raw_provenance_count}" -ne 1 ]]; then
    echo "ERROR: order_created RAW provenance is invalid"
    exit 1
fi

paid_raw_provenance_count="$(
    psql_scalar "
        SELECT COUNT(*)
        FROM raw_order_paid
        WHERE order_id = '${ORDER_ID}'
          AND contract_name = 'order_paid'
          AND contract_version = 1
          AND original_payload ->> 'order_id' = '${ORDER_ID}';
    "
)"

if [[ "${paid_raw_provenance_count}" -ne 1 ]]; then
    echo "ERROR: order_paid RAW provenance is invalid"
    exit 1
fi

if [[ "${EVENT_SERIALIZATION}" == "avro" ]]; then
    valid_dlq_provenance_count="$(
        psql_scalar "
            SELECT COUNT(*)
            FROM dead_letter_events
            WHERE kafka_topic = 'ecommerce.order_created.v2'
              AND contract_name = 'order_created'
              AND contract_version = 2
              AND event_payload IS NULL
              AND raw_payload IS NOT NULL
              AND get_byte(raw_payload, 0) = 0
              AND error_message LIKE
                    'Message is not valid Avro:%'
              AND load_dttm >=
                    '${E2E_STARTED_AT}'::timestamptz;
        "
    )"
else
    valid_dlq_provenance_count="$(
        psql_scalar "
            SELECT COUNT(*)
            FROM dead_letter_events
            WHERE event_payload ->> 'order_id' = '${INVALID_ORDER_ID}'
              AND contract_name = 'order_created'
              AND contract_version = 2
              AND raw_payload IS NOT NULL
              AND convert_from(raw_payload, 'UTF8')::jsonb
                    ->> 'order_id' = '${INVALID_ORDER_ID}';
        "
    )"
fi

if [[ "${valid_dlq_provenance_count}" -ne 1 ]]; then
    echo "ERROR: DLQ provenance or raw payload is invalid"
    exit 1
fi

echo "RAW and DLQ provenance is valid"

echo
echo "[7/8] Building DDS and MART"

make build-analytics

echo
echo "[8/8] Checking dbt DDS and MART"

dds_rows_count="$(
    psql_scalar "
        SELECT COUNT(*)
        FROM dbt.dds_orders
        WHERE order_id = '${ORDER_ID}';
    "
)"

if [[ "${dds_rows_count}" -lt 1 ]]; then
    echo "ERROR: ${ORDER_ID} was not found in dbt.dds_orders"
    exit 1
fi

echo "Found ${ORDER_ID} in dbt.dds_orders"

valid_dds_state_count="$(
    psql_scalar "
        SELECT COUNT(*)
        FROM dbt.dds_orders
        WHERE order_id = '${ORDER_ID}'
          AND status = 'paid'
          AND payment_currency = currency
          AND dq_multiple_payments_flg = FALSE
          AND dq_multiple_cancellations_flg = FALSE
          AND dq_payment_amount_mismatch_flg = FALSE
          AND dq_payment_currency_mismatch_flg = FALSE
          AND dq_payment_before_creation_flg = FALSE
          AND dq_cancellation_before_creation_flg = FALSE
          AND dq_payment_after_cancellation_flg = FALSE;
    "
)"

if [[ "${valid_dds_state_count}" -ne 1 ]]; then
    echo "ERROR: ${ORDER_ID} has unexpected dbt DDS or DQ state"
    exit 1
fi

echo "dbt DDS status and DQ flags are valid"

mart_rows_count="$(
    psql_scalar "
        SELECT COUNT(*)
        FROM dbt.mart_daily_orders;
    "
)"

if [[ "${mart_rows_count}" -lt 1 ]]; then
    echo "ERROR: dbt.mart_daily_orders is empty"
    exit 1
fi

echo "dbt.mart_daily_orders contains ${mart_rows_count} row(s)"

observed_run_count="$(
    psql_scalar "
        SELECT COUNT(*)
        FROM analytics_run_history
        WHERE trigger_type = 'manual'
          AND status = 'success'
          AND freshness_status = 'pass'
          AND build_status = 'success'
          AND started_at >= '${E2E_STARTED_AT}'::timestamptz;
    "
)"

if [[ "${observed_run_count}" -lt 1 ]]; then
    echo "ERROR: successful observed analytics run was not recorded"
    exit 1
fi

echo "Observed analytics run was recorded successfully"

echo
echo "dbt DDS result:"

docker exec -i "${POSTGRES_CONTAINER}" \
    psql \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -v ON_ERROR_STOP=1 \
    -x \
    -c "
        SELECT *
        FROM dbt.dds_orders
        WHERE order_id = '${ORDER_ID}';
    "

echo
echo "dbt MART result:"

docker exec -i "${POSTGRES_CONTAINER}" \
    psql \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -v ON_ERROR_STOP=1 \
    -c "
        SELECT *
        FROM dbt.mart_daily_orders
        ORDER BY 1 DESC
        LIMIT 5;
    "

echo
echo "========================================"
echo "E2E TEST PASSED"
echo "Order ID: ${ORDER_ID}"
echo "========================================"
