#!/usr/bin/env bash

set -euo pipefail

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-contract_dwh_postgres}"
POSTGRES_USER="${POSTGRES_USER:-dwh}"
POSTGRES_DB="${POSTGRES_DB:-dwh}"
AIRFLOW_DAG_ID="${AIRFLOW_DAG_ID:-contract_dwh_analytics}"

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
suffix="$(date -u +%Y%m%d%H%M%S)"
order_id="ord_airflow_${suffix}"
run_id="stage9_e2e_${suffix}"

echo "Producing a fresh event for ${order_id}"
make produce-created \
    ORDER_ID="${order_id}" \
    VERSION=2 \
    SOURCE_CHANNEL=airflow_e2e

attempt=1
until docker exec "${POSTGRES_CONTAINER}" \
    psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc \
    "SELECT EXISTS (
        SELECT 1
        FROM raw_order_created
        WHERE order_id = '${order_id}'
    );" | grep -q t; do
    if [[ "${attempt}" -ge 30 ]]; then
        echo "Fresh event did not reach PostgreSQL"
        exit 1
    fi
    sleep 1
    attempt=$((attempt + 1))
done

echo "Triggering Airflow run ${run_id}"
docker compose exec -T airflow-scheduler \
    airflow dags trigger \
    --run-id "${run_id}" \
    "${AIRFLOW_DAG_ID}"

attempt=1
while true; do
    state="$(
        docker compose exec -T airflow-scheduler \
            airflow dags state "${AIRFLOW_DAG_ID}" "${run_id}" \
            2>/dev/null \
            | tail -n 1 \
            | tr -d '\r'
    )"
    case "${state}" in
        success)
            echo "Airflow DAG run succeeded"
            break
            ;;
        failed)
            echo "Airflow DAG run failed"
            docker compose --profile airflow logs --tail=200 \
                airflow-scheduler airflow-dag-processor
            exit 1
            ;;
    esac
    if [[ "${attempt}" -ge 180 ]]; then
        echo "Timed out waiting for Airflow DAG run; last state: ${state:-unknown}"
        exit 1
    fi
    sleep 2
    attempt=$((attempt + 1))
done

analytics_status="$(
    docker exec "${POSTGRES_CONTAINER}" \
        psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc \
        "SELECT status
         FROM analytics_run_history
         WHERE trigger_type = 'airflow'
           AND started_at >= '${started_at}'::timestamptz
         ORDER BY started_at DESC
         LIMIT 1;"
)"

case "${analytics_status}" in
    success|warning)
        echo "Observed analytics_run_history status=${analytics_status}, trigger_type=airflow"
        ;;
    *)
        echo "Missing successful Airflow audit record; status=${analytics_status:-none}"
        exit 1
        ;;
esac

echo "Stage 9 Airflow E2E passed"
