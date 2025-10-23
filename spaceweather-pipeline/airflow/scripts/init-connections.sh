#!/usr/bin/env bash
set -euo pipefail

echo "[airflow-init] Ensuring required connections exist..."

create_conn() {
  local conn_id="$1"
  shift
  echo "[airflow-init] Refreshing connection '$conn_id'"
  airflow connections delete "$conn_id" >/dev/null 2>&1 || true
  airflow connections add "$conn_id" "$@" >/dev/null
}

create_conn ingestor_http --conn-type http --conn-host ingestor --conn-port 9000
create_conn dbt_runner_http --conn-type http --conn-host dbt-runner --conn-port 9001

echo "[airflow-init] Connection setup complete."
