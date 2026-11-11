#!/usr/bin/env bash
set -euo pipefail

mkdir -p \
  /opt/airflow/models \
  /opt/airflow/training_data \
  /opt/airflow/airflow_state \
  /opt/airflow/logs \
  /opt/airflow/dags

# Каталоги общие с data_loader и fraud_detector, которые работают от root.
chmod 777 \
  /opt/airflow/models \
  /opt/airflow/training_data \
  /opt/airflow/airflow_state \
  /opt/airflow/logs

export PYTHONPATH="/opt/airflow${PYTHONPATH:+:$PYTHONPATH}"
export AIRFLOW__CORE__EXECUTOR="${AIRFLOW__CORE__EXECUTOR:-SequentialExecutor}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-sqlite:////opt/airflow/airflow_state/airflow.db}"
export AIRFLOW__CORE__LOAD_EXAMPLES="${AIRFLOW__CORE__LOAD_EXAMPLES:-false}"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="${AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION:-false}"

AIRFLOW_USER_NAME="${_AIRFLOW_WWW_USER_USERNAME:-admin}"
AIRFLOW_USER_PASSWORD="${_AIRFLOW_WWW_USER_PASSWORD:-admin}"

# В образе нет gosu: официальный entrypoint не понижает права, контейнер стартует от root.
run_as_airflow() {
  /usr/sbin/runuser --preserve-environment --user airflow -- "$@"
}

# Официальный entrypoint создаёт пользователя при каждом старте и падает, если он уже есть.
unset _AIRFLOW_DB_MIGRATE
unset _AIRFLOW_WWW_USER_CREATE

run_as_airflow airflow db migrate
run_as_airflow airflow users create \
  --username "${AIRFLOW_USER_NAME}" \
  --password "${AIRFLOW_USER_PASSWORD}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  || true

exec /usr/sbin/runuser --preserve-environment --user airflow -- /entrypoint standalone
