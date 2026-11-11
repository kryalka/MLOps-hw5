# MLOps-hw5

Мониторинг скоринга транзакций: `Kafka → CatBoost → Kafka → PostgreSQL`.
Код сервисов взят из [семинара](https://github.com/NikitaMalykhin/mlops_realtime_fraud_detector_monitoring),
настройка Kafka и зависимости Airflow — из [ДЗ №3](https://github.com/kryalka/MLOps-hw3).
Конфигурация Prometheus, дашборды и алерты добавлены в этом ДЗ.

## Запуск

Команды выполняются из корня проекта.
Положите `train.csv` с колонкой `target` из
[Kaggle](https://www.kaggle.com/competitions/teta-ml-1-2025/data) в `data/train.csv`.

```bash
docker compose up -d --build --wait --wait-timeout 900
docker compose ps -a
```

`postgres`, `kafka`, `airflow` должны стать `healthy`, а `data_loader`
и `kafka-setup` — завершиться с кодом 0. Настройка `.env` не требуется.
Симулятор непрерывно отправляет случайные транзакции из CSV каждые 0.1 с.

| Сервис | Адрес |
|---|---|
| Grafana | http://localhost:3000 — `admin` / `admin` |
| Prometheus | http://localhost:9090 |
| Метрики скоринга | http://localhost:8000/metrics |
| Метрики записи | http://localhost:8001/metrics |
| Airflow | http://localhost:8083 — `admin` / `admin` |
| Kafka UI | http://localhost:8084 |

## Дашборды

В Grafana: **Dashboards → Fraud monitoring**. Источники Prometheus и PostgreSQL,
дашборды и алерты подключаются автоматически через `grafana/provisioning/`.
Сбор метрик — раз в секунду, обновление графиков — каждые 5 с.

| Дашборд | Панели |
|---|---|
| [01 · Скоринг и транзакции](http://localhost:3000/d/fraud-scoring) | TPS, p95 обработки, доля фрода, распределение скоров, задержка потока, ошибки, смена модели |
| [02 · Запись результатов](http://localhost:3000/d/fraud-writer) | скорость и p95 записи, число фродов, распределение записанных скоров |
| [03 · Системные метрики](http://localhost:3000/d/fraud-system) | CPU, память, сеть, чтение и запись диска, заполнение файловых систем |

На дашборде скоринга также есть SQL-панели из PostgreSQL:

- Число скоров, доля фрода и средний скор по версиям модели.
- Плотность скоров и TPS с фильтрами по `us_state` и `merch`.
- Доля фрода по `cat_id` в последних 1000 транзакций.
- Топ-10 мерчантов по сумме платежей с флагом фрода — для выбора операций на ручную проверку.

Фильтры **Штат** и **Мерчант** применяются к SQL-панелям. Можно выбрать несколько
значений или All. Для панели по категориям сначала берутся последние 1000 строк,
затем применяются фильтры; выбор периода на неё не влияет.
Node Exporter на Docker Desktop показывает метрики Linux VM.

## Метрики

В скоринге:

- `transactions_total` — сколько транзакций обработано;
- `transaction_processing_seconds` — время обработки;
- `fraud_score` — распределение скоров;
- `fraud_transactions_total`, `fraud_ratio` — количество и доля фрода;
- `transaction_errors_total` — ошибки;
- `last_transaction_timestamp_seconds` — когда обработали последнюю транзакцию.

Для смены модели используются `model_version_updates_total{version}`,
`model_version_changed_timestamp_seconds{version}` и `active_model{version}`:
счётчик обновлений, время обновления и текущая версия.

Сервис записи результатов (`scoring_writer`) отдаёт `scorings_total` (число записей), `scoring_db_write_seconds`
(время записи), `fraud_detected_total` (число фродов) и `scoring_fraud_score`
(распределение записанных скоров).

Системные метрики берутся из Node Exporter: `node_cpu_seconds_total`,
`node_memory_*`, `node_network_*`, `node_disk_*`, `node_filesystem_*`.

Добавлена своя метрика `transaction_pipeline_lag_seconds`, которой нет в семинаре.
Она показывает задержку от отправки в Kafka до готового скора, включая очередь.
Смотреть на графике «Задержка от Kafka до скора: p95».

## Проверка

1. Откройте http://localhost:9090/targets — `scoring`, `writer`, `host` должны быть **UP**.
2. Выполните команды дважды с паузой в несколько секунд — значения должны расти:

   ```bash
   curl -s http://localhost:8000/metrics | grep '^transactions_total '
   curl -s http://localhost:8001/metrics | grep '^scorings_total '
   ```

3. Откройте дашборды. Для графиков скорости нужно накопить 30–60 с данных.
4. В Airflow запустите `retrain_catboost` через **Trigger DAG** и дождитесь успеха
   `publish_model`. На дашборде скоринга появится отметка **«Модель <версия>»**
   длительностью около 30 с. Новая версия также появится в SQL-таблице и счётчике:

   ```bash
   curl -s http://localhost:8000/metrics | grep '^model_version_updates_total'
   ```

Модель подхватывается без перезапуска скоринга. Оба DAG запускаются каждые 5 минут:
`retrain_catboost` переобучает CatBoost на CPU, `supply_labeled_batch` дописывает размеченные месяцы.

## Алерты

Правила: `grafana/provisioning/alerting/rules.yml`.
Проверка состояния: http://localhost:3000/alerting/list.

| Правило | Условие |
|---|---|
| Высокая доля фрода | доля флагов за минуту > 20% в течение 30 с |
| Остановился поток | нет обработки > 30 с, условие сохраняется ещё 30 с |

Для проверки остановите симулятор. Через 70–90 с после обработки очереди
алерт должен перейти в **Firing**, после запуска — вернуться в **Normal**.

```bash
docker compose stop transaction_simulator
docker compose start transaction_simulator
```

Для проверки доли фрода временно установите порог 0. Через 1–2 минуты проверьте
**Firing**, затем верните обычный порог 0.5 второй командой:

```bash
FRAUD_THRESHOLD=0 docker compose up -d --no-deps fraud_detector
docker compose up -d --no-deps fraud_detector
```

Остановка с сохранением данных: `docker compose down`.
Полный сброс: `docker compose down --volumes --remove-orphans`.
