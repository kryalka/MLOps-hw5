import json
import logging
import os
import sys
import time

import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

sys.path.append(os.path.abspath('./src'))  # noqa: PTH100
from preprocessing import run_preproc
from scorer import ensure_bootstrap, make_pred


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
SCORE_BUCKETS = [i / 50.0 for i in range(51)]

PROCESSING_TIME = Histogram(
    'transaction_processing_seconds',
    'Время обработки транзакции',
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
TRANSACTION_COUNT = Counter(
    'transactions_total',
    'Общее количество обработанных транзакций',
)
FRAUD_SCORE = Histogram(
    'fraud_score',
    'Распределение скоров мошенничества',
    buckets=SCORE_BUCKETS,
)
FRAUD_RATIO = Gauge(
    'fraud_ratio',
    'Соотношение мошеннических транзакций к общему числу',
)
MODEL_VERSION_UPDATES = Counter(
    'model_version_updates_total',
    'Смена версии модели при горячей подмене',
    ['version'],
)

FRAUD_COUNT = Counter('fraud_transactions_total', 'Транзакции с fraud_flag=1')
LAST_TRANSACTION = Gauge('last_transaction_timestamp_seconds', 'Время последней обработанной транзакции')
PIPELINE_LATENCY = Histogram(
    'transaction_pipeline_lag_seconds', 'От отправки в Kafka до публикации скора',
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
MODEL_CHANGED_AT = Gauge(
    'model_version_changed_timestamp_seconds', 'Когда скоринг подхватил версию', ['version'],
)
MODEL_INFO = Gauge('active_model', 'Текущая версия модели', ['version'])
PROCESSING_ERRORS = Counter('transaction_errors_total', 'Ошибки обработки сообщений')


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)
        self.total_transactions = 0
        self.fraud_transactions = 0
        self._seen_model_version = None
        LAST_TRANSACTION.set_to_current_time()
        start_http_server(METRICS_PORT)
        logger.info("Prometheus метрики доступны на порту %s", METRICS_PORT)

    def _record_model_version(self, model_version):
        if self._seen_model_version is None:
            self._seen_model_version = model_version
            MODEL_INFO.labels(version=model_version).set(1)
            MODEL_VERSION_UPDATES.labels(version=model_version).inc(0)
            return
        if model_version != self._seen_model_version:
            MODEL_VERSION_UPDATES.labels(version=model_version).inc()
            MODEL_CHANGED_AT.labels(version=model_version).set_to_current_time()
            MODEL_INFO.labels(version=self._seen_model_version).set(0)
            MODEL_INFO.labels(version=model_version).set(1)
            logger.info(
                "Модель сменилась: %s -> %s",
                self._seen_model_version,
                model_version,
            )
            self._seen_model_version = model_version

    def process_message(self, msg):
        started = time.perf_counter()
        try:
            # Десериализация JSON
            data = json.loads(msg.value().decode('utf-8'))

            # Извлекаем ID и данные
            transaction_id = data['transaction_id']
            input_df = pd.DataFrame([data['data']])

            # Препроцессинг и предсказание
            processed_df = run_preproc(input_df)
            submission, y_proba, model_version = make_pred(processed_df, "kafka_stream")

            score = float(y_proba[0])
            fraud_flag = int(submission["fraud_flag"].iloc[0])
            submission['transaction_id'] = transaction_id
            submission['model_version'] = model_version
            for column in ["us_state", "merch", "cat_id", "amount"]:
                submission[column] = data["data"][column]

            delivered = {"ok": False, "error": None}

            def on_delivery(error, _message):
                delivered["ok"] = error is None
                delivered["error"] = error

            self.producer.produce(
                SCORING_TOPIC,
                key=transaction_id,
                value=submission.to_json(orient='records'),
                on_delivery=on_delivery,
            )
            remaining = self.producer.flush(30)
            if remaining or not delivered["ok"]:
                raise RuntimeError(f"Не доставлен скор: {delivered['error']}")
            offsets = self.consumer.commit(message=msg, asynchronous=False)
            if not offsets or any(offset.error is not None for offset in offsets):
                raise RuntimeError("Не сохранён offset Kafka")

            TRANSACTION_COUNT.inc()
            FRAUD_SCORE.observe(score)
            FRAUD_COUNT.inc(fraud_flag)
            self.total_transactions += 1
            self.fraud_transactions += fraud_flag
            FRAUD_RATIO.set(self.fraud_transactions / self.total_transactions)
            LAST_TRANSACTION.set_to_current_time()
            if "produced_at" in data:
                PIPELINE_LATENCY.observe(max(0, time.time() - float(data["produced_at"])))
            PROCESSING_TIME.observe(time.perf_counter() - started)
            self._record_model_version(model_version)
            return True
        except Exception as e:
            PROCESSING_ERRORS.inc()
            logger.exception(f"Error processing message: {e}")
            return False

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                raise RuntimeError(str(msg.error()))

            if not self.process_message(msg):
                raise RuntimeError("Ошибка скоринга, offset не пропускаем")


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    ensure_bootstrap()
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
    finally:
        service.producer.flush(30)
        service.consumer.close()
