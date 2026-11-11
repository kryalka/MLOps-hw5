import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import Counter, Histogram, start_http_server

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
SCORE_BUCKETS = [i / 50.0 for i in range(51)]

DB_WRITE_TIME = Histogram('scoring_db_write_seconds', 'Время записи скоринга в БД',
                          buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5))
SCORING_COUNT = Counter('scorings_total', 'Общее количество записанных скорингов')
FRAUD_SCORE_HISTOGRAM = Histogram(
    'scoring_fraud_score',
    'Распределение записанных скоров мошенничества',
    buckets=SCORE_BUCKETS,
)
FRAUD_COUNT = Counter(
    'fraud_detected_total',
    'Количество обнаруженных мошеннических транзакций',
)


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scores (
                id SERIAL PRIMARY KEY,
                transaction_id TEXT NOT NULL UNIQUE,
                score FLOAT NOT NULL,
                fraud_flag INT NOT NULL,
                model_version TEXT NOT NULL,
                us_state TEXT NOT NULL,
                merch TEXT NOT NULL,
                cat_id TEXT NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS scores_created_at_idx ON scores (created_at);
            CREATE INDEX IF NOT EXISTS scores_state_merch_time_idx
                ON scores (us_state, merch, created_at);
        """)
        conn.commit()
        logger.info("Таблица 'scores' проверена или создана.")


def insert_score(conn, data):
    started = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO scores (transaction_id, score, fraud_flag, model_version,
                                us_state, merch, cat_id, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
            RETURNING id;
        """, (
            data["transaction_id"], data["score"], data["fraud_flag"], data["model_version"],
            data["us_state"], data["merch"], data["cat_id"], float(data["amount"]),
        ))
        inserted = cur.fetchone() is not None
        conn.commit()

    if inserted:
        DB_WRITE_TIME.observe(time.perf_counter() - started)
        SCORING_COUNT.inc()
        FRAUD_SCORE_HISTOGRAM.observe(data["score"])
        FRAUD_COUNT.inc(int(data["fraud_flag"]))
    return inserted


def run_consumer():
    start_http_server(METRICS_PORT)
    logger.info("Prometheus метрики доступны на порту %s", METRICS_PORT)

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    scoring_topic = os.getenv("KAFKA_SCORING_TOPIC")

    consumer_config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'scoring-writer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }

    logger.info(f"Подключение к Kafka: {kafka_bootstrap_servers}, топик: {scoring_topic}")
    consumer = Consumer(consumer_config)
    consumer.subscribe([scoring_topic])

    db_config = get_db_config()
    conn = psycopg2.connect(**db_config)
    create_table(conn)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                logger.debug("Ожидание сообщений...")
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                raise RuntimeError(str(msg.error()))

            try:
                value_str = msg.value().decode('utf-8')
                logger.debug(f"Получено сообщение: {value_str}")
                data_list = json.loads(value_str)

                # Проверяем, что это список
                if isinstance(data_list, list):
                    for data in data_list:
                        required_keys = ['transaction_id', 'score', 'fraud_flag', 'model_version',
                                         'us_state', 'merch', 'cat_id', 'amount']
                        if not all(k in data for k in required_keys):
                            raise ValueError("В сообщении не хватает полей")
                        insert_score(conn, data)
                        logger.info(f"Записано в БД: {data['transaction_id']}")
                else:
                    raise ValueError("Ожидался список скоров")
                offsets = consumer.commit(message=msg, asynchronous=False)
                if not offsets or any(offset.error is not None for offset in offsets):
                    raise RuntimeError("Не сохранён offset Kafka")

            except json.JSONDecodeError as je:
                logger.exception(f"Ошибка декодирования JSON: {je}")
                raise
            except Exception as e:
                conn.rollback()
                logger.exception(f"Ошибка обработки сообщения: {e}")
                raise

    except KeyboardInterrupt:
        logger.info("Потребитель остановлен пользователем.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    logger.info("Запуск потребителя и писателя в БД...")
    run_consumer()
