"""Бесконечный поток транзакций в Kafka из размеченного CSV."""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("transaction_simulator")

CSV_PATH = Path(os.getenv("TRANSACTIONS_CSV_PATH", "/data/train.csv"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", "0.1"))
TIME_FORMAT = "%Y-%m-%d %H:%M"


def load_transactions(path: Path) -> pd.DataFrame:
    """Читает размеченный CSV один раз и держит его в памяти для случайной выборки."""
    if not path.exists():
        raise FileNotFoundError(f"Не найден файл транзакций: {path}")
    frame = pd.read_csv(path)
    if "transaction_time" not in frame.columns:
        raise ValueError("В CSV нет колонки transaction_time")
    logger.info("Загружено %s строк из %s", len(frame), path)
    return frame


def json_safe(value):
    """Приводит значение ячейки pandas к типу, который сериализуется в JSON."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def build_message(row: pd.Series) -> dict:
    """Собирает сообщение топика transactions: новый id и текущее время события."""
    data = {column: json_safe(value) for column, value in row.items()}
    data["transaction_time"] = datetime.now().strftime(TIME_FORMAT)
    return {
        "transaction_id": str(uuid.uuid4()),
        "produced_at": time.time(),
        "data": data,
    }


def connect_producer() -> KafkaProducer:
    """Ждёт брокер и открывает продюсер с JSON-сериализацией."""
    last_error = None
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                security_protocol="PLAINTEXT",
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            logger.info("Kafka ещё не готова (%s/30): %s", attempt, exc)
            time.sleep(2)
    raise last_error


def stream_transactions(frame: pd.DataFrame, producer: KafkaProducer) -> None:
    """Без остановки шлёт случайные строки в Kafka с паузой SEND_INTERVAL_SECONDS."""
    row_count = len(frame)
    sent = 0
    while True:
        row = frame.iloc[random.randrange(row_count)]
        message = build_message(row)
        producer.send(KAFKA_TOPIC, value=message)
        producer.flush()
        sent += 1
        if sent % 100 == 0:
            logger.info(
                "Отправлено %s сообщений, последнее время %s",
                sent,
                message["data"]["transaction_time"],
            )
        time.sleep(SEND_INTERVAL_SECONDS)


def main() -> None:
    """Загружает CSV и запускает бесконечную поставку транзакций."""
    frame = load_transactions(CSV_PATH)
    producer = connect_producer()
    logger.info(
        "Старт потока: топик %s, брокер %s, интервал %s с",
        KAFKA_TOPIC,
        KAFKA_BROKERS,
        SEND_INTERVAL_SECONDS,
    )
    try:
        stream_transactions(frame, producer)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
