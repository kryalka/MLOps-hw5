"""Подключение к витрине Postgres и состояние окна обучения."""

import io
import logging
import os
from datetime import date

import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

LABELED_COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merchant_lat",
    "merchant_lon",
    "target",
]


def get_conn():
    """Открывает соединение с базой, где лежит витрина labeled_transactions.

    Хост, порт, имя базы и учётные данные читаются из переменных окружения
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def month_end_exclusive(month_start: date) -> date:
    """Возвращает первое число месяца, следующего за month_start.

    Верхняя граница не включается в выборку месяца, который начинается в month_start.
    """
    return month_start + relativedelta(months=1)


def read_training_state(conn) -> dict:
    """Читает границы трёхмесячного окна и месяц следующего батча обучения.

    Ожидает строку id = 1 в training_state, которую создаёт data_loader.
    Возвращает словарь с ключами window_start, window_end и next_batch_month.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT window_start, window_end, next_batch_month
            FROM training_state
            WHERE id = 1
            """
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Таблица training_state пуста. Сначала должен отработать data_loader.")
    return {
        "window_start": row[0],
        "window_end": row[1],
        "next_batch_month": row[2],
    }


def advance_training_state(conn, batch_month: date) -> None:
    """Сдвигает окно обучения на месяц успешно опубликованного батча.

    Новое окно заканчивается batch_month и покрывает три месяца.
    next_batch_month становится следующим календарным месяцем.
    Вызывать только после записи модели, чтобы повтор упавшего прогона
    не пропускал месяц.
    """
    window_end = batch_month
    window_start = batch_month - relativedelta(months=2)
    next_batch = month_end_exclusive(batch_month)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE training_state
            SET window_start = %s,
                window_end = %s,
                next_batch_month = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (window_start, window_end, next_batch),
        )
    conn.commit()
    logger.info(
        "Состояние обновлено: окно %s — %s, следующий батч %s",
        window_start,
        window_end,
        next_batch,
    )


def copy_labeled_frame(conn, frame: pd.DataFrame) -> None:
    """Дописывает строки в конец labeled_transactions через COPY.

    frame должен содержать колонки LABELED_COLUMNS. transaction_time
    записывается как timestamp. Транзакция фиксируется в конце загрузки.
    """
    payload = frame.loc[:, LABELED_COLUMNS].copy()
    payload["transaction_time"] = pd.to_datetime(payload["transaction_time"])
    buffer = io.StringIO()
    payload.to_csv(
        buffer,
        index=False,
        header=False,
        date_format="%Y-%m-%d %H:%M:%S",
    )
    buffer.seek(0)
    column_list = ", ".join(LABELED_COLUMNS)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY labeled_transactions ({column_list}) FROM STDIN WITH (FORMAT CSV)",
            buffer,
        )
    conn.commit()
    logger.info("В labeled_transactions дописано %s строк.", len(payload))
