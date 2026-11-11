import io
import logging
import os
import time
from pathlib import Path

import pandas as pd
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("data_loader")

CSV_PATH = Path(os.getenv("LABELED_CSV_PATH", "/data/train.csv"))
TRAINING_DIR = Path(os.getenv("TRAINING_DATA_DIR", "/training_data"))
CURRENT_DATASET = TRAINING_DIR / "current.parquet"

COLUMNS = [
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

# Исходное окно обучения: январь–март 2019 включительно.
INITIAL_WINDOW_START = "2019-01-01"
INITIAL_WINDOW_END = "2019-03-01"
INITIAL_NEXT_BATCH = "2019-04-01"


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def connect():
    config = get_db_config()
    last_error = None
    for attempt in range(1, 31):
        try:
            return psycopg2.connect(**config)
        except psycopg2.OperationalError as exc:
            last_error = exc
            logger.info("Postgres ещё не готов (%s/30): %s", attempt, exc)
            time.sleep(2)
    raise last_error


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS labeled_transactions (
                transaction_time TIMESTAMP NOT NULL,
                merch TEXT,
                cat_id TEXT,
                amount DOUBLE PRECISION,
                name_1 TEXT,
                name_2 TEXT,
                gender TEXT,
                street TEXT,
                one_city TEXT,
                us_state TEXT,
                post_code INTEGER,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                population_city INTEGER,
                jobs TEXT,
                merchant_lat DOUBLE PRECISION,
                merchant_lon DOUBLE PRECISION,
                target SMALLINT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS training_state (
                id INT PRIMARY KEY,
                window_start DATE NOT NULL,
                window_end DATE NOT NULL,
                next_batch_month DATE NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    conn.commit()
    logger.info("Схема витрины проверена.")


def ensure_time_index(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS labeled_transactions_time_idx
            ON labeled_transactions (transaction_time);
            """
        )
    conn.commit()


def row_count(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM labeled_transactions")
        return cur.fetchone()[0]


def load_csv():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Не найден файл размеченных данных: {CSV_PATH}")
    logger.info("Чтение %s", CSV_PATH)
    frame = pd.read_csv(CSV_PATH)
    missing = [column for column in COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"В CSV нет колонок: {missing}")
    frame = frame[COLUMNS].copy()
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"])
    logger.info(
        "CSV прочитан: %s строк, время с %s по %s",
        len(frame),
        frame["transaction_time"].min(),
        frame["transaction_time"].max(),
    )
    return frame


def copy_frame(conn, frame):
    buffer = io.StringIO()
    frame.to_csv(
        buffer,
        index=False,
        header=False,
        date_format="%Y-%m-%d %H:%M:%S",
    )
    buffer.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            """
            COPY labeled_transactions (
                transaction_time, merch, cat_id, amount, name_1, name_2, gender,
                street, one_city, us_state, post_code, lat, lon, population_city,
                jobs, merchant_lat, merchant_lon, target
            )
            FROM STDIN WITH (FORMAT CSV)
            """,
            buffer,
        )
    conn.commit()
    logger.info("В витрину labeled_transactions загружено %s строк.", len(frame))


def ensure_state(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM training_state WHERE id = 1")
        if cur.fetchone()[0]:
            logger.info("training_state уже заполнен, указатель месяца не сбрасывается.")
            return
        cur.execute(
            """
            INSERT INTO training_state (id, window_start, window_end, next_batch_month)
            VALUES (1, %s, %s, %s)
            """,
            (INITIAL_WINDOW_START, INITIAL_WINDOW_END, INITIAL_NEXT_BATCH),
        )
    conn.commit()
    logger.info(
        "Стартовое окно обучения: %s — %s, первый батч DAG: %s",
        INITIAL_WINDOW_START,
        INITIAL_WINDOW_END,
        INITIAL_NEXT_BATCH,
    )


def read_state(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT window_start, window_end
            FROM training_state
            WHERE id = 1
            """
        )
        window_start, window_end = cur.fetchone()
    return window_start, window_end


def materialize_window(conn):
    TRAINING_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(TRAINING_DIR, 0o777)
    if CURRENT_DATASET.exists():
        logger.info("%s уже существует, исходный датасет не перезаписывается.", CURRENT_DATASET)
        return

    window_start, window_end = read_state(conn)
    end_exclusive = pd.Timestamp(window_end) + pd.DateOffset(months=1)
    frame = pd.read_sql(
        """
        SELECT transaction_time, merch, cat_id, amount, name_1, name_2, gender,
               street, one_city, us_state, post_code, lat, lon, population_city,
               jobs, merchant_lat, merchant_lon, target
        FROM labeled_transactions
        WHERE transaction_time >= %s AND transaction_time < %s
        ORDER BY transaction_time
        """,
        conn,
        params=(window_start, end_exclusive.to_pydatetime()),
    )
    frame.to_parquet(CURRENT_DATASET, index=False)
    os.chmod(CURRENT_DATASET, 0o666)
    logger.info(
        "Исходный датасет обучения: %s строк, %s — %s, файл %s",
        len(frame),
        frame["transaction_time"].min() if len(frame) else None,
        frame["transaction_time"].max() if len(frame) else None,
        CURRENT_DATASET,
    )


def main():
    conn = connect()
    try:
        ensure_schema(conn)
        if row_count(conn) == 0:
            copy_frame(conn, load_csv())
        else:
            logger.info("Витрина labeled_transactions уже заполнена, повторная загрузка пропущена.")
        ensure_time_index(conn)
        ensure_state(conn)
        materialize_window(conn)
    finally:
        conn.close()
    logger.info("Загрузка витрины завершена.")


if __name__ == "__main__":
    main()
