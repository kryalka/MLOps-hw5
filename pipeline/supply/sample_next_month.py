"""Случайная выборка 100 тысяч строк и перенос их в следующий месяц."""

import calendar
import logging

import pandas as pd

from pipeline.db import LABELED_COLUMNS, get_conn
from pipeline.settings import SAMPLE_SIZE, SUPPLY_BATCH, TRAINING_DIR

logger = logging.getLogger(__name__)


def next_month_start(max_time: pd.Timestamp) -> pd.Timestamp:
    """Возвращает первое число месяца, следующего за месяцем max_time."""
    return (pd.Timestamp(max_time).to_period("M") + 1).to_timestamp()


def shift_into_month(frame: pd.DataFrame, month_start: pd.Timestamp) -> pd.DataFrame:
    """Переписывает transaction_time в заданный месяц, сохраняя день, час и минуту.

    Если в целевом месяце нет такого дня, день обрезается до последнего числа.
    Секунды обнуляются. Исходный frame не меняется.
    """
    shifted = frame.copy()
    times = pd.to_datetime(shifted["transaction_time"])
    year = int(month_start.year)
    month = int(month_start.month)
    last_day = calendar.monthrange(year, month)[1]
    shifted["transaction_time"] = pd.to_datetime(
        {
            "year": year,
            "month": month,
            "day": times.dt.day.clip(upper=last_day),
            "hour": times.dt.hour,
            "minute": times.dt.minute,
        }
    )
    return shifted


def sample_next_month() -> dict:
    """Сэмплирует строки витрины и готовит батч следующего календарного месяца.

    Из labeled_transactions случайно берётся до SAMPLE_SIZE наблюдений.
    Месяц батча — следующий после max(transaction_time). Времена переносятся
    в этот месяц, результат пишется в supply_batch.parquet.
    Витрину не изменяет. Возвращает путь к файлу и первое число месяца батча.
    """
    TRAINING_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT max(transaction_time), count(*) FROM labeled_transactions")
            max_time, row_count = cur.fetchone()
        if max_time is None or row_count == 0:
            raise RuntimeError("Витрина labeled_transactions пуста. Сначала должен отработать data_loader.")

        sample_size = min(SAMPLE_SIZE, row_count)
        columns = ", ".join(LABELED_COLUMNS)
        frame = pd.read_sql(
            f"""
            SELECT {columns}
            FROM labeled_transactions
            ORDER BY random()
            LIMIT %s
            """,
            conn,
            params=(sample_size,),
        )
    finally:
        conn.close()

    month_start = next_month_start(max_time)
    batch = shift_into_month(frame, month_start)
    batch.to_parquet(SUPPLY_BATCH, index=False)
    logger.info(
        "Батч %s: %s строк из %s, время %s — %s",
        month_start.strftime("%Y-%m"),
        len(batch),
        row_count,
        batch["transaction_time"].min(),
        batch["transaction_time"].max(),
    )
    return {
        "batch_path": str(SUPPLY_BATCH),
        "batch_month": month_start.date().isoformat(),
        "rows": int(len(batch)),
    }
