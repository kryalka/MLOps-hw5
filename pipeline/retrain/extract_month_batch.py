"""Извлечение очередного месяца из размеченной витрины."""

import logging

import pandas as pd
from airflow.exceptions import AirflowSkipException

from pipeline.db import LABELED_COLUMNS, get_conn, month_end_exclusive, read_training_state
from pipeline.settings import BATCHES_DIR

logger = logging.getLogger(__name__)


def extract_month_batch() -> dict:
    """Забирает из labeled_transactions месяц, указанный в training_state.

    Читает next_batch_month, выбирает наблюдения этого календарного месяца
    и сохраняет их в parquet. Указатель месяца не меняет.
    Возвращает путь к файлу батча и месяц в формате ISO.
    Если строк нет, помечает задачу как skipped: новый месяц ещё не поставлен.
    """
    BATCHES_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        state = read_training_state(conn)
        batch_month = state["next_batch_month"]
        batch_end = month_end_exclusive(batch_month)
        logger.info(
            "Текущее окно %s — %s. Извлекаем месяц %s.",
            state["window_start"],
            state["window_end"],
            batch_month,
        )
        columns = ", ".join(LABELED_COLUMNS)
        frame = pd.read_sql(
            f"""
            SELECT {columns}
            FROM labeled_transactions
            WHERE transaction_time >= %s AND transaction_time < %s
            ORDER BY transaction_time
            """,
            conn,
            params=(batch_month, batch_end),
        )
    finally:
        conn.close()

    if frame.empty:
        raise AirflowSkipException(
            f"В витрине нет наблюдений за {batch_month}. Новый месяц ещё не поставлен."
        )

    batch_path = BATCHES_DIR / f"batch_{batch_month.strftime('%Y%m')}.parquet"
    frame.to_parquet(batch_path, index=False)
    logger.info(
        "Батч %s: %s строк, %s — %s",
        batch_month,
        len(frame),
        frame["transaction_time"].min(),
        frame["transaction_time"].max(),
    )
    return {
        "batch_path": str(batch_path),
        "batch_month": batch_month.isoformat(),
    }
