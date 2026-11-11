"""Дозапись подготовленного месячного батча в размеченную витрину."""

import logging
from pathlib import Path

import pandas as pd

from pipeline.db import copy_labeled_frame, get_conn

logger = logging.getLogger(__name__)


def append_labeled_batch(batch_info: dict) -> dict:
    """Дописывает parquet батча в конец labeled_transactions.

    Читает путь из batch_info. Существующие строки не обновляет и не удаляет.
    Возвращает тот же словарь с числом записанных строк, чтобы следующая
    задача знала, какой месяц только что появился в витрине.
    """
    batch_path = Path(batch_info["batch_path"])
    if not batch_path.exists():
        raise FileNotFoundError(f"Нет файла батча для загрузки: {batch_path}")

    frame = pd.read_parquet(batch_path)
    conn = get_conn()
    try:
        copy_labeled_frame(conn, frame)
    finally:
        conn.close()
    logger.info(
        "Месяц %s добавлен в витрину, строк: %s",
        batch_info["batch_month"],
        len(frame),
    )
    return {
        "batch_path": batch_info["batch_path"],
        "batch_month": batch_info["batch_month"],
        "rows": int(len(frame)),
    }
