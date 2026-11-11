"""Очистка витрины: хранить только последние три года."""

import logging
from datetime import date

from dateutil.relativedelta import relativedelta

from pipeline.db import get_conn
from pipeline.settings import HISTORY_YEARS

logger = logging.getLogger(__name__)


def prune_old_months(batch_info: dict) -> dict:
    """Удаляет наблюдения старше трёх лет относительно самого нового месяца.

    Граница считается от начала месяца max(transaction_time) после дозаписи
    батча. Строки с transaction_time строго раньше этой границы удаляются.
    Пока горизонт витрины короче трёх лет, удалять нечего.
    Возвращает месяц батча, границу хранения и число удалённых строк.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT date_trunc('month', max(transaction_time))::date
                FROM labeled_transactions
                """
            )
            newest_month = cur.fetchone()[0]
            if newest_month is None:
                raise RuntimeError("Нечего чистить: витрина labeled_transactions пуста.")
            cutoff = newest_month - relativedelta(years=HISTORY_YEARS)
            cur.execute(
                "DELETE FROM labeled_transactions WHERE transaction_time < %s",
                (cutoff,),
            )
            deleted = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "Глубина хранения %s лет от %s. Удалено строк старше %s: %s",
        HISTORY_YEARS,
        newest_month,
        cutoff,
        deleted,
    )
    return {
        "batch_month": batch_info["batch_month"],
        "newest_month": newest_month.isoformat() if isinstance(newest_month, date) else str(newest_month),
        "cutoff": cutoff.isoformat(),
        "deleted_rows": int(deleted),
    }
