"""Публикация бинарника модели и фиксация нового окна обучения."""

import json
import logging
import os
from datetime import date
from pathlib import Path

from pipeline.db import advance_training_state, get_conn
from pipeline.settings import CURRENT_DATASET, MODELS_DIR

logger = logging.getLogger(__name__)


def publish_model(train_info: dict) -> str:
    """Кладёт модель в общий каталог и только потом сдвигает указатель месяца.

    Переименовывает черновик .cbm в my_catboost_<version>.cbm, пишет метрики
    и атомарно обновляет latest.txt. Затем заменяет current.parquet новым окном
    и обновляет training_state. Повтор задачи не сдвигает окно второй раз,
    если pending-файл уже перенесён.
    Возвращает id опубликованной версии.
    """
    version = train_info["version"]
    staging_model = Path(train_info["staging_model"])
    final_model = MODELS_DIR / f"my_catboost_{version}.cbm"
    metrics_path = MODELS_DIR / f"my_catboost_{version}.metrics.json"

    if staging_model.exists():
        os.replace(staging_model, final_model)
    elif not final_model.exists():
        raise FileNotFoundError(f"Нет файла модели для публикации: {staging_model}")

    metrics_path.write_text(
        json.dumps(
            {
                "version": version,
                "batch_month": train_info["batch_month"],
                "train_months": train_info["train_months"],
                "test_month": train_info["test_month"],
                "train_rows": train_info["train_rows"],
                "test_rows": train_info["test_rows"],
                "roc_auc": train_info["roc_auc"],
                "f1": train_info["f1"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    latest_tmp = MODELS_DIR / f".latest.{version}.txt"
    latest_tmp.write_text(version + "\n", encoding="utf-8")
    os.replace(latest_tmp, MODELS_DIR / "latest.txt")
    logger.info("Опубликована модель %s (%s)", version, final_model)

    pending_window = Path(train_info["dataset_path"])
    if pending_window.exists():
        os.replace(pending_window, CURRENT_DATASET)
        logger.info("Окно обучения обновлено: %s", CURRENT_DATASET)
    elif not CURRENT_DATASET.exists():
        raise FileNotFoundError(f"Нет нового окна обучения: {pending_window}")
    else:
        logger.info("Окно уже перенесено в %s, повторно не сдвигаем.", CURRENT_DATASET)

    batch_month = date.fromisoformat(train_info["batch_month"])
    conn = get_conn()
    try:
        advance_training_state(conn, batch_month)
    finally:
        conn.close()
    return version
