"""Препроцессинг train и test той же функцией, что и онлайн-скоринг."""

import logging
import sys

import pandas as pd

from pipeline.settings import FRAUD_SRC, SPLITS_DIR

logger = logging.getLogger(__name__)


def preprocess_dataset(split_info: dict) -> dict:
    """Применяет run_preproc из fraud_detector/src к сырым train и test.

    Читает пути из split_info. transaction_time, name_1, name_2 и street
    уходят внутри run_preproc, колонка target остаётся для обучения.
    Пишет splits/train_proc.parquet и splits/test_proc.parquet.
    Возвращает пути к ним и метаданные месяца, нужные следующим задачам.
    """
    if FRAUD_SRC not in sys.path:
        sys.path.insert(0, FRAUD_SRC)
    from preprocessing import run_preproc

    train = run_preproc(pd.read_parquet(split_info["train_raw_path"]))
    test = run_preproc(pd.read_parquet(split_info["test_raw_path"]))
    train_path = SPLITS_DIR / "train_proc.parquet"
    test_path = SPLITS_DIR / "test_proc.parquet"
    train.to_parquet(train_path, index=False)
    test.to_parquet(test_path, index=False)
    logger.info("После препроцессинга: train %s, test %s", train.shape, test.shape)
    return {
        "train_proc_path": str(train_path),
        "test_proc_path": str(test_path),
        "dataset_path": split_info["dataset_path"],
        "batch_month": split_info["batch_month"],
        "train_months": split_info["train_months"],
        "test_month": split_info["test_month"],
    }
