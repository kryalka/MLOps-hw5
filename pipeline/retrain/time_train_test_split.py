"""Временной train/test split трёхмесячного окна."""

import logging

import pandas as pd

from pipeline.settings import SPLITS_DIR

logger = logging.getLogger(__name__)


def time_train_test_split(window_info: dict) -> dict:
    """Делит окно по времени: два более ранних месяца в train, свежий месяц в test.

    Читает parquet окна из window_info, сортирует по transaction_time и
    сохраняет сырые части в splits/train_raw.parquet и splits/test_raw.parquet.
    Колонка времени остаётся, потому что препроцессинг сам строит календарные признаки.
    Возвращает пути к частям, месяц батча и подписи месяцев train и test.
    """
    SPLITS_DIR.mkdir(parents=True, exist_ok=True)
    frame = pd.read_parquet(window_info["dataset_path"])
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"])
    frame = frame.sort_values("transaction_time")
    periods = frame["transaction_time"].dt.to_period("M")
    latest = periods.max()
    train = frame.loc[periods < latest]
    test = frame.loc[periods == latest]
    if train.empty or test.empty:
        raise RuntimeError(
            f"Не удалось разрезать окно по времени: train={len(train)}, test={len(test)}"
        )

    train_path = SPLITS_DIR / "train_raw.parquet"
    test_path = SPLITS_DIR / "test_raw.parquet"
    train.to_parquet(train_path, index=False)
    test.to_parquet(test_path, index=False)
    train_months = sorted(periods.loc[periods < latest].unique().astype(str))
    logger.info(
        "Train месяцы %s (%s строк), test %s (%s строк).",
        train_months,
        len(train),
        latest,
        len(test),
    )
    return {
        "train_raw_path": str(train_path),
        "test_raw_path": str(test_path),
        "dataset_path": window_info["dataset_path"],
        "batch_month": window_info["batch_month"],
        "train_months": ",".join(train_months),
        "test_month": str(latest),
    }
