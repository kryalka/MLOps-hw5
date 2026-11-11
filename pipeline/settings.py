"""Общие пути артефактов обучения и поставки батчей."""

import os
from pathlib import Path

TRAINING_DIR = Path(os.getenv("TRAINING_DATA_DIR", "/opt/airflow/training_data"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/opt/airflow/models"))
FRAUD_SRC = os.getenv("FRAUD_SRC", "/opt/airflow/fraud_src")

CURRENT_DATASET = TRAINING_DIR / "current.parquet"
PENDING_WINDOW = TRAINING_DIR / "pending_window.parquet"
BATCHES_DIR = TRAINING_DIR / "batches"
SPLITS_DIR = TRAINING_DIR / "splits"
SUPPLY_BATCH = TRAINING_DIR / "supply_batch.parquet"

CAT_FEATURES = ["merch", "cat_id", "gender", "one_city", "us_state", "jobs"]
LABEL = "target"
SAMPLE_SIZE = 100_000
HISTORY_YEARS = 3
