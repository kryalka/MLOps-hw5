"""Обучение CatBoost на новом трёхмесячном окне."""

import logging
from datetime import datetime, timezone

import pandas as pd

from pipeline.settings import CAT_FEATURES, LABEL, MODELS_DIR

logger = logging.getLogger(__name__)


def prepare_features(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Отделяет target от признаков и приводит категориальные колонки к строкам.

    Категории совпадают с теми, что ожидает онлайн-скоринг после run_preproc.
    Возвращает матрицу признаков и вектор целевой переменной.
    """
    if LABEL not in frame.columns:
        raise ValueError(f"В датасете нет колонки {LABEL}")
    features = frame.drop(columns=[LABEL]).copy()
    for column in CAT_FEATURES:
        if column not in features.columns:
            raise ValueError(f"После препроцессинга нет категориального признака {column}")
        features[column] = features[column].astype(str)
    return features, frame[LABEL]


def fit_catboost(preproc_info: dict) -> dict:
    """Обучает CatBoost на train и считает ROC-AUC и F1 на свежем месяце.

    Читает обработанные parquet из preproc_info. Модель учится с ранней
    остановкой по test-месяцу и сохраняется во временный .cbm в каталоге моделей.
    В общий latest.txt файл ещё не публикуется.
    Возвращает версию, путь к черновику, метрики и путь к новому окну.
    """
    from catboost import CatBoostClassifier
    from sklearn.metrics import f1_score, roc_auc_score

    train = pd.read_parquet(preproc_info["train_proc_path"])
    test = pd.read_parquet(preproc_info["test_proc_path"])
    x_train, y_train = prepare_features(train)
    x_test, y_test = prepare_features(test)

    model = CatBoostClassifier(
        iterations=100,
        task_type="CPU",
        thread_count=2,
        depth=6,
        learning_rate=0.1,
        loss_function="Logloss",
        eval_metric="AUC",
        auto_class_weights="Balanced",
        early_stopping_rounds=20,
        random_seed=42,
        allow_writing_files=False,
        verbose=20,
    )
    model.fit(
        x_train,
        y_train,
        cat_features=CAT_FEATURES,
        eval_set=(x_test, y_test),
        use_best_model=True,
    )

    proba = model.predict_proba(x_test)[:, 1]
    pred = (proba > 0.5).astype(int)
    f1 = float(f1_score(y_test, pred, zero_division=0))
    if y_test.nunique() < 2:
        roc_auc = None
        logger.warning("В test одна метка, ROC-AUC не определён. F1=%.4f", f1)
    else:
        roc_auc = float(roc_auc_score(y_test, proba))
        logger.info(
            "Качество на test %s: ROC-AUC=%.4f, F1=%.4f",
            preproc_info["test_month"],
            roc_auc,
            f1,
        )

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    staging_model = MODELS_DIR / f".staging_{version}.cbm"
    model.save_model(str(staging_model))
    logger.info("Черновик модели сохранён: %s", staging_model)
    return {
        "version": version,
        "staging_model": str(staging_model),
        "dataset_path": preproc_info["dataset_path"],
        "batch_month": preproc_info["batch_month"],
        "train_months": preproc_info["train_months"],
        "test_month": preproc_info["test_month"],
        "train_rows": int(len(x_train)),
        "test_rows": int(len(x_test)),
        "roc_auc": roc_auc,
        "f1": f1,
    }
