"""DAG скользящего переобучения CatBoost.

Файл только связывает задачи. Логика каждой задачи лежит в pipeline.retrain.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.retrain.extract_month_batch import extract_month_batch
from pipeline.retrain.fit_catboost import fit_catboost
from pipeline.retrain.preprocess_dataset import preprocess_dataset
from pipeline.retrain.publish_model import publish_model
from pipeline.retrain.slide_training_window import slide_training_window
from pipeline.retrain.time_train_test_split import time_train_test_split


@dag(
    dag_id="retrain_catboost",
    description="Раз в 5 минут сдвигает 3-месячное окно и переобучает CatBoost",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "catboost"],
    doc_md="""
### Переобучение my_catboost

Каждый запуск имитирует новый месяц в проде:

1. Из витрины `labeled_transactions` берётся следующий календарный месяц.
2. Месяц добавляется в датасет из трёх месяцев, самый старый месяц выбрасывается.
3. Train — два более ранних месяца, test — самый свежий.
4. Препроцессинг — `run_preproc` из `fraud_detector/src`.
5. CatBoost обучается, бинарник публикуется в общий каталог моделей.

Указатель месяца в `training_state` сдвигается только после публикации модели.
Если очередного месяца в витрине ещё нет, задачи помечаются как skipped.
Новые месяцы после исторических данных дописывает DAG `supply_labeled_batch`.
    """,
)
def retrain_catboost():
    """Связывает извлечение месяца, сдвиг окна, split, препроцессинг, обучение и публикацию."""
    extracted = task(extract_month_batch, task_id="extract_month_batch")()
    slid = task(slide_training_window, task_id="slide_training_window")(extracted)
    split = task(time_train_test_split, task_id="time_train_test_split")(slid)
    processed = task(preprocess_dataset, task_id="preprocess_dataset")(split)
    trained = task(fit_catboost, task_id="fit_catboost")(processed)
    task(publish_model, task_id="publish_model")(trained)


retrain_catboost()
