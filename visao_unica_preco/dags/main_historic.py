from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from sness.datalake.metadata import Mode, Zone
from sness.pipeline.custom_operators.cloud_storage_to_big_query_operator import (  # noqa
    CloudStorageToBigQueryOperator,
)
from sness.pipeline.custom_operators.dataproc_operator import (
    build_ness_etl_path,
)
from sness.pipeline.custom_operators.minecraft_operator import (
    MinecraftOperator,
)
from sness.pipeline.slack import slack_failed_task
from sness.settings.pod_type import PodType


def get_environment() -> str:
    if Variable.get("AIRFLOW_ENVIRONMENT") == "production":
        return "prd"
    return "stg"


environment = get_environment()
DAG_FOLDER = "visao_unica_preco"
NAMESPACE_NAME = "visao_unica"
DATASET_NAME = "price_historic"
UTILS_FOLDER = "visao_unica_commons"
WRITING_MODE = Variable.get("writing_mode_visao_preco")
DAYS_TO_PROCESS = Variable.get("days_to_process_prices_historic_to_bq")


if environment == "prd":
    slack_notification_channel = "integração-visao-unica"
    schedule_interval = "0 */6 * * *"
else:
    slack_notification_channel = "integração-visao-unica-staging"
    schedule_interval = None


default_args = {
    "owner": "Canais Digitais",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime.strptime("2024-06-17", "%Y-%m-%d"),
    "on_failure_callback": slack_failed_task,
    "slack_notification_channel": slack_notification_channel,
    "params": {
        "labels": {
            "se_tribe": "canais_digitais",
            "se_vertical": "canais_digitais",
        },
        "se_email": "luiz.gois@luizalabs.com",
    },
}

dag = DAG(
    "VisaoUnicaPreco_Historico",
    catchup=False,
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=1,
)

start = DummyOperator(task_id="Start", dag=dag)

prices_historic_last_90_days = MinecraftOperator(
    task_id="prices_historic_last_90_days",
    main=build_ness_etl_path(
        DAG_FOLDER, "trusted", "prices_historic_last_90_days.py"
    ),
    instances=15,
    max_instances=25,
    dag=dag,
    executor_size=PodType.EXECUTOR_ULTRA_LARGE,
    driver_size=PodType.DRIVER_LARGE,
    arguments=[
        "--environment",
        environment,
        "--days_to_process",
        DAYS_TO_PROCESS,
    ],
    pyfiles=[build_ness_etl_path(UTILS_FOLDER, "utils", "utils.py")],
)


if environment == "prd":

    bq_prices_historic = CloudStorageToBigQueryOperator(
        task_id="bq_prices_historic",
        zone=Zone.REFINED,
        namespace=NAMESPACE_NAME,
        dataset="prices_historic_last_90_days",
        environment=environment,
        mode=Mode.OVERWRITE,
        partition_column="price_date",
        partition_type="DAY",
        clustering_fields=["price_date"],
        dag=dag,
    )

    end = DummyOperator(task_id="End", dag=dag)

    (start >> prices_historic_last_90_days >> bq_prices_historic >> end)
