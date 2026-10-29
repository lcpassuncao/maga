from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from sness.pipeline.custom_operators.dataproc_operator import (
    build_ness_etl_path,
)
from sness.pipeline.custom_operators.minecraft_operator import (
    MinecraftOperator,
)
from sness.pipeline.slack import slack_failed_task
from sness.settings.pod_type import PodType

# Last change: 2026-08-25 22:56 | Author: rodrigo.silva@luizalabs.com

def get_environment() -> str:
    if Variable.get("AIRFLOW_ENVIRONMENT") == "production":
        return "prd"
    return "stg"


environment = get_environment()
DAG_FOLDER = "visao_unica_preco"
UTILS_FOLDER = "visao_unica_commons"


# A trilha incremental importa utils (parsing do Kafka), visao_precos
# (regra de negocio compartilhada com o batch) e constants_fast (schemas dos
# topicos). O MinecraftOperator NAO coloca arquivos-irmaos no classpath
# automaticamente (precedente: fry_base_prices passa constants.py
# explicitamente), entao os tres precisam ir como pyfiles.
pyfiles = [
    build_ness_etl_path(UTILS_FOLDER, "utils", "utils.py"),
    build_ness_etl_path(DAG_FOLDER, "trusted", "visao_precos.py"),
    build_ness_etl_path(DAG_FOLDER, "refined", "constants_fast.py"),
]


if environment == "prd":
    slack_notification_channel = "integração-visao-unica"
    schedule_interval = None
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
    "VisaoUnicaPrecoFast",
    catchup=False,
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=1,
)

start = DummyOperator(task_id="Start", dag=dag)

fast_prices = MinecraftOperator(
    task_id="fast_prices",
    main=build_ness_etl_path(DAG_FOLDER, "refined", "fast_path_prices.py"),
    instances=1,
    max_instances=4,
    dag=dag,
    # Uma unica stream consome os dois topicos CDC; o executor LARGE segue a
    # convencao dos ETLs de streaming CDC do repo (fry_base_prices,
    # fry_price_campaign). instances=1 porque ha um unico consumer/checkpoint
    # e a cadencia e curta (5 min): paralelismo intra-run nao se aplica.
    # O DRIVER_SMALL basta (so orquestra o availableNow).
    executor_size=PodType.EXECUTOR_ULTRA_MEGA_LARGE,
    driver_size=PodType.DRIVER_LARGE,
    arguments=[
        "--environment",
        environment,
    ],
    pyfiles=pyfiles,
)

fast_path_prices_copy = MinecraftOperator(
    task_id="fast_path_prices_copy",
    main=build_ness_etl_path(DAG_FOLDER, "refined", "fast_path_prices_copy.py"),
    instances=1,
    max_instances=4,
    dag=dag,
    # Uma unica stream consome os dois topicos CDC; o executor LARGE segue a
    # convencao dos ETLs de streaming CDC do repo (fry_base_prices,
    # fry_price_campaign). instances=1 porque ha um unico consumer/checkpoint
    # e a cadencia e curta (5 min): paralelismo intra-run nao se aplica.
    # O DRIVER_SMALL basta (so orquestra o availableNow).
    executor_size=PodType.EXECUTOR_ULTRA_MEGA_LARGE,
    driver_size=PodType.DRIVER_LARGE,
    arguments=[
        "--environment",
        environment,
    ],
    pyfiles=pyfiles,
)

end = DummyOperator(task_id="End", dag=dag)

(start >> fast_prices >> end)
(start >> fast_path_prices_copy >> end)
