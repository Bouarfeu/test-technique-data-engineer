"""DAG 2/2 - Transformation : bronze -> silver -> gold, un run par MOIS.

Chaque couche est calculée par UN SEUL job Spark qui enchaîne tous les jours de la période :
le démarrage de Spark (~30 s) n'est payé qu'une fois par run, au lieu d'une fois par jour.
Les écritures restent partitionnées par jour et idempotentes (rejouer = réécrire les mêmes dossiers).

Deux façons de le lancer :
  1. Backfill (mois entiers) :
       docker compose exec airflow airflow backfill create --dag-id taxi_transform \
           --start-date 2022-07-01 --end-date 2022-09-30
     -> 3 runs : juillet, août, septembre.
  2. Trigger avec dates (période libre, ex. quelques jours) : bouton Trigger, champs start_date / end_date.
     -> 1 run qui traite exactement cette période.
"""
import calendar
import os
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, Param

START_DATE = os.environ.get("PIPELINE_START_DATE", "2022-07-01")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "1g")
TZ = "Europe/Paris"  # fuseau par défaut du projet (AIRFLOW__CORE__DEFAULT_TIMEZONE)


def period_start(params: dict, logical_date) -> str:
    """Date de début : celle saisie au Trigger, sinon le 1er jour du mois du run (backfill)."""
    if params.get("start_date"):
        return params["start_date"]
    if logical_date is None:
        raise ValueError("Run manuel : renseigner start_date et end_date")
    return logical_date.in_timezone(TZ).strftime("%Y-%m-01")


def period_end(params: dict, logical_date) -> str:
    """Date de fin : celle saisie au Trigger, sinon le dernier jour du mois du run (backfill)."""
    if params.get("end_date"):
        return params["end_date"]
    if logical_date is None:
        raise ValueError("Run manuel : renseigner start_date et end_date")
    local = logical_date.in_timezone(TZ)
    last_day = calendar.monthrange(local.year, local.month)[1]
    return local.strftime(f"%Y-%m-{last_day:02d}")


PERIOD_ARGS = [
    "--start", "{{ period_start(params, logical_date) }}",
    "--end", "{{ period_end(params, logical_date) }}",
]

with DAG(
    dag_id="taxi_transform",
    schedule="@monthly",
    start_date=datetime.fromisoformat(START_DATE),
    catchup=False,          # rien ne part tout seul : backfill ou Trigger
    max_active_runs=1,      # un seul job Spark à la fois (RAM)
    user_defined_macros={"period_start": period_start, "period_end": period_end},
    params={
        "start_date": Param(None, type=["null", "string"], format="date",
                            description="Trigger manuel : premier jour (inclus). Vide en backfill."),
        "end_date": Param(None, type=["null", "string"], format="date",
                          description="Trigger manuel : dernier jour (inclus). Vide en backfill."),
    },
    tags=["taxi", "silver", "gold"],
) as dag:

    silver_transform = SparkSubmitOperator(
        task_id="silver_transform",
        conn_id="spark_local",
        application="/opt/airflow/jobs/silver_trips.py",
        py_files="/opt/airflow/jobs/common.py",
        application_args=PERIOD_ARGS,
        conf={"spark.driver.memory": SPARK_DRIVER_MEMORY},
    )

    gold_aggregates = SparkSubmitOperator(
        task_id="gold_aggregates",
        conn_id="spark_local",
        application="/opt/airflow/jobs/gold_aggregates.py",
        py_files="/opt/airflow/jobs/common.py",
        application_args=PERIOD_ARGS,
        conf={"spark.driver.memory": SPARK_DRIVER_MEMORY},
    )

    silver_transform >> gold_aggregates