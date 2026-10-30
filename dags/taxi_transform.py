"""DAG 2/2 - Transformation : bronze -> silver -> gold, sur une période donnée.

Chaque couche est calculée par UN SEUL job Spark qui enchaîne tous les jours de la période :
le démarrage de Spark (~30 s) n'est payé qu'une fois par run, au lieu d'une fois par jour.
Les écritures restent partitionnées par jour et idempotentes (rejouer = réécrire les mêmes dossiers).

Pas de planning propre. Le DAG est déclenché :
  - automatiquement par taxi_ingestion, chaque matin, sur la veille (start_date = end_date = J-1) ;
  - à la demande : bouton Trigger, champs start_date / end_date (ex. tout un trimestre en un seul run).
"""
import os
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, Param

SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "1g")


def period(params: dict, key: str) -> str:
    """Date de la période (start_date ou end_date), obligatoire."""
    if not params.get(key):
        raise ValueError(f"Paramètre {key} manquant : renseigner start_date et end_date au Trigger")
    return params[key]


PERIOD_ARGS = [
    "--start", "{{ period(params, 'start_date') }}",
    "--end", "{{ period(params, 'end_date') }}",
]

with DAG(
    dag_id="taxi_transform",
    schedule=None,          # déclenché par taxi_ingestion ou à la main, jamais tout seul
    start_date=datetime(2022, 1, 1),
    catchup=False,
    max_active_runs=1,      # un seul job Spark à la fois (RAM)
    user_defined_macros={"period": period},
    params={
        "start_date": Param(None, type=["null", "string"], format="date",
                            description="Premier jour à traiter (inclus)"),
        "end_date": Param(None, type=["null", "string"], format="date",
                          description="Dernier jour à traiter (inclus)"),
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