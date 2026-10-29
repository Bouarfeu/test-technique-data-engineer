"""Pipeline Chicago Taxi Trips : bronze -> silver -> gold, un run par jour.

La période traitée est paramétrable via variables d'environnement (docker-compose.yml) :
  PIPELINE_START_DATE   premier jour traité (YYYY-MM-DD)
  PIPELINE_END_DATE     dernier jour traité, inclus (YYYY-MM-DD)
  PIPELINE_CATCHUP      false (défaut) -> rien ne part tout seul, on charge la période via un backfill
                        true           -> les jours de la période sont lancés automatiquement au démarrage

Chargement initial :
  docker compose exec airflow airflow backfill create --dag-id taxi_trips_pipeline \
      --start-date 2022-07-01 --end-date 2022-09-30
"""
import logging
import os
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, task

from taxi.ingestion import count_day_rows, ingest_day

log = logging.getLogger(__name__)

START_DATE = os.environ.get("PIPELINE_START_DATE", "2022-07-01")
END_DATE = os.environ.get("PIPELINE_END_DATE", "2022-09-30")
CATCHUP = os.environ.get("PIPELINE_CATCHUP", "false").lower() == "true"

# Airflow 3 : avec un cron, logical_date = date du run (ex. 2022-07-01 pour le 1er juillet)
DAY = "{{ logical_date.strftime('%Y-%m-%d') }}"

with DAG(
    dag_id="taxi_trips_pipeline",
    schedule="@daily",
    start_date=datetime.fromisoformat(START_DATE),
    end_date=datetime.fromisoformat(END_DATE),  # le run du dernier jour est inclus
    catchup=CATCHUP,
    max_active_runs=3,     # 3 jours en parallèle : rapide sans surcharger l'API de Chicago
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["taxi", "medallion"],
) as dag:

    @task.short_circuit(task_id="check_source_has_data")
    def check_source_has_data(day: str) -> bool:
        """Pas de données publiées pour ce jour -> toute la suite est marquée 'skipped' (pas d'échec)."""
        n = count_day_rows(day)
        log.info("Source : %d trajets publiés pour %s", n, day)
        return n > 0

    @task(task_id="ingest_bronze")
    def ingest_bronze(day: str) -> dict:
        return ingest_day(day)

    silver_transform = SparkSubmitOperator(
        task_id="silver_transform",
        conn_id="spark_local",
        application="/opt/airflow/jobs/silver_trips.py",
        py_files="/opt/airflow/jobs/common.py",
        application_args=["--day", DAY],
        conf={"spark.driver.memory": "2g"},
        # Un seul job Spark à la fois (RAM) ; les ingestions des autres jours continuent en parallèle
        max_active_tis_per_dag=1,
    )

    check_source_has_data(day=DAY) >> ingest_bronze(day=DAY) >> silver_transform

    # Prochaines étapes : >> quality_checks >> [gold_*]