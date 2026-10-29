"""DAG 1/2 - Ingestion bronze : API SODA -> MinIO, un run par jour.

Un run = un jour = une partition bronze (bronze/taxi_trips/day=YYYY-MM-DD/).
L'API est interrogée jour par jour (requêtes courtes, rejouables isolément).

Chargement initial de la période :
  docker compose exec airflow airflow backfill create --dag-id taxi_ingestion \
      --start-date 2022-07-01 --end-date 2022-09-30
Puis transformation silver + gold : DAG taxi_transform.
"""
import logging
import os
from datetime import datetime, timedelta

from airflow.sdk import DAG, task

from taxi.ingestion import count_day_rows, ingest_day

log = logging.getLogger(__name__)

START_DATE = os.environ.get("PIPELINE_START_DATE", "2022-07-01")
END_DATE = os.environ.get("PIPELINE_END_DATE", "2022-09-30")
CATCHUP = os.environ.get("PIPELINE_CATCHUP", "false").lower() == "true"

# Airflow 3 : avec un cron, logical_date = date du run (ex. 2022-07-01 00:00 pour le 1er juillet).
# Conversion explicite en heure de Paris (fuseau par défaut du projet) : le jour traité est exactement
# le jour saisi, sans décalage UTC.
DAY = "{{ logical_date.in_timezone('Europe/Paris').strftime('%Y-%m-%d') }}"

with DAG(
    dag_id="taxi_ingestion",
    schedule="@daily",
    start_date=datetime.fromisoformat(START_DATE),
    end_date=datetime.fromisoformat(END_DATE),  # le run du dernier jour est inclus
    catchup=CATCHUP,
    max_active_runs=3,     # 3 jours en parallèle : rapide sans surcharger l'API de Chicago
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["taxi", "bronze"],
) as dag:

    @task.short_circuit(task_id="check_source_has_data")
    def check_source_has_data(day: str) -> bool:
        """Pas de données publiées pour ce jour -> la suite est marquée 'skipped' (pas d'échec)."""
        n = count_day_rows(day)
        log.info("Source : %d trajets publiés pour %s", n, day)
        return n > 0

    @task(task_id="ingest_bronze")
    def ingest_bronze(day: str) -> dict:
        return ingest_day(day)

    check_source_has_data(day=DAY) >> ingest_bronze(day=DAY)