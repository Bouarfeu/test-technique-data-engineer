"""DAG 1/2 - Ingestion bronze : API SODA -> MinIO, un run par jour.

Un run = un jour = une partition bronze (bronze/taxi_trips/day=YYYY-MM-DD/).

Production : le DAG tourne chaque matin à 8h (heure de Paris) et ingère la VEILLE (journée complète),
puis déclenche automatiquement taxi_transform sur ce seul jour.

Chargement de l'historique : backfill depuis l'interface (Trigger > Backfill), ex. du 2022-07-01 au
2022-09-30. En backfill, la transformation n'est PAS déclenchée jour par jour (92 jobs Spark) :
on la lance ensuite une seule fois sur toute la période (DAG taxi_transform).
"""
import logging
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from taxi.ingestion import count_day_rows, ingest_day

log = logging.getLogger(__name__)

# Premier trajet du dataset (borne basse des backfills ; catchup=False : rien n'est lancé tout seul)
START_DATE = os.environ.get("PIPELINE_START_DATE", "2013-01-01")

# Planning à intervalles : le run déclenché le jour J+1 à 8h couvre l'intervalle [J 8h, J+1 8h[.
# Sa logical_date vaut J 8h -> le jour ingéré est J, c'est-à-dire la veille, dont les données sont complètes.
# Même règle en backfill : un backfill du 2022-07-01 au 2022-09-30 ingère exactement ces jours.
DAILY_AT_8AM_PARIS = CronDataIntervalTimetable("0 8 * * *", timezone="Europe/Paris")

DAY = "{{ logical_date.in_timezone('Europe/Paris').strftime('%Y-%m-%d') }}"

with DAG(
    dag_id="taxi_ingestion",
    schedule=DAILY_AT_8AM_PARIS,
    start_date=datetime.fromisoformat(START_DATE),
    catchup=False,         # pas de rattrapage automatique de l'historique : il se charge en backfill
    max_active_runs=3,     # 3 jours en parallèle en backfill, sans surcharger l'API de Chicago
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

    @task.short_circuit(task_id="is_daily_run")
    def is_daily_run() -> bool:
        """Seul le run quotidien planifié enchaîne sur la transformation (pas les backfills)."""
        run_type = get_current_context()["dag_run"].run_type
        log.info("Type de run : %s", run_type)
        return run_type == "scheduled"

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="taxi_transform",
        conf={"start_date": DAY, "end_date": DAY},  # transformation du seul jour ingéré
        wait_for_completion=False,
    )

    check_source_has_data(day=DAY) >> ingest_bronze(day=DAY) >> is_daily_run() >> trigger_transform