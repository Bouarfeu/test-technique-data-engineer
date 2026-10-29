"""DAG de validation : Airflow -> spark-submit -> MinIO. À supprimer une fois le vrai DAG en place."""
from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

with DAG(
    dag_id="hello_spark_minio",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["infra"],
) as dag:
    SparkSubmitOperator(
        task_id="spark_write_minio",
        conn_id="spark_local",
        application="/opt/airflow/jobs/hello_minio.py",
        py_files="/opt/airflow/jobs/common.py",
        conf={"spark.driver.memory": "2g"},
        verbose=False,
    )
