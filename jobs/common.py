"""Utilitaires partagés par tous les jobs Spark."""
import argparse
import os
from datetime import date, timedelta

from pyspark.sql import SparkSession


def parse_days() -> list[str]:
    """Jours à traiter, depuis la ligne de commande.

    --day 2022-07-15                       -> un seul jour
    --start 2022-07-01 --end 2022-07-31    -> tous les jours de la période, bornes incluses
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", help="Jour unique, format YYYY-MM-DD")
    parser.add_argument("--start", help="Premier jour de la période, format YYYY-MM-DD")
    parser.add_argument("--end", help="Dernier jour de la période (inclus), format YYYY-MM-DD")
    args = parser.parse_args()

    if args.day:
        return [args.day]
    if not (args.start and args.end):
        parser.error("indiquer --day, ou bien --start et --end")
    first, last = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if last < first:
        parser.error("--end doit être postérieur ou égal à --start")
    days = [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]
    print(f"{len(days)} jour(s) à traiter : du {days[0]} au {days[-1]}")
    return days


def get_spark(app_name: str) -> SparkSession:
    """SparkSession configurée pour lire/écrire dans MinIO via s3a://."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Rejouer une partition n'écrase que cette partition -> idempotence
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        # Un jour ≈ 20 000 lignes : 4 partitions de shuffle suffisent (200 par défaut = surcoût inutile)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )