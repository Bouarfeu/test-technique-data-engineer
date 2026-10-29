"""Utilitaires partagés par tous les jobs Spark."""
import os

from pyspark.sql import SparkSession


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
        .getOrCreate()
    )