"""Job de validation de l'infra : Spark écrit puis relit un Parquet dans MinIO."""
from common import get_spark

spark = get_spark("hello_minio")

df = spark.createDataFrame([(1, "bronze"), (2, "silver"), (3, "gold")], ["id", "layer"])
path = "s3a://gold/_healthcheck"

df.write.mode("overwrite").parquet(path)
count = spark.read.parquet(path).count()

assert count == 3, f"Attendu 3 lignes, obtenu {count}"
print(f"OK : {count} lignes écrites et relues depuis {path}")
spark.stop()
