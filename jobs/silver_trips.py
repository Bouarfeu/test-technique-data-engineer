"""Job Spark silver : bronze (CSV brut) -> silver (Parquet nettoyé, typé, dédupliqué).

Usage :
  spark-submit silver_trips.py --day 2022-07-15                       (un jour)
  spark-submit silver_trips.py --start 2022-07-01 --end 2022-07-31    (une période, une seule session Spark)

- Lit uniquement la partition bronze du jour : s3a://bronze/taxi_trips/day=YYYY-MM-DD/
- Écrit les lignes valides dans    s3a://silver/taxi_trips/trip_date=YYYY-MM-DD/
- Écrit les lignes rejetées dans   s3a://silver/taxi_trips_quarantine/trip_date=YYYY-MM-DD/
  avec la raison du rejet (rien n'est jeté silencieusement).
Rejouer un jour réécrit uniquement ses partitions : le job est idempotent.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import get_spark, parse_days

BRONZE_PATH = "s3a://bronze/taxi_trips"
SILVER_PATH = "s3a://silver/taxi_trips"
QUARANTINE_PATH = "s3a://silver/taxi_trips_quarantine"

# Colonnes de la source conservées en silver. Tout est lu en texte, puis typé explicitement.
SOURCE_COLUMNS = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
    "trip_seconds", "trip_miles", "pickup_census_tract", "dropoff_census_tract",
    "pickup_community_area", "dropoff_community_area",
    "fare", "tips", "tolls", "extras", "trip_total",
    "payment_type", "company",
    "pickup_centroid_latitude", "pickup_centroid_longitude",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude",
]

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"  # format des timestamps renvoyés par l'API SODA
MAX_TRIP_SECONDS = 24 * 3600              # au-delà d'une journée, le trajet est jugé aberrant


def read_bronze(spark: SparkSession, day: str) -> DataFrame:
    """Lit le CSV brut du jour. Colonnes lues par leur NOM (en-tête), toutes en texte."""
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)   # sécurité : champs texte avec retours à la ligne
        .option("escape", '"')
        .csv(f"{BRONZE_PATH}/day={day}/*.csv")
    )
    # Colonne absente de la source -> créée vide, pour garder un schéma silver stable
    for c in SOURCE_COLUMNS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df.select(*SOURCE_COLUMNS)


def _parse_ts(col: str):
    """Format SODA (2022-07-01T00:15:00.000), avec repli sur le format ISO standard."""
    return F.coalesce(F.to_timestamp(col, TS_FORMAT), F.to_timestamp(col))


def cast_types(df: DataFrame) -> DataFrame:
    """Parsing et typage explicite de chaque colonne."""
    money = ["fare", "tips", "tolls", "extras", "trip_total"]
    coords = ["pickup_centroid_latitude", "pickup_centroid_longitude",
              "dropoff_centroid_latitude", "dropoff_centroid_longitude"]
    out = (
        df.withColumn("trip_id", F.trim("trip_id"))
        .withColumn("taxi_id", F.trim("taxi_id"))
        .withColumn("trip_start_timestamp", _parse_ts("trip_start_timestamp"))
        .withColumn("trip_end_timestamp", _parse_ts("trip_end_timestamp"))
        .withColumn("trip_seconds", F.col("trip_seconds").cast("int"))
        .withColumn("trip_miles", F.col("trip_miles").cast("double"))
        .withColumn("pickup_census_tract", F.col("pickup_census_tract").cast("long"))
        .withColumn("dropoff_census_tract", F.col("dropoff_census_tract").cast("long"))
        .withColumn("pickup_community_area", F.col("pickup_community_area").cast("int"))
        .withColumn("dropoff_community_area", F.col("dropoff_community_area").cast("int"))
        .withColumn("payment_type", F.initcap(F.trim("payment_type")))
        .withColumn("company", F.trim("company"))
    )
    for c in money:
        out = out.withColumn(c, F.col(c).cast("decimal(10,2)"))
    for c in coords:
        out = out.withColumn(c, F.col(c).cast("double"))
    return out


def add_rejection_reason(df: DataFrame) -> DataFrame:
    """Ajoute `rejection_reason` : null si la ligne est valide, sinon la première règle violée."""
    rules = [
        (F.col("trip_id").isNull() | (F.col("trip_id") == ""), "missing_trip_id"),
        (F.col("trip_start_timestamp").isNull(), "missing_start_timestamp"),
        (F.col("trip_end_timestamp") < F.col("trip_start_timestamp"), "end_before_start"),
        (F.col("trip_seconds").isNull() | (F.col("trip_seconds") <= 0), "invalid_duration"),
        (F.col("trip_seconds") > MAX_TRIP_SECONDS, "duration_over_24h"),
        (F.col("trip_miles") < 0, "negative_distance"),
        (F.col("fare").isNull() | (F.col("fare") < 0), "invalid_fare"),
        (F.col("trip_total").isNull() | (F.col("trip_total") < 0), "invalid_total"),
    ]
    reason = F.lit(None).cast("string")
    for condition, label in reversed(rules):  # reversed -> la première règle de la liste l'emporte
        reason = F.when(condition, F.lit(label)).otherwise(reason)
    return df.withColumn("rejection_reason", reason)


def deduplicate(df: DataFrame) -> DataFrame:
    """Un trip_id = un trajet : on garde une seule occurrence."""
    return df.dropDuplicates(["trip_id"])


def enrich(df: DataFrame) -> DataFrame:
    """Colonnes dérivées utiles aux agrégats gold."""
    return (
        df.withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .withColumn("trip_hour", F.hour("trip_start_timestamp"))
        .withColumn("trip_minutes", F.round(F.col("trip_seconds") / 60, 2))
    )


def transform(df_raw: DataFrame, day: str) -> tuple[DataFrame, DataFrame]:
    """Bronze brut -> (lignes valides, lignes rejetées). Fonction pure, testable sans MinIO."""
    df = add_rejection_reason(cast_types(df_raw))
    df = df.withColumn("trip_date", F.lit(day).cast("date"))  # partition = jour ingéré

    rejected = df.filter(F.col("rejection_reason").isNotNull())
    valid = deduplicate(df.filter(F.col("rejection_reason").isNull())).drop("rejection_reason")
    return enrich(valid), rejected


def write_partition(df: DataFrame, root: str, day: str) -> None:
    """Écrase exactement le dossier du jour (même si df est vide) : rejouer un jour est idempotent.

    La colonne trip_date n'est pas stockée dans les fichiers : Spark la retrouve
    automatiquement depuis le nom du dossier trip_date=YYYY-MM-DD.
    """
    # coalesce(1) : un jour ≈ 20 000 lignes (quelques Mo) -> un seul fichier Parquet par partition,
    # au lieu de dizaines de micro-fichiers issus du shuffle de dropDuplicates
    df.drop("trip_date").coalesce(1).write.mode("overwrite").parquet(f"{root}/trip_date={day}")


def process_day(spark: SparkSession, day: str) -> bool:
    """Traite un jour. Renvoie False si sa partition bronze est absente."""
    try:
        raw = read_bronze(spark, day).cache()
        n_raw = raw.count()
    except Exception as exc:  # partition bronze absente
        if "Path does not exist" not in str(exc):
            raise
        print(f"[silver] {day} : partition bronze absente, jour ignoré")
        return False

    valid, rejected = transform(raw, day)
    valid, rejected = valid.cache(), rejected.cache()
    write_partition(valid, SILVER_PATH, day)
    write_partition(rejected, QUARANTINE_PATH, day)

    n_valid, n_rejected = valid.count(), rejected.count()
    print(f"[silver] {day} : {n_raw} lignes bronze -> {n_valid} valides, "
          f"{n_rejected} en quarantaine, {n_raw - n_valid - n_rejected} doublons supprimés")
    if n_raw > 0 and n_valid == 0:
        raise ValueError(f"Aucune ligne valide pour {day} : vérifier le bronze ou les règles qualité")

    for df in (raw, valid, rejected):  # libère la mémoire avant le jour suivant
        df.unpersist()
    return True


def main() -> None:
    days = parse_days()
    # Une seule session Spark pour toute la période : on ne paie le démarrage qu'une fois
    spark = get_spark(f"silver_trips_{days[0]}_{days[-1]}")
    missing = [day for day in days if not process_day(spark, day)]
    spark.stop()
    if missing:
        raise ValueError(f"Bronze absent pour {len(missing)} jour(s) : {', '.join(missing)}")


if __name__ == "__main__":
    main()