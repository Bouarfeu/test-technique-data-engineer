"""Job Spark gold : silver (trajets propres) -> tables analytiques prêtes pour la BI.

Usage :
  spark-submit gold_aggregates.py --day 2022-07-15                       (un jour)
  spark-submit gold_aggregates.py --start 2022-07-01 --end 2022-07-31    (une période, une seule session Spark)

Lit la partition silver du jour, la joint au référentiel des 77 zones de Chicago,
puis écrit 3 tables, chacune partitionnée par jour (s3a://gold/<table>/trip_date=YYYY-MM-DD/) :

  daily_revenue        revenus du jour (CA, pourboires, panier moyen...)
  trip_duration_stats  durée et distance des trajets, par heure de la journée
  pickup_zones_daily   activité par zone de prise en charge, avec classement du jour

Rejouer un jour réécrit uniquement ses partitions : le job est idempotent.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import get_spark, parse_days

SILVER_PATH = "s3a://silver/taxi_trips"
GOLD_PATH = "s3a://gold"
# Référentiel des 77 community areas de Chicago (numéro -> nom), versionné avec le code
ZONES_SEED = "file:///opt/airflow/jobs/seeds/community_areas.csv"


def read_zones(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("header", True).csv(ZONES_SEED)
        .select(F.col("community_area").cast("int"), F.col("community_area_name"))
    )


def daily_revenue(trips: DataFrame) -> DataFrame:
    """KPI 'revenus par jour'."""
    return trips.groupBy("trip_date").agg(
        F.count("*").alias("nb_trips"),
        F.sum("trip_total").alias("total_revenue"),
        F.sum("fare").alias("total_fare"),
        F.sum("tips").alias("total_tips"),
        F.round(F.avg("trip_total"), 2).alias("avg_revenue_per_trip"),
        F.round(F.sum("tips") / F.sum("fare") * 100, 2).alias("tip_rate_pct"),
        F.countDistinct("taxi_id").alias("active_taxis"),
    )


def trip_duration_stats(trips: DataFrame) -> DataFrame:
    """KPI 'durée moyenne des trajets', détaillé par heure de prise en charge."""
    return trips.groupBy("trip_date", "trip_hour").agg(
        F.count("*").alias("nb_trips"),
        F.round(F.avg("trip_minutes"), 2).alias("avg_duration_min"),
        F.round(F.percentile_approx("trip_minutes", 0.5), 2).alias("median_duration_min"),
        F.round(F.avg("trip_miles"), 2).alias("avg_distance_miles"),
    )


def pickup_zones_daily(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """KPI 'top zones de prise en charge' : activité par zone, avec rang dans la journée."""
    by_zone = (
        trips.withColumnRenamed("pickup_community_area", "community_area")
        .groupBy("trip_date", "community_area")
        .agg(
            F.count("*").alias("nb_pickups"),
            F.sum("trip_total").alias("total_revenue"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_duration_min"),
        )
    )
    rank_window = Window.partitionBy("trip_date").orderBy(F.desc("nb_pickups"))
    return (
        by_zone.join(F.broadcast(zones), "community_area", "left")  # petit référentiel -> broadcast
        .withColumn(
            "community_area_name",
            # Zone non renseignée (trajet hors Chicago ou non géolocalisé) -> libellé explicite
            F.coalesce("community_area_name", F.lit("Unknown / outside Chicago")),
        )
        .withColumn("pickup_rank", F.row_number().over(rank_window))
    )


def write_partition(df: DataFrame, table: str, day: str) -> None:
    """Un fichier Parquet par jour et par table ; le dossier du jour est écrasé (idempotence)."""
    df.drop("trip_date").coalesce(1).write.mode("overwrite").parquet(
        f"{GOLD_PATH}/{table}/trip_date={day}"
    )


def process_day(spark: SparkSession, zones: DataFrame, day: str) -> bool:
    """Calcule les 3 tables gold d'un jour. Renvoie False si sa partition silver est absente."""
    try:
        trips = (
            spark.read.parquet(f"{SILVER_PATH}/trip_date={day}")
            .withColumn("trip_date", F.lit(day).cast("date"))
            .cache()
        )
        trips.count()
    except Exception as exc:  # partition silver absente
        if "Path does not exist" not in str(exc):
            raise
        print(f"[gold] {day} : partition silver absente, jour ignoré")
        return False

    tables = {
        "daily_revenue": daily_revenue(trips),
        "trip_duration_stats": trip_duration_stats(trips),
        "pickup_zones_daily": pickup_zones_daily(trips, zones),
    }
    for name, df in tables.items():
        write_partition(df, name, day)
    print(f"[gold] {day} : {', '.join(tables)} écrites")
    trips.unpersist()
    return True


def main() -> None:
    days = parse_days()
    # Une seule session Spark pour toute la période : on ne paie le démarrage qu'une fois
    spark = get_spark(f"gold_aggregates_{days[0]}_{days[-1]}")
    zones = read_zones(spark).cache()
    missing = [day for day in days if not process_day(spark, zones, day)]
    spark.stop()
    if missing:
        raise ValueError(f"Silver absent pour {len(missing)} jour(s) : {', '.join(missing)}")


if __name__ == "__main__":
    main()