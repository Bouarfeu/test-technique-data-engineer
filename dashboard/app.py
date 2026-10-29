"""Couche d'exposition : dashboard BI sur les tables gold.

DuckDB lit directement les fichiers Parquet du gold dans MinIO (aucune base à charger)
et les expose sous forme de vues SQL. Streamlit affiche les KPIs.
"""
import os

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Chicago Taxi – KPIs", layout="wide")

GOLD_TABLES = ["daily_revenue", "trip_duration_stats", "pickup_zones_daily"]


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB configurée pour MinIO, avec une vue SQL par table gold."""
    con = duckdb.connect()
    con.sql("LOAD httpfs")
    endpoint = os.environ["S3_ENDPOINT"].replace("http://", "").replace("https://", "")
    con.sql(f"""
        SET s3_endpoint = '{endpoint}';
        SET s3_access_key_id = '{os.environ["S3_ACCESS_KEY"]}';
        SET s3_secret_access_key = '{os.environ["S3_SECRET_KEY"]}';
        SET s3_use_ssl = false;
        SET s3_url_style = 'path';
        SET s3_region = 'us-east-1';
    """)
    for table in GOLD_TABLES:
        # hive_partitioning : la colonne trip_date est reconstruite depuis le nom des dossiers
        con.sql(f"""
            CREATE OR REPLACE VIEW {table} AS
            SELECT * REPLACE (CAST(trip_date AS DATE) AS trip_date)
            FROM read_parquet('s3://gold/{table}/*/*.parquet', hive_partitioning = true)
        """)
    return con


def query(sql: str):
    return get_connection().sql(sql).df()


st.title("Chicago Taxi Trips – tableau de bord BI")
st.caption("Source : couche gold du datalake (MinIO), lue en direct par DuckDB.")

try:
    bounds = query("SELECT min(trip_date) AS d_min, max(trip_date) AS d_max FROM daily_revenue")
except Exception as exc:  # gold pas encore construit
    st.warning("Aucune donnée gold disponible : lance d'abord le backfill du DAG `taxi_trips_pipeline`.")
    st.exception(exc)
    st.stop()

d_min = pd.Timestamp(bounds.loc[0, "d_min"]).date()
d_max = pd.Timestamp(bounds.loc[0, "d_max"]).date()
period = st.date_input("Période", value=(d_min, d_max), min_value=d_min, max_value=d_max)
if len(period) != 2:  # l'utilisateur n'a choisi que la date de début
    st.info("Choisis une date de fin.")
    st.stop()
start, end = period
where = f"trip_date BETWEEN DATE '{start}' AND DATE '{end}'"

# --- KPIs globaux ---
kpi = query(f"""
    SELECT sum(nb_trips) AS trips,
           sum(total_revenue) AS revenue,
           sum(total_revenue) / sum(nb_trips) AS avg_ticket,
           sum(total_tips) / sum(total_fare) * 100 AS tip_rate
    FROM daily_revenue WHERE {where}
""").iloc[0]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Trajets", f"{int(kpi.trips):,}".replace(",", " "))
c2.metric("Chiffre d'affaires", f"{kpi.revenue:,.0f} $".replace(",", " "))
c3.metric("Panier moyen", f"{kpi.avg_ticket:.2f} $")
c4.metric("Taux de pourboire", f"{kpi.tip_rate:.1f} %")

# --- Revenus par jour ---
st.subheader("Revenus par jour")
revenue = query(f"""
    SELECT trip_date, total_revenue, nb_trips
    FROM daily_revenue WHERE {where} ORDER BY trip_date
""")
st.line_chart(revenue, x="trip_date", y="total_revenue")

col_left, col_right = st.columns(2)

# --- Durée moyenne par heure (moyenne pondérée par le nombre de trajets) ---
with col_left:
    st.subheader("Durée moyenne des trajets par heure")
    duration = query(f"""
        SELECT trip_hour,
               round(sum(avg_duration_min * nb_trips) / sum(nb_trips), 2) AS avg_duration_min
        FROM trip_duration_stats WHERE {where}
        GROUP BY trip_hour ORDER BY trip_hour
    """)
    st.bar_chart(duration, x="trip_hour", y="avg_duration_min")

# --- Top zones de prise en charge sur la période ---
with col_right:
    st.subheader("Top 10 des zones de prise en charge")
    zones = query(f"""
        SELECT community_area_name AS zone,
               sum(nb_pickups) AS prises_en_charge,
               round(sum(total_revenue), 0) AS chiffre_affaires
        FROM pickup_zones_daily WHERE {where}
        GROUP BY zone ORDER BY prises_en_charge DESC LIMIT 10
    """)
    st.dataframe(zones, hide_index=True, use_container_width=True)

# --- Exposition SQL : requête libre sur les vues gold ---
with st.expander("Requête SQL libre sur le gold"):
    st.caption(f"Vues disponibles : {', '.join(GOLD_TABLES)}")
    sql = st.text_area(
        "SQL",
        "SELECT * FROM pickup_zones_daily WHERE pickup_rank <= 3 ORDER BY trip_date, pickup_rank LIMIT 30",
    )
    if st.button("Exécuter"):
        try:
            st.dataframe(query(sql), use_container_width=True)
        except Exception as exc:
            st.error(str(exc))