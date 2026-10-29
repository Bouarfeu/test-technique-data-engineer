"""Ingestion bronze : API SODA (Chicago Taxi Trips 2013-2023) -> MinIO, un jour à la fois.

Les données sont stockées telles quelles (CSV brut, une page = un fichier),
dans une partition par jour : taxi_trips/day=YYYY-MM-DD/.
Rejouer un jour supprime d'abord sa partition : l'ingestion est idempotente.
"""
import csv
import io
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# Dataset "Taxi Trips (2013-2023)" de l'énoncé. Pour 2024 et après : ajtu-isnz.
SODA_DATASET_ID = os.environ.get("SODA_DATASET_ID", "wrvz-psew")
SODA_URL = f"https://data.cityofchicago.org/resource/{SODA_DATASET_ID}.csv"
# Pages de 20 000 lignes : chaque appel reste court, l'API de Chicago étant lente sur les gros paquets
PAGE_SIZE = int(os.environ.get("SODA_PAGE_SIZE", "20000"))
# Délai max d'attente d'une réponse de l'API, en secondes
SODA_TIMEOUT = int(os.environ.get("SODA_TIMEOUT", "300"))
# Plafond optionnel de lignes par jour (0 = pas de plafond), utile uniquement pour un test rapide
MAX_ROWS = int(os.environ.get("SODA_MAX_ROWS_PER_DAY", "0"))
BRONZE_BUCKET = "bronze"
DATASET = "taxi_trips"


def _soda_headers() -> dict:
    token = os.environ.get("SODA_APP_TOKEN")
    return {"X-App-Token": token} if token else {}  # le token évite le throttling de l'API


def _http() -> requests.Session:
    """Session HTTP qui réessaie automatiquement une page en cas de lenteur ou d'erreur passagère.

    Seule la page en échec est rejouée (pas tout le jour), avec une attente croissante
    entre les essais : 2 s, 4 s, 8 s, 16 s, 32 s.
    """
    retry = Retry(
        total=5,
        connect=5,
        read=5,  # couvre les "Read timed out"
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],  # 429 = trop de requêtes (throttling)
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(_soda_headers())
    return session


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )


def _day_bounds(day: str) -> tuple[str, str]:
    """'2022-07-01' -> ('2022-07-01T00:00:00', '2022-07-02T00:00:00')"""
    start = date.fromisoformat(day)
    end = start + timedelta(days=1)
    return f"{start.isoformat()}T00:00:00", f"{end.isoformat()}T00:00:00"


def _day_filter(day: str) -> str:
    start, end = _day_bounds(day)
    return f"trip_start_timestamp >= '{start}' AND trip_start_timestamp < '{end}'"


def _clear_prefix(s3, prefix: str) -> None:
    """Supprime une partition existante pour rendre la ré-exécution idempotente."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=BRONZE_BUCKET, Delete={"Objects": keys})


def count_day_rows(day: str) -> int:
    """Nombre de trajets publiés par la source pour ce jour (calculé côté API, rien n'est téléchargé)."""
    resp = _http().get(
        f"https://data.cityofchicago.org/resource/{SODA_DATASET_ID}.json",
        params={"$select": "count(*) AS n", "$where": _day_filter(day)},
        timeout=SODA_TIMEOUT,
    )
    resp.raise_for_status()
    return int(resp.json()[0]["n"])


def ingest_day(day: str) -> dict:
    """Télécharge tous les trajets du jour `day` (format YYYY-MM-DD) dans le bronze."""
    prefix = f"{DATASET}/day={day}/"
    s3 = _s3()
    _clear_prefix(s3, prefix)

    http = _http()
    where = _day_filter(day)
    offset, page_num, total_rows = 0, 0, 0

    while True:
        limit = PAGE_SIZE if not MAX_ROWS else min(PAGE_SIZE, MAX_ROWS - total_rows)
        if limit <= 0:
            break

        resp = http.get(
            SODA_URL,
            params={
                "$where": where,
                "$order": ":id",  # tri stable, indispensable pour une pagination fiable
                "$limit": limit,
                "$offset": offset,
            },
            timeout=SODA_TIMEOUT,
        )
        resp.raise_for_status()

        body = resp.content
        # Comptage via le parseur CSV (robuste aux retours à la ligne entre guillemets), -1 pour l'en-tête
        n_rows = max(sum(1 for _ in csv.reader(io.StringIO(body.decode("utf-8")))) - 1, 0)
        if n_rows == 0:
            break

        page_num += 1
        key = f"{prefix}page_{page_num:04d}.csv"
        s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=body)
        total_rows += n_rows
        offset += n_rows
        log.info("Page %d : %d lignes -> s3://%s/%s", page_num, n_rows, BRONZE_BUCKET, key)

        if n_rows < limit:
            break  # dernière page

    manifest = {
        "dataset": DATASET,
        "day": day,
        "source": SODA_URL,
        "where": where,
        "pages": page_num,
        "rows": total_rows,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=f"{prefix}_manifest.json",
        Body=json.dumps(manifest, indent=2).encode(),
    )

    if total_rows == 0:
        raise ValueError(f"Aucune ligne récupérée pour {day} : vérifier la période ou l'API")

    log.info("Jour %s terminé : %d lignes, %d pages", day, total_rows, page_num)
    return manifest