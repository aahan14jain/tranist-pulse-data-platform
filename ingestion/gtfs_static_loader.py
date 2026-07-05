"""
Load GTFS static reference schedule data for Transit Pulse.

This module downloads a transit agency's published GTFS static feed (a ZIP of
CSV files) and lands parsed tables in the raw object store. That reference
schedule — stops, routes, trips, and stop times — is what realtime vehicle
position events are later joined against to measure reliability and on-time
performance.

This is a personal portfolio project using publicly available feeds (e.g.
King County Metro); it is not affiliated with any transit agency.
"""

from __future__ import annotations

import logging
import os
import zipfile
from datetime import date
from io import BytesIO, StringIO
from typing import Any

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

RAW_BUCKET = "raw-zone"

GTFS_FILES: dict[str, str] = {
    "stops.txt": "stops",
    "routes.txt": "routes",
    "trips.txt": "trips",
    "stop_times.txt": "stop_times",
}

REQUIRED_COLUMNS: dict[str, list[str]] = {
    "stops.txt": ["stop_id", "stop_name", "stop_lat", "stop_lon"],
    "routes.txt": ["route_id", "route_type"],
    "trips.txt": ["route_id", "service_id", "trip_id"],
    "stop_times.txt": ["trip_id", "stop_id", "stop_sequence"],
}

STRING_ID_COLUMNS: dict[str, list[str]] = {
    "stops.txt": ["stop_id"],
    "routes.txt": ["route_id"],
    "trips.txt": ["route_id", "service_id", "trip_id"],
    "stop_times.txt": ["trip_id", "stop_id"],
}


class GtfsValidationError(ValueError):
    """Raised when a GTFS file or required column is missing."""


def download_gtfs_zip(url: str, session: requests.Session | None = None) -> bytes:
    """Download the GTFS static feed ZIP from the given URL."""
    http = session or requests.Session()
    response = http.get(url, timeout=120)
    response.raise_for_status()
    return response.content


def _find_zip_member(members: list[str], filename: str) -> str | None:
    """Match a GTFS CSV at the ZIP root or one directory level deep."""
    target = filename.lower()
    for member in members:
        normalized = member.replace("\\", "/").lstrip("./")
        base = os.path.basename(normalized).lower()
        if base == target:
            return member
    return None


def _validate_columns(df: pd.DataFrame, gtfs_file: str) -> None:
    """Validate required GTFS columns are present; raise if any are missing."""
    missing = [col for col in REQUIRED_COLUMNS[gtfs_file] if col not in df.columns]
    if missing:
        raise GtfsValidationError(
            f"GTFS file {gtfs_file} is missing required column(s): {', '.join(missing)}"
        )

    if gtfs_file == "routes.txt":
        if "route_short_name" not in df.columns and "route_long_name" not in df.columns:
            raise GtfsValidationError(
                "GTFS file routes.txt is missing required column(s): "
                "route_short_name or route_long_name"
            )


def _read_gtfs_csv(csv_text: str, gtfs_file: str) -> pd.DataFrame:
    """Parse a GTFS CSV with ID fields forced to string dtype."""
    id_columns = STRING_ID_COLUMNS[gtfs_file]
    dtype = {column: str for column in id_columns}
    return pd.read_csv(StringIO(csv_text), dtype=dtype)


def parse_gtfs_zip(zip_bytes: bytes) -> dict[str, pd.DataFrame]:
    """
    Unzip GTFS static data in memory and parse the core schedule tables.

    Returns a mapping of table name (e.g. ``stops``) to DataFrame.
    """
    tables: dict[str, pd.DataFrame] = {}

    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        members = archive.namelist()

        for gtfs_file, table_name in GTFS_FILES.items():
            member = _find_zip_member(members, gtfs_file)
            if member is None:
                raise GtfsValidationError(f"Missing required GTFS file: {gtfs_file}")

            csv_text = archive.read(member).decode("utf-8-sig")
            df = _read_gtfs_csv(csv_text, gtfs_file)
            _validate_columns(df, gtfs_file)
            tables[table_name] = df

    return tables


def _s3_key(table_name: str, ingestion_date: str) -> str:
    return f"gtfs-static/{table_name}/{ingestion_date}/{table_name}.parquet"


def write_tables_to_minio(
    tables: dict[str, pd.DataFrame],
    s3_client: Any,
    bucket: str = RAW_BUCKET,
    ingestion_date: str | None = None,
) -> dict[str, str]:
    """
    Write parsed GTFS tables as Parquet objects to MinIO (S3-compatible).

    Returns a mapping of table name to the object key written.
    """
    load_date = ingestion_date or date.today().isoformat()
    written: dict[str, str] = {}

    for table_name, df in tables.items():
        key = _s3_key(table_name, load_date)
        try:
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        except Exception as exc:
            # Surface the specific table so Airflow/task logs are actionable.
            raise RuntimeError(
                f"Failed to load GTFS static table {table_name!r} "
                f"(bucket={bucket!r}, key={key!r}): {exc}"
            ) from exc
        written[table_name] = key
        logger.info("Wrote GTFS static table %s -> s3://%s/%s", table_name, bucket, key)

    return written


def _build_s3_client(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region_name: str,
) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name,
    )


def _ensure_bucket(s3_client: Any, bucket: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError:
        s3_client.create_bucket(Bucket=bucket)


def load_gtfs_static(
    gtfs_url: str | None = None,
    ingestion_date: str | None = None,
    session: requests.Session | None = None,
    s3_client: Any | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Download, parse, and persist GTFS static tables end to end.

    Environment variables (see ``.env.example``):
    GTFS_STATIC_URL, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, AWS_REGION
    """
    load_dotenv()

    url = gtfs_url or os.environ.get("GTFS_STATIC_URL")
    if not url:
        raise ValueError("GTFS_STATIC_URL is not set")

    zip_bytes = download_gtfs_zip(url, session=session)
    tables = parse_gtfs_zip(zip_bytes)

    if s3_client is None:
        endpoint = os.environ.get("MINIO_ENDPOINT")
        access_key = os.environ.get("MINIO_ACCESS_KEY")
        secret_key = os.environ.get("MINIO_SECRET_KEY")
        region = os.environ.get("AWS_REGION", "us-west-2")

        if not all([endpoint, access_key, secret_key]):
            raise ValueError(
                "MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set"
            )

        s3_client = _build_s3_client(endpoint, access_key, secret_key, region)
        _ensure_bucket(s3_client, RAW_BUCKET)

    write_tables_to_minio(
        tables,
        s3_client=s3_client,
        ingestion_date=ingestion_date,
    )

    return tables


def main() -> None:
    """Run a full GTFS static load and print per-table row counts."""
    tables = load_gtfs_static()
    for table_name, df in tables.items():
        print(f"{table_name}: {len(df)} rows")


if __name__ == "__main__":
    main()
