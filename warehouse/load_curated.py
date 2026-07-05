"""
Load curated route-delay windows into the analytics warehouse.

Local-dev approach
------------------
Amazon Redshift is not practical on a laptop, so this module uses a local
Postgres table with the same logical schema as warehouse/redshift_schema.sql
(route_delay_metrics). Postgres speaks the same wire protocol family and is
enough to exercise delete-then-insert load logic, connection config, and
row-level mapping from curated Parquet (MinIO) → warehouse.

Real Redshift (when this leaves the laptop)
-------------------------------------------
1. Point WarehouseConnectionConfig at the Redshift cluster (host/port/db/user
   password or IAM auth) and set backend="redshift".
2. Prefer COPY from S3 curated Parquet (see the commented template in
   redshift_schema.sql) instead of row-wise INSERT — swap only
   write_metrics()'s backend branch, not the read/idempotency flow.
3. Keep the same delete-for-date-range step before COPY so reloads stay
   idempotent.

Personal portfolio project; not affiliated with any transit agency.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from typing import Any, Literal, Sequence

import boto3
import psycopg2
import pyarrow.parquet as pq
from botocore.client import BaseClient
from dotenv import load_dotenv
from psycopg2.extensions import connection as PgConnection

from common.delay_logic import CURATED_BUCKET, CURATED_ROUTE_DELAY_WINDOWS

logger = logging.getLogger(__name__)

TABLE_NAME = "route_delay_metrics"

# Postgres stand-in DDL (no DISTKEY/SORTKEY — those are Redshift-only).
POSTGRES_CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    route_id           VARCHAR(64)       NOT NULL,
    window_start       TIMESTAMP         NOT NULL,
    window_end         TIMESTAMP         NOT NULL,
    avg_delay_seconds  DOUBLE PRECISION,
    vehicle_count      BIGINT,
    computed_at        TIMESTAMP         NOT NULL DEFAULT NOW()
)
"""

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    route_id,
    window_start,
    window_end,
    avg_delay_seconds,
    vehicle_count,
    computed_at
) VALUES (%s, %s, %s, %s, %s, %s)
"""

# Redshift COPY template — used when backend=redshift and s3_copy_uri is set.
REDSHIFT_COPY_SQL_TEMPLATE = """
COPY {table} (
    route_id,
    window_start,
    window_end,
    avg_delay_seconds,
    vehicle_count
)
FROM %s
IAM_ROLE %s
FORMAT AS PARQUET
"""


WarehouseBackend = Literal["postgres", "redshift"]


@dataclass(frozen=True)
class WarehouseConnectionConfig:
    """Connection parameters for local Postgres or Amazon Redshift."""

    host: str
    port: int
    database: str
    user: str
    password: str
    backend: WarehouseBackend = "postgres"
    # Redshift-only: IAM role ARN for COPY from S3.
    redshift_copy_iam_role: str | None = None

    @classmethod
    def from_env(cls) -> WarehouseConnectionConfig:
        backend_raw = os.environ.get("WAREHOUSE_BACKEND", "postgres").lower()
        if backend_raw not in {"postgres", "redshift"}:
            raise ValueError("WAREHOUSE_BACKEND must be 'postgres' or 'redshift'")
        return cls(
            # Prefer 127.0.0.1 over localhost so we hit Docker-published 5432,
            # not a host Postgres bound only on ::1.
            host=os.environ.get("WAREHOUSE_HOST", "127.0.0.1"),
            port=int(os.environ.get("WAREHOUSE_PORT", "5433")),
            database=os.environ.get("WAREHOUSE_DATABASE", "warehouse"),
            user=os.environ.get("WAREHOUSE_USER", "airflow"),
            password=os.environ.get("WAREHOUSE_PASSWORD", "airflow"),
            backend=backend_raw,  # type: ignore[arg-type]
            redshift_copy_iam_role=os.environ.get("REDSHIFT_COPY_IAM_ROLE"),
        )


@dataclass(frozen=True)
class CuratedSourceConfig:
    """MinIO / S3 location of curated route-delay-windows Parquet."""

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    prefix: str = CURATED_ROUTE_DELAY_WINDOWS
    region_name: str = "us-west-2"

    @classmethod
    def from_env(cls) -> CuratedSourceConfig:
        endpoint = os.environ.get("MINIO_ENDPOINT")
        access_key = os.environ.get("MINIO_ACCESS_KEY")
        secret_key = os.environ.get("MINIO_SECRET_KEY")
        if not all([endpoint, access_key, secret_key]):
            raise ValueError(
                "MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set"
            )
        return cls(
            endpoint_url=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket=os.environ.get("CURATED_ZONE_BUCKET", CURATED_BUCKET),
            region_name=os.environ.get("AWS_REGION", "us-west-2"),
        )


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}; expected YYYY-MM-DD"
        ) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Idempotently load curated route-delay windows into the warehouse "
            "for an inclusive date range."
        )
    )
    parser.add_argument("--start-date", required=True, type=parse_iso_date)
    parser.add_argument("--end-date", required=True, type=parse_iso_date)
    args = parser.parse_args(argv)
    if args.end_date < args.start_date:
        parser.error("--end-date must be on or after --start-date")
    return args


def build_s3_client(source: CuratedSourceConfig) -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=source.endpoint_url,
        aws_access_key_id=source.access_key,
        aws_secret_access_key=source.secret_key,
        region_name=source.region_name,
    )


def _partition_value(key: str, partition_name: str) -> str | None:
    prefix = f"{partition_name}="
    for part in key.split("/"):
        if part.startswith(prefix):
            return part.split("=", 1)[1]
    return None


def _window_date_from_key(key: str) -> date | None:
    raw = _partition_value(key, "window_date")
    if raw is None:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def list_curated_parquet_keys(
    s3_client: BaseClient,
    source: CuratedSourceConfig,
    start_date: date,
    end_date: date,
) -> list[str]:
    """List curated Parquet object keys whose window_date is in [start, end]."""
    prefix = f"{source.prefix.rstrip('/')}/"
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=source.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            window_date = _window_date_from_key(key)
            if window_date is None:
                continue
            if start_date <= window_date <= end_date:
                keys.append(key)
    return keys


def read_curated_rows(
    s3_client: BaseClient,
    source: CuratedSourceConfig,
    start_date: date,
    end_date: date,
    *,
    computed_at: datetime | None = None,
) -> list[tuple[Any, ...]]:
    """
    Read curated Parquet from MinIO/S3 and map to warehouse row tuples.

    Curated column vehicle_event_count → warehouse vehicle_count.
    """
    keys = list_curated_parquet_keys(s3_client, source, start_date, end_date)
    if not keys:
        logger.warning(
            "No curated Parquet objects for %s → %s under s3://%s/%s/",
            start_date,
            end_date,
            source.bucket,
            source.prefix,
        )
        return []

    stamp = computed_at or datetime.now(tz=timezone.utc).replace(tzinfo=None)
    rows: list[tuple[Any, ...]] = []
    for key in keys:
        # Hive-style partitions are path-only (not always in Parquet body).
        route_id_from_path = _partition_value(key, "route_id")
        body = s3_client.get_object(Bucket=source.bucket, Key=key)["Body"].read()
        table = pq.read_table(BytesIO(body))
        for batch in table.to_batches():
            for record in batch.to_pylist():
                route_id = record.get("route_id") or route_id_from_path
                if route_id is None:
                    raise KeyError(f"route_id missing from Parquet and path: {key}")
                rows.append(
                    (
                        str(route_id),
                        record["window_start"],
                        record["window_end"],
                        float(record["avg_delay_seconds"])
                        if record["avg_delay_seconds"] is not None
                        else None,
                        int(record["vehicle_event_count"])
                        if record["vehicle_event_count"] is not None
                        else None,
                        stamp,
                    )
                )
    logger.info("Read %d curated rows from %d Parquet objects", len(rows), len(keys))
    return rows


def connect(config: WarehouseConnectionConfig) -> PgConnection:
    """Open a DB-API connection (psycopg2 works for Postgres and Redshift)."""
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
    )


def ensure_warehouse_database(config: WarehouseConnectionConfig) -> None:
    """Create the warehouse database on local Postgres if it does not exist."""
    if config.backend != "postgres":
        return
    admin = psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname="postgres",
        user=config.user,
        password=config.password,
    )
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config.database,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{config.database}"')
                logger.info("Created database %s", config.database)
    finally:
        admin.close()


def ensure_table(conn: PgConnection) -> None:
    with conn.cursor() as cur:
        cur.execute(POSTGRES_CREATE_TABLE_SQL)
    conn.commit()


def delete_metrics_for_rows(
    conn: PgConnection,
    rows: list[tuple[Any, ...]],
) -> int:
    """
    Remove existing rows that match the natural key of the incoming batch.

    Curated Hive partitions use service-local window_date, while window_start
    may be stored in UTC — so delete-by-calendar-date alone is not idempotent.
    """
    if not rows:
        return 0
    keys = sorted({(row[0], row[1]) for row in rows})
    with conn.cursor() as cur:
        values_sql = b",".join(
            cur.mogrify("(%s, %s::timestamp)", key) for key in keys
        ).decode()
        cur.execute(
            f"""
            DELETE FROM {TABLE_NAME}
            WHERE (route_id, window_start) IN ({values_sql})
            """
        )
        deleted = cur.rowcount
    logger.info(
        "Deleted %d existing %s rows matching %d incoming keys",
        deleted,
        TABLE_NAME,
        len(keys),
    )
    return deleted


def insert_metrics(conn: PgConnection, rows: list[tuple[Any, ...]]) -> int:
    """Row-wise INSERT — fine for local Postgres; acceptable for small Redshift tests."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
    return len(rows)


def copy_metrics_from_s3(
    conn: PgConnection,
    config: WarehouseConnectionConfig,
    s3_uri: str,
) -> None:
    """
    Production Redshift path: COPY Parquet from S3.

    Requires redshift_copy_iam_role on the config. Local Postgres cannot run this.
    """
    if not config.redshift_copy_iam_role:
        raise ValueError(
            "REDSHIFT_COPY_IAM_ROLE must be set for backend=redshift COPY loads"
        )
    sql = REDSHIFT_COPY_SQL_TEMPLATE.format(table=TABLE_NAME)
    with conn.cursor() as cur:
        cur.execute(sql, (s3_uri, config.redshift_copy_iam_role))


def write_metrics(
    conn: PgConnection,
    config: WarehouseConnectionConfig,
    rows: list[tuple[Any, ...]],
    start_date: date,
    end_date: date,
    *,
    s3_copy_uri: str | None = None,
) -> int:
    """
    Idempotent load: delete matching keys, then INSERT (Postgres) or COPY (Redshift).

    Swapping to real Redshift is: point config at the cluster, set backend=redshift,
    and pass s3_copy_uri to use COPY instead of executemany.
    """
    if config.backend == "redshift" and s3_copy_uri:
        # Production path: range delete (Redshift TRUNC) then COPY from S3.
        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {TABLE_NAME}
                WHERE TRUNC(window_start) BETWEEN %s AND %s
                """,
                (start_date, end_date),
            )
        copy_metrics_from_s3(conn, config, s3_copy_uri)
        conn.commit()
        logger.info("COPY from %s completed for %s → %s", s3_copy_uri, start_date, end_date)
        return -1  # COPY does not report rowcount the same way

    delete_metrics_for_rows(conn, rows)
    inserted = insert_metrics(conn, rows)
    conn.commit()
    logger.info("Inserted %d rows into %s", inserted, TABLE_NAME)
    return inserted


def load_curated_for_date_range(
    start_date: date,
    end_date: date,
    *,
    warehouse: WarehouseConnectionConfig | None = None,
    source: CuratedSourceConfig | None = None,
    s3_copy_uri: str | None = None,
) -> dict[str, int]:
    """Read curated Parquet and idempotently load route_delay_metrics."""
    warehouse = warehouse or WarehouseConnectionConfig.from_env()
    source = source or CuratedSourceConfig.from_env()

    ensure_warehouse_database(warehouse)
    s3_client = build_s3_client(source)
    rows = read_curated_rows(s3_client, source, start_date, end_date)

    conn = connect(warehouse)
    try:
        ensure_table(conn)
        written = write_metrics(
            conn,
            warehouse,
            rows,
            start_date,
            end_date,
            s3_copy_uri=s3_copy_uri,
        )
    finally:
        conn.close()

    return {
        "rows_read": len(rows),
        "rows_written": written if written >= 0 else len(rows),
    }


def main(argv: Sequence[str] | None = None) -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    summary = load_curated_for_date_range(args.start_date, args.end_date)
    print(
        f"Loaded {summary['rows_written']} rows into {TABLE_NAME} "
        f"for {args.start_date} → {args.end_date} "
        f"(read {summary['rows_read']} from curated-zone)"
    )


if __name__ == "__main__":
    main()
