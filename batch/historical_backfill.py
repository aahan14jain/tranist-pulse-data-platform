"""
PySpark batch job: historical delay backfill vs GTFS schedule.

Reprocesses archived vehicle-position Parquet from MinIO raw-zone over an
arbitrary date range, applying the same join/delay logic as the streaming
delay calculator. Writes curated route-delay-windows Parquet to the same
curated-zone path structure so downstream Redshift loading is source-agnostic.

Personal portfolio project; not affiliated with any transit agency.
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta
from typing import Sequence

import pyspark
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col

from common.delay_logic import (
    CURATED_BUCKET,
    JOIN_MODE_TRIP_ID_NEAREST,
    JOIN_MODE_TRIP_ID_STOP_ID,
    JoinMode,
    RAW_BUCKET,
    VEHICLE_POSITIONS_PREFIX,
    aggregate_windowed_delays,
    compute_delay_seconds,
    join_positions_to_schedule,
    load_schedule_dataframe,
    measure_stop_field_null_rates,
    select_join_strategy,
    write_curated_route_delay_windows,
)

logger = logging.getLogger(__name__)

LOCAL_SHUFFLE_PARTITIONS = 8
SPARK_VERSION = pyspark.__version__


def build_spark_session(
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> SparkSession:
    """
    Create a SparkSession tuned for local / in-container execution against MinIO.

    Memory and parallelism are overridable via env so Airflow workers can run
    with a smaller footprint than a laptop Spark session.
    """
    master = os.environ.get("SPARK_MASTER", "local[*]")
    shuffle_partitions = int(
        os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", str(LOCAL_SHUFFLE_PARTITIONS))
    )
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    max_result_size = os.environ.get("SPARK_DRIVER_MAX_RESULT_SIZE", "1g")

    return (
        SparkSession.builder.appName("transit-pulse-historical-backfill")
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.maxResultSize", max_result_size)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(shuffle_partitions))
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def parse_iso_date(value: str) -> date:
    """Parse a YYYY-MM-DD date string."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}; expected YYYY-MM-DD"
        ) from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """CLI: --start-date and --end-date (inclusive, YYYY-MM-DD)."""
    parser = argparse.ArgumentParser(
        description=(
            "Backfill curated route-delay-windows from historical vehicle-position "
            "Parquet in MinIO raw-zone."
        )
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=parse_iso_date,
        help="Inclusive start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=parse_iso_date,
        help="Inclusive end date (YYYY-MM-DD)",
    )
    args = parser.parse_args(argv)
    if args.end_date < args.start_date:
        parser.error("--end-date must be on or after --start-date")
    return args


def iter_dates(start: date, end: date) -> list[date]:
    """Inclusive list of calendar dates from start through end."""
    days = (end - start).days
    return [start + timedelta(days=offset) for offset in range(days + 1)]


def vehicle_positions_path(raw_bucket: str, day: date) -> str:
    """Raw-zone path for one day's archived vehicle-position Parquet."""
    return f"s3a://{raw_bucket}/{VEHICLE_POSITIONS_PREFIX}/{day.isoformat()}/"


def _path_exists(spark: SparkSession, path: str) -> bool:
    """Return True when the S3A/Hadoop path exists."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI(path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    return bool(fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)))


def read_vehicle_positions_for_date_range(
    spark: SparkSession,
    raw_bucket: str,
    start_date: date,
    end_date: date,
) -> tuple[DataFrame | None, list[str], list[str]]:
    """
    Read vehicle-position Parquet for each day in [start_date, end_date].

    Returns (positions_df or None, dates_with_data, dates_missing).
    Missing days are flagged rather than treated as empty success.
    """
    dates_with_data: list[str] = []
    dates_missing: list[str] = []
    frames: list[DataFrame] = []

    for day in iter_dates(start_date, end_date):
        day_str = day.isoformat()
        path = vehicle_positions_path(raw_bucket, day)
        if not _path_exists(spark, path):
            logger.warning("No vehicle-position data at %s", path)
            dates_missing.append(day_str)
            continue

        day_df = spark.read.parquet(path)
        if day_df.rdd.isEmpty():
            logger.warning("Empty vehicle-position Parquet at %s", path)
            dates_missing.append(day_str)
            continue

        frames.append(day_df)
        dates_with_data.append(day_str)

    if not frames:
        return None, dates_with_data, dates_missing

    positions = frames[0]
    for frame in frames[1:]:
        positions = positions.unionByName(frame, allowMissingColumns=True)

    return positions, dates_with_data, dates_missing


def resolve_join_strategy_from_positions(
    positions: DataFrame,
    join_mode: JoinMode = "auto",
) -> str:
    """Choose join strategy from stop_id population in the historical batch."""
    stats = measure_stop_field_null_rates(positions)
    strategy = select_join_strategy(stats, join_mode=join_mode)
    logger.info(
        "Selected join strategy=%s (join_mode=%s, stop_id_null_pct=%.1f%%)",
        strategy,
        join_mode,
        stats["stop_id_null_pct"],
    )
    return strategy


def routes_with_no_delay_data(
    positions: DataFrame,
    delayed: DataFrame,
) -> list[str]:
    """
    Routes present in input positions but absent from delay output.

    Gaps are flagged (not hidden) so operators can investigate join failures
    or missing schedule coverage.
    """
    input_routes = {
        row.route_id
        for row in positions.select(col("route_id").cast("string").alias("route_id"))
        .filter(col("route_id").isNotNull())
        .distinct()
        .collect()
    }
    delayed_routes = {
        row.route_id
        for row in delayed.select(col("route_id").cast("string").alias("route_id"))
        .distinct()
        .collect()
    }
    return sorted(input_routes - delayed_routes)


def print_backfill_summary(
    start_date: date,
    end_date: date,
    *,
    dates_with_data: list[str],
    dates_missing: list[str],
    input_row_count: int,
    delayed_row_count: int,
    curated_row_count: int,
    routes_without_data: list[str],
    join_strategy: str,
) -> None:
    """Print an operator-facing summary of the backfill run."""
    lines = [
        "",
        "=== historical backfill summary ===",
        f"Date range:              {start_date.isoformat()} → {end_date.isoformat()} (inclusive)",
        f"Join strategy:           {join_strategy}",
        f"Days with input data:    {len(dates_with_data)} ({', '.join(dates_with_data) or 'none'})",
        f"Days missing input data: {len(dates_missing)} ({', '.join(dates_missing) or 'none'})",
        f"Input vehicle events:    {input_row_count}",
        f"Events with delay:       {delayed_row_count}",
        f"Curated window rows:     {curated_row_count}",
    ]
    if routes_without_data:
        lines.append(
            f"Routes with no delay data ({len(routes_without_data)}): "
            f"{', '.join(routes_without_data)}"
        )
    else:
        lines.append("Routes with no delay data: none")
    print("\n".join(lines), flush=True)


def run_historical_backfill(
    spark: SparkSession,
    *,
    start_date: date,
    end_date: date,
    raw_bucket: str,
    curated_bucket: str,
    ingestion_date: str,
    join_mode: JoinMode = "auto",
) -> dict[str, object]:
    """Run the batch delay pipeline for [start_date, end_date] and write curated output."""
    positions, dates_with_data, dates_missing = read_vehicle_positions_for_date_range(
        spark,
        raw_bucket,
        start_date,
        end_date,
    )

    if positions is None:
        print_backfill_summary(
            start_date,
            end_date,
            dates_with_data=dates_with_data,
            dates_missing=dates_missing,
            input_row_count=0,
            delayed_row_count=0,
            curated_row_count=0,
            routes_without_data=[],
            join_strategy="n/a",
        )
        return {
            "dates_with_data": dates_with_data,
            "dates_missing": dates_missing,
            "input_row_count": 0,
            "delayed_row_count": 0,
            "curated_row_count": 0,
            "routes_without_data": [],
            "join_strategy": "n/a",
        }

    input_row_count = positions.count()
    join_strategy = resolve_join_strategy_from_positions(positions, join_mode=join_mode)
    schedule_df = load_schedule_dataframe(spark, raw_bucket, ingestion_date)

    # Batch nearest-arrival uses Spark SQL windows (no driver-side collect of the
    # full schedule). The broadcast-UDF path is reserved for streaming.
    trip_schedule_bc = None
    schedule_for_join = schedule_df
    use_sql_nearest = False
    if join_strategy == JOIN_MODE_TRIP_ID_STOP_ID:
        schedule_for_join = broadcast(schedule_df)
    elif join_strategy == JOIN_MODE_TRIP_ID_NEAREST:
        use_sql_nearest = True

    enriched = join_positions_to_schedule(
        positions,
        schedule_for_join,
        join_strategy,
        trip_schedule_bc=trip_schedule_bc,
        use_sql_nearest=use_sql_nearest,
    )
    delayed = compute_delay_seconds(enriched)
    delayed_row_count = delayed.count()
    routes_without_data = routes_with_no_delay_data(positions, delayed)

    windowed = aggregate_windowed_delays(delayed, with_watermark=False).cache()
    curated_row_count = windowed.count()
    write_curated_route_delay_windows(windowed, curated_bucket)
    windowed.unpersist()

    print_backfill_summary(
        start_date,
        end_date,
        dates_with_data=dates_with_data,
        dates_missing=dates_missing,
        input_row_count=input_row_count,
        delayed_row_count=delayed_row_count,
        curated_row_count=curated_row_count,
        routes_without_data=routes_without_data,
        join_strategy=join_strategy,
    )

    return {
        "dates_with_data": dates_with_data,
        "dates_missing": dates_missing,
        "input_row_count": input_row_count,
        "delayed_row_count": delayed_row_count,
        "curated_row_count": curated_row_count,
        "routes_without_data": routes_without_data,
        "join_strategy": join_strategy,
    }


def main(argv: Sequence[str] | None = None) -> None:
    """Run the historical delay backfill job in local mode."""
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    args = parse_args(argv)

    minio_endpoint = os.environ.get("MINIO_ENDPOINT")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
    ingestion_date = os.environ.get("GTFS_STATIC_INGESTION_DATE")
    join_mode_raw = os.environ.get("DELAY_CALCULATOR_JOIN_MODE", "auto").lower()
    if join_mode_raw not in {"auto", JOIN_MODE_TRIP_ID_STOP_ID, JOIN_MODE_TRIP_ID_NEAREST}:
        raise ValueError(
            "DELAY_CALCULATOR_JOIN_MODE must be one of: "
            "auto, trip_id_stop_id, trip_id_nearest"
        )
    join_mode: JoinMode = join_mode_raw  # type: ignore[assignment]

    if not all([minio_endpoint, minio_access_key, minio_secret_key]):
        raise ValueError("MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set")
    if not ingestion_date:
        raise ValueError(
            "GTFS_STATIC_INGESTION_DATE is not set (YYYY-MM-DD folder under raw-zone/gtfs-static/)"
        )

    spark = build_spark_session(minio_endpoint, minio_access_key, minio_secret_key)
    try:
        run_historical_backfill(
            spark,
            start_date=args.start_date,
            end_date=args.end_date,
            raw_bucket=os.environ.get("RAW_ZONE_BUCKET", RAW_BUCKET),
            curated_bucket=os.environ.get("CURATED_ZONE_BUCKET", CURATED_BUCKET),
            ingestion_date=ingestion_date,
            join_mode=join_mode,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
