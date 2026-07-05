"""
On-demand historical delay backfill.

Unlike the daily static refresh, delay backfills are intentional and bounded:
an operator chooses a date range (e.g. after fixing join logic, or to rebuild
curated metrics for a service day). schedule=None keeps this DAG out of the
scheduler loop so it never runs by accident and only starts when triggered
with explicit start_date / end_date params.

Personal portfolio project; not affiliated with any transit agency.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "transit-pulse",
}


def _parse_iso_date(value: object, field_name: str) -> date:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty YYYY-MM-DD string")
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"{field_name}={value!r} is not a valid YYYY-MM-DD date"
        ) from exc


@dag(
    dag_id="historical_delay_backfill",
    description="Reprocess archived vehicle positions into curated delay windows",
    doc_md=__doc__,
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "start_date": Param(
            default="2026-07-01",
            type="string",
            format="date",
            title="Start date",
            description="Inclusive start date (YYYY-MM-DD) for the backfill window",
        ),
        "end_date": Param(
            default="2026-07-01",
            type="string",
            format="date",
            title="End date",
            description="Inclusive end date (YYYY-MM-DD) for the backfill window",
        ),
    },
    tags=["backfill", "batch", "delays", "minio"],
)
def historical_delay_backfill():
    """
    Manually triggered backfill over an operator-supplied date range.

    Params use Airflow's Param schema (format=date) so malformed dates fail at
    trigger time in the UI/API. A first task then enforces business rules
    (start <= end, not in the future) before any Spark work starts.
    """

    @task
    def validate_date_range() -> dict[str, str]:
        """Validate DAG run params before starting the Spark backfill."""
        from airflow.operators.python import get_current_context

        context = get_current_context()
        params = context["params"]
        start = _parse_iso_date(params.get("start_date"), "start_date")
        end = _parse_iso_date(params.get("end_date"), "end_date")
        today = date.today()

        if start > end:
            raise ValueError(
                f"Invalid date range: start_date ({start.isoformat()}) is after "
                f"end_date ({end.isoformat()})"
            )
        if start > today or end > today:
            raise ValueError(
                f"Invalid date range: start_date/end_date must not be in the future "
                f"(today={today.isoformat()}, start={start.isoformat()}, "
                f"end={end.isoformat()})"
            )

        logger.info(
            "Validated backfill date range %s → %s",
            start.isoformat(),
            end.isoformat(),
        )
        return {"start_date": start.isoformat(), "end_date": end.isoformat()}

    @task
    def run_historical_backfill_job(date_range: dict[str, str]) -> dict[str, object]:
        """Run batch.historical_backfill.run_historical_backfill for the range."""
        import os

        from batch.historical_backfill import build_spark_session, run_historical_backfill
        from common.delay_logic import (
            CURATED_BUCKET,
            JOIN_MODE_TRIP_ID_NEAREST,
            JOIN_MODE_TRIP_ID_STOP_ID,
            RAW_BUCKET,
        )

        start = date.fromisoformat(date_range["start_date"])
        end = date.fromisoformat(date_range["end_date"])

        minio_endpoint = os.environ.get("MINIO_ENDPOINT")
        minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
        minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
        ingestion_date = os.environ.get("GTFS_STATIC_INGESTION_DATE")
        join_mode_raw = os.environ.get("DELAY_CALCULATOR_JOIN_MODE", "auto").lower()

        if not all([minio_endpoint, minio_access_key, minio_secret_key]):
            raise ValueError(
                "MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set"
            )
        if not ingestion_date:
            raise ValueError(
                "GTFS_STATIC_INGESTION_DATE is not set "
                "(YYYY-MM-DD folder under raw-zone/gtfs-static/)"
            )
        if join_mode_raw not in {
            "auto",
            JOIN_MODE_TRIP_ID_STOP_ID,
            JOIN_MODE_TRIP_ID_NEAREST,
        }:
            raise ValueError(
                "DELAY_CALCULATOR_JOIN_MODE must be one of: "
                "auto, trip_id_stop_id, trip_id_nearest"
            )

        spark = build_spark_session(minio_endpoint, minio_access_key, minio_secret_key)
        try:
            result = run_historical_backfill(
                spark,
                start_date=start,
                end_date=end,
                raw_bucket=os.environ.get("RAW_ZONE_BUCKET", RAW_BUCKET),
                curated_bucket=os.environ.get("CURATED_ZONE_BUCKET", CURATED_BUCKET),
                ingestion_date=ingestion_date,
                join_mode=join_mode_raw,  # type: ignore[arg-type]
            )
        finally:
            # JVM may already be dead after an OOM; don't mask the original error.
            try:
                spark.stop()
            except Exception:
                logger.exception("Failed to stop SparkSession cleanly")

        logger.info("Backfill finished: %s", result)
        return result

    @task
    def sanity_check_curated_output(
        date_range: dict[str, str],
        backfill_result: dict[str, object],
    ) -> dict[str, object]:
        """
        Query curated-zone route-delay-windows and log row counts.

        Filters to window_date within the backfill range so the check reflects
        this run's slice, not the entire curated history.
        """
        import os

        from pyspark.sql.functions import col

        from batch.historical_backfill import build_spark_session
        from common.delay_logic import CURATED_BUCKET, curated_route_delay_windows_path

        start = date_range["start_date"]
        end = date_range["end_date"]
        curated_bucket = os.environ.get("CURATED_ZONE_BUCKET", CURATED_BUCKET)
        curated_path = curated_route_delay_windows_path(curated_bucket)

        minio_endpoint = os.environ.get("MINIO_ENDPOINT")
        minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
        minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
        if not all([minio_endpoint, minio_access_key, minio_secret_key]):
            raise ValueError(
                "MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set"
            )

        spark = build_spark_session(minio_endpoint, minio_access_key, minio_secret_key)
        try:
            curated = spark.read.parquet(curated_path)
            in_range = curated.filter(
                (col("window_date") >= start) & (col("window_date") <= end)
            )
            row_count = in_range.count()
            route_count = in_range.select("route_id").distinct().count()
            day_count = in_range.select("window_date").distinct().count()
        finally:
            spark.stop()

        expected_curated_rows = int(backfill_result.get("curated_row_count") or 0)
        summary = {
            "start_date": start,
            "end_date": end,
            "curated_row_count": row_count,
            "distinct_routes": route_count,
            "distinct_window_dates": day_count,
            "backfill_reported_curated_rows": expected_curated_rows,
        }
        logger.info(
            "Curated-zone sanity check for %s → %s: rows=%d routes=%d days=%d "
            "(backfill reported %d curated rows)",
            start,
            end,
            row_count,
            route_count,
            day_count,
            expected_curated_rows,
        )
        if row_count == 0 and expected_curated_rows > 0:
            raise ValueError(
                "Curated-zone sanity check found 0 rows in range "
                f"{start} → {end}, but backfill reported "
                f"{expected_curated_rows} curated rows"
            )
        return summary

    date_range = validate_date_range()
    backfill_result = run_historical_backfill_job(date_range)
    sanity_check_curated_output(date_range, backfill_result)


historical_delay_backfill()
