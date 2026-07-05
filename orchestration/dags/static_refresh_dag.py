"""
Daily GTFS static schedule refresh.

GTFS static feeds (stops, routes, trips, stop_times) change infrequently —
new service periods, route renumbers, seasonal schedules — but when they do,
every delay calculation that joins live or historical vehicle positions against
the schedule is wrong until the reference data is updated. A daily refresh is
cheap insurance: the feed is small, MinIO writes are idempotent by ingestion
date, and we avoid silent drift between the agency's published schedule and
what Transit Pulse uses for reliability metrics.

Personal portfolio project; not affiliated with any transit agency.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "transit-pulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="gtfs_static_refresh",
    description="Download GTFS static tables and land them in MinIO raw-zone",
    doc_md=__doc__,
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["gtfs", "static", "ingestion", "minio"],
)
def gtfs_static_refresh():
    """
    Orchestrate a full GTFS static load into MinIO.

    Scheduled daily because static schedules change on agency release cycles
    (often overnight), not continuously. One task keeps the graph honest:
    download → parse → write is a single unit of work with shared credentials
    and a single retry policy.
    """

    @task
    def load_gtfs_static_tables() -> dict[str, int]:
        """
        Run ingestion.gtfs_static_loader.load_gtfs_static.

        Per-table MinIO write failures raise RuntimeError naming the table so
        Airflow task logs show e.g. "Failed to load GTFS static table 'trips'"
        rather than a generic task failure.
        """
        # Import inside the task so DAG parsing does not require project deps
        # to be importable at scheduler heartbeat time.
        from ingestion.gtfs_static_loader import load_gtfs_static

        try:
            tables = load_gtfs_static()
        except Exception:
            logger.exception(
                "GTFS static refresh failed while loading tables into MinIO"
            )
            raise

        row_counts = {name: len(df) for name, df in tables.items()}
        for table_name, row_count in row_counts.items():
            logger.info("Loaded GTFS static table %s (%d rows)", table_name, row_count)
        return row_counts

    load_gtfs_static_tables()


gtfs_static_refresh()
