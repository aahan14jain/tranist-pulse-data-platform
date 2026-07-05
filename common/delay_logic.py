"""
Shared delay-calculation logic for streaming and batch jobs.

Join strategies and delay math live here so both jobs produce identical
curated metrics and can be unit-tested with the same functions.

Personal portfolio project; not affiliated with any transit agency.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from pyspark import Broadcast
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    coalesce,
    col,
    count,
    date_format,
    from_unixtime,
    lit,
    monotonically_increasing_id,
    row_number,
    udf,
    window,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

RAW_BUCKET = "raw-zone"
CURATED_BUCKET = "curated-zone"
VEHICLE_POSITIONS_PREFIX = "vehicle-positions"
CURATED_ROUTE_DELAY_WINDOWS = "route-delay-windows"
WATERMARK_DELAY = "5 minutes"
WINDOW_DURATION = "5 minutes"
STOP_FIELD_SAMPLE_COUNT = 5
# Use trip_id + stop_id join when stop_id is null in <= 30% of sample messages.
STOP_ID_NULL_RATE_THRESHOLD = 0.30
JOIN_MODE_TRIP_ID_STOP_ID = "trip_id_stop_id"
JOIN_MODE_TRIP_ID_NEAREST = "trip_id_nearest"
JoinMode = Literal["auto", "trip_id_stop_id", "trip_id_nearest"]
SERVICE_TIMEZONE = ZoneInfo("America/Los_Angeles")

VEHICLE_POSITION_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), nullable=False),
        StructField("trip_id", StringType(), nullable=True),
        StructField("route_id", StringType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("timestamp", LongType(), nullable=True),
        StructField("current_stop_sequence", IntegerType(), nullable=True),
        StructField("stop_id", StringType(), nullable=True),
        StructField("current_status", StringType(), nullable=True),
    ]
)

NEAREST_SCHEDULE_MATCH_SCHEMA = StructType(
    [
        StructField("arrival_time", StringType(), nullable=True),
        StructField("schedule_route_id", StringType(), nullable=True),
        StructField("schedule_stop_id", StringType(), nullable=True),
        StructField("stop_sequence", IntegerType(), nullable=True),
        StructField("scheduled_arrival_epoch", LongType(), nullable=True),
    ]
)

# trip_id -> list of (arrival_time, schedule_route_id, schedule_stop_id, stop_sequence)
TripScheduleLookup = dict[str, list[tuple[str, str, str, int]]]

CURATED_OUTPUT_COLUMNS = (
    "route_id",
    "window_date",
    "window_start",
    "window_end",
    "avg_delay_seconds",
    "vehicle_event_count",
)


def arrival_time_to_epoch(
    arrival_time: str | None,
    actual_timestamp: int | None,
) -> int | None:
    """Pure-Python GTFS arrival_time → Unix epoch (shared by Spark UDFs)."""
    if arrival_time is None or actual_timestamp is None:
        return None

    local_event = datetime.fromtimestamp(actual_timestamp, tz=SERVICE_TIMEZONE)
    service_date = local_event.date()

    parts = arrival_time.split(":")
    if len(parts) != 3:
        return None

    hours, minutes, seconds = (int(parts[0]), int(parts[1]), int(parts[2]))
    extra_days = hours // 24
    hours = hours % 24

    scheduled_local = datetime(
        service_date.year,
        service_date.month,
        service_date.day,
        hours,
        minutes,
        seconds,
        tzinfo=SERVICE_TIMEZONE,
    ) + timedelta(days=extra_days)

    return int(scheduled_local.timestamp())


@udf(LongType())
def gtfs_arrival_to_epoch(arrival_time: str | None, actual_timestamp: int | None) -> int | None:
    """
    Convert a GTFS stop_times arrival_time (HH:MM:SS, may exceed 24:00) to
    Unix epoch seconds using the service day implied by actual_timestamp.
    """
    return arrival_time_to_epoch(arrival_time, actual_timestamp)


def make_nearest_schedule_lookup_udf(broadcast_lookup):
    """UDF factory: pick the scheduled stop nearest to a vehicle event timestamp."""

    @udf(NEAREST_SCHEDULE_MATCH_SCHEMA)
    def lookup_nearest_schedule_stop(
        trip_id: str | None,
        timestamp: int | None,
    ) -> tuple[str, str, str, int, int] | None:
        if trip_id is None or timestamp is None:
            return None

        stops = broadcast_lookup.value.get(trip_id)
        if not stops:
            return None

        best_match: tuple[str, str, str, int, int] | None = None
        best_delta: int | None = None
        for arrival_time, route_id, stop_id, stop_sequence in stops:
            scheduled_epoch = arrival_time_to_epoch(arrival_time, timestamp)
            if scheduled_epoch is None:
                continue
            delta = abs(timestamp - scheduled_epoch)
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_match = (arrival_time, route_id, stop_id, stop_sequence, scheduled_epoch)

        return best_match

    return lookup_nearest_schedule_stop


def load_schedule_dataframe(
    spark: SparkSession,
    raw_bucket: str,
    ingestion_date: str,
) -> DataFrame:
    """
    Load GTFS stop_times joined to trips for position-static lookup keys.

    The joined table is small enough to broadcast against vehicle positions.
    """
    stop_times_path = f"s3a://{raw_bucket}/gtfs-static/stop_times/{ingestion_date}/stop_times.parquet"
    trips_path = f"s3a://{raw_bucket}/gtfs-static/trips/{ingestion_date}/trips.parquet"

    stop_times = spark.read.parquet(stop_times_path)
    trips = spark.read.parquet(trips_path)

    return stop_times.join(
        trips.select(
            col("trip_id").cast("string").alias("trip_id"),
            col("route_id").cast("string").alias("schedule_route_id"),
        ),
        on="trip_id",
        how="inner",
    ).select(
        col("trip_id").cast("string").alias("trip_id"),
        col("schedule_route_id"),
        col("stop_id").cast("string").alias("schedule_stop_id"),
        col("stop_sequence").cast("integer").alias("stop_sequence"),
        col("arrival_time").cast("string").alias("arrival_time"),
    )


def build_trip_schedule_broadcast(
    spark: SparkSession,
    schedule: DataFrame,
) -> Broadcast[TripScheduleLookup]:
    """
    Group schedule rows by trip_id for per-event nearest-stop lookup.

    Structured Streaming does not support ROW_NUMBER() on stream DFs, so the
    trip_id-nearest strategy uses a driver-side broadcast map + UDF instead.
    """
    trip_stops: TripScheduleLookup = {}
    for row in schedule.collect():
        trip_stops.setdefault(row.trip_id, []).append(
            (row.arrival_time, row.schedule_route_id, row.schedule_stop_id, row.stop_sequence)
        )
    logger.info(
        "Built trip schedule lookup for nearest-arrival join (%d trips, %d stop_times rows)",
        len(trip_stops),
        sum(len(stops) for stops in trip_stops.values()),
    )
    return spark.sparkContext.broadcast(trip_stops)


def _is_null_or_blank(value: object) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def measure_stop_field_null_rates(positions: DataFrame) -> dict[str, object]:
    """Compute null rates for current_stop_sequence and stop_id in a positions sample."""
    rows = positions.select("current_stop_sequence", "stop_id").collect()
    total = len(rows)
    if total == 0:
        return {
            "total": 0,
            "stop_sequence_null_count": 0,
            "stop_sequence_null_pct": 100.0,
            "stop_id_null_count": 0,
            "stop_id_null_pct": 100.0,
            "sample_stop_ids": [],
        }

    stop_sequence_null_count = sum(1 for row in rows if row.current_stop_sequence is None)
    stop_id_null_count = sum(1 for row in rows if _is_null_or_blank(row.stop_id))
    sample_stop_ids = [
        row.stop_id for row in rows if not _is_null_or_blank(row.stop_id)
    ][:STOP_FIELD_SAMPLE_COUNT]

    return {
        "total": total,
        "stop_sequence_null_count": stop_sequence_null_count,
        "stop_sequence_null_pct": 100.0 * stop_sequence_null_count / total,
        "stop_id_null_count": stop_id_null_count,
        "stop_id_null_pct": 100.0 * stop_id_null_count / total,
        "sample_stop_ids": sample_stop_ids,
    }


def select_join_strategy(
    stats: dict[str, object],
    join_mode: JoinMode = "auto",
) -> str:
    """Pick position-static join strategy from stop_id population in a sample."""
    if join_mode == JOIN_MODE_TRIP_ID_STOP_ID:
        return JOIN_MODE_TRIP_ID_STOP_ID
    if join_mode == JOIN_MODE_TRIP_ID_NEAREST:
        return JOIN_MODE_TRIP_ID_NEAREST

    stop_id_null_rate = float(stats["stop_id_null_pct"]) / 100.0
    if stop_id_null_rate <= STOP_ID_NULL_RATE_THRESHOLD:
        return JOIN_MODE_TRIP_ID_STOP_ID
    return JOIN_MODE_TRIP_ID_NEAREST


def join_positions_to_schedule_by_stop_id(
    positions: DataFrame,
    schedule: DataFrame,
) -> DataFrame:
    """Join vehicle positions to schedule on trip_id + stop_id (stop_times.stop_id)."""
    return positions.join(
        schedule,
        (positions.trip_id == schedule.trip_id)
        & (positions.stop_id == schedule.schedule_stop_id),
        how="left",
    ).drop(schedule.trip_id)


def join_positions_to_schedule_nearest_arrival(
    positions: DataFrame,
    trip_schedule_bc,
) -> DataFrame:
    """
    Look up the scheduled stop whose arrival_time is closest to the event
    timestamp via a broadcast trip_id → stop_times map.

    Streaming-safe (no window over the stream), but collects the full schedule
    onto the driver. Prefer join_positions_to_schedule_nearest_arrival_sql for
    batch jobs in memory-constrained workers (e.g. Airflow).
    """
    lookup_udf = make_nearest_schedule_lookup_udf(trip_schedule_bc)
    with_match = positions.withColumn(
        "_schedule_match",
        lookup_udf(col("trip_id"), col("timestamp")),
    )
    position_columns = [col(name) for name in positions.columns]
    return with_match.select(
        *position_columns,
        col("_schedule_match.arrival_time").alias("arrival_time"),
        col("_schedule_match.schedule_route_id").alias("schedule_route_id"),
        col("_schedule_match.schedule_stop_id").alias("schedule_stop_id"),
        col("_schedule_match.stop_sequence").alias("stop_sequence"),
        col("_schedule_match.scheduled_arrival_epoch").alias("scheduled_arrival_epoch"),
    ).filter(col("scheduled_arrival_epoch").isNotNull())


def join_positions_to_schedule_nearest_arrival_sql(
    positions: DataFrame,
    schedule: DataFrame,
) -> DataFrame:
    """
    Batch nearest-arrival join using trip_id join + row_number().

    Same delay semantics as the broadcast-UDF path, but keeps the schedule in
    Spark executors instead of collecting ~1M stop_times onto the driver (which
    OOMs Airflow workers during delayed.count()).
    """
    events = positions.withColumn("_event_id", monotonically_increasing_id())
    joined = events.alias("p").join(
        schedule.alias("s"),
        col("p.trip_id") == col("s.trip_id"),
        how="inner",
    )
    with_epoch = joined.withColumn(
        "scheduled_arrival_epoch",
        gtfs_arrival_to_epoch(col("s.arrival_time"), col("p.timestamp")),
    ).filter(col("scheduled_arrival_epoch").isNotNull())
    with_delta = with_epoch.withColumn(
        "_time_delta",
        spark_abs(col("p.timestamp").cast("long") - col("scheduled_arrival_epoch")),
    )
    best = (
        with_delta.withColumn(
            "_rn",
            row_number().over(
                Window.partitionBy("p._event_id").orderBy(
                    col("_time_delta").asc(),
                    col("s.stop_sequence").asc(),
                )
            ),
        )
        .filter(col("_rn") == 1)
    )
    position_columns = [col(f"p.{name}").alias(name) for name in positions.columns]
    return best.select(
        *position_columns,
        col("s.arrival_time").alias("arrival_time"),
        col("s.schedule_route_id").alias("schedule_route_id"),
        col("s.schedule_stop_id").alias("schedule_stop_id"),
        col("s.stop_sequence").alias("stop_sequence"),
        col("scheduled_arrival_epoch"),
    )


def join_positions_to_schedule(
    positions: DataFrame,
    schedule: DataFrame,
    join_strategy: str,
    trip_schedule_bc=None,
    *,
    use_sql_nearest: bool = False,
) -> DataFrame:
    """Dispatch to the selected position-static schedule join strategy."""
    if join_strategy == JOIN_MODE_TRIP_ID_STOP_ID:
        return join_positions_to_schedule_by_stop_id(positions, schedule)
    if join_strategy == JOIN_MODE_TRIP_ID_NEAREST:
        if use_sql_nearest:
            return join_positions_to_schedule_nearest_arrival_sql(positions, schedule)
        if trip_schedule_bc is None:
            raise ValueError("trip_schedule_bc is required for trip_id_nearest join strategy")
        return join_positions_to_schedule_nearest_arrival(positions, trip_schedule_bc)
    raise ValueError(f"Unsupported join strategy: {join_strategy}")


def compute_delay_seconds(enriched: DataFrame) -> DataFrame:
    """Attach scheduled_arrival_epoch and delay_seconds for each vehicle event."""
    if "scheduled_arrival_epoch" in enriched.columns:
        with_schedule = enriched
    else:
        with_schedule = enriched.withColumn(
            "scheduled_arrival_epoch",
            gtfs_arrival_to_epoch(col("arrival_time"), col("timestamp")),
        )

    return (
        with_schedule.withColumn(
            "route_id",
            coalesce(col("route_id").cast("string"), col("schedule_route_id")),
        )
        .withColumn(
            "delay_seconds",
            col("timestamp").cast("double") - col("scheduled_arrival_epoch").cast("double"),
        )
        .filter(col("delay_seconds").isNotNull() & col("route_id").isNotNull())
    )


def aggregate_windowed_delays(
    delayed_events: DataFrame,
    *,
    with_watermark: bool = False,
) -> DataFrame:
    """
    Aggregate per-route delay into 5-minute tumbling windows.

    Watermark (streaming only):
      Vehicle GPS events can arrive out of order over the network. The watermark
      tells Spark how long to keep partial window state while still accepting
      late records. Five minutes matches our poll interval and typical agency
      update cadence without hoarding unbounded state on a laptop.

    Tumbling 5-minute windows:
      Non-overlapping buckets make per-route delay easy to chart and reason
      about in interviews ("average lateness for route 7 between 8:00–8:05").
      Tumbling (vs sliding) keeps one row per route per window for idempotent
      partition overwrites downstream.
    """
    event_timed = delayed_events.withColumn(
        "event_time",
        from_unixtime(col("timestamp")).cast("timestamp"),
    )
    if with_watermark:
        event_timed = event_timed.withWatermark("event_time", WATERMARK_DELAY)

    return (
        event_timed.groupBy(
            window(col("event_time"), WINDOW_DURATION),
            col("route_id"),
        )
        .agg(
            avg("delay_seconds").alias("avg_delay_seconds"),
            count(lit(1)).alias("vehicle_event_count"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("window_date", date_format(col("window.start"), "yyyy-MM-dd"))
        .drop("window")
    )


def curated_route_delay_windows_path(curated_bucket: str) -> str:
    """MinIO path for curated route-delay-windows Parquet (streaming and batch)."""
    return f"s3a://{curated_bucket}/{CURATED_ROUTE_DELAY_WINDOWS}"


def write_curated_route_delay_windows(
    windowed_df: DataFrame,
    curated_bucket: str,
) -> None:
    """
    Write windowed delay rows with dynamic partition overwrite.

    Partitioned by route_id + window_date so reruns/backfills replace only the
    slices present in this write instead of appending duplicates.
    """
    if windowed_df.rdd.isEmpty():
        return

    output_path = curated_route_delay_windows_path(curated_bucket)
    spark = windowed_df.sparkSession
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        windowed_df.select(*CURATED_OUTPUT_COLUMNS)
        .write.mode("overwrite")
        .partitionBy("route_id", "window_date")
        .parquet(output_path)
    )
