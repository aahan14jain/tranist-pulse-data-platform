"""
Spark Structured Streaming job: real-time delay vs GTFS schedule.

Reads vehicle-position JSON events from Kafka, joins each update against
broadcast GTFS static stop_times/trips (loaded from MinIO raw-zone by
gtfs_static_loader.py), and computes how late each vehicle is relative to
schedule. Join strategy is chosen at startup from a batch null-rate check on
stop_id (trip_id + stop_id when populated; otherwise trip_id-only with
nearest scheduled arrival_time). Results are aggregated into 5-minute
tumbling windows and written idempotently to MinIO curated-zone Parquet.

A second writeStream on the same parsed Kafka DataFrame archives raw
vehicle-position events to MinIO raw-zone (partitioned by event date) so
batch/historical_backfill.py can reprocess them later. Both sinks share one
SparkSession and one Kafka source plan; each has its own checkpoint.

Join/delay business logic lives in common.delay_logic so the batch backfill
job computes identical metrics.

Test coverage for Structured Streaming is intentionally deferred: verifying
this job end-to-end needs a memory-sink / checkpoint test harness, which is
flagged as a follow-up rather than silently skipping coverage.

Personal portfolio project; not affiliated with any transit agency.
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from typing import Callable

import pyspark
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import (
    broadcast,
    col,
    date_format,
    from_json,
    from_utc_timestamp,
)

from common.delay_logic import (
    CURATED_BUCKET,
    JOIN_MODE_TRIP_ID_NEAREST,
    JOIN_MODE_TRIP_ID_STOP_ID,
    JoinMode,
    RAW_BUCKET,
    VEHICLE_POSITIONS_PREFIX,
    VEHICLE_POSITION_SCHEMA,
    aggregate_windowed_delays,
    build_trip_schedule_broadcast,
    compute_delay_seconds,
    join_positions_to_schedule,
    join_positions_to_schedule_by_stop_id,
    join_positions_to_schedule_nearest_arrival,
    load_schedule_dataframe,
    measure_stop_field_null_rates,
    select_join_strategy,
    write_curated_route_delay_windows,
)

logger = logging.getLogger(__name__)

VEHICLE_POSITIONS_TOPIC = "vehicle-positions"
LOCAL_SHUFFLE_PARTITIONS = 8
TRIP_ID_DEBUG_SAMPLE_COUNT = 10
TRIP_ID_DEBUG_TIMEOUT_SECONDS = 90
TRIP_ID_MEMBERSHIP_KAFKA_BATCH_SIZE = 100
TRIP_ID_MEMBERSHIP_UNMATCHED_EXAMPLES = 5
STOP_FIELD_DIAGNOSTIC_BATCH_SIZE = 100
DEFAULT_CHECKPOINT_DIR = "/tmp/transit-pulse-delay-calculator-checkpoint"
DEBUG_CHECKPOINT_DIR = "/tmp/transit-pulse-delay-calculator-checkpoint-debug"
DEFAULT_ARCHIVER_CHECKPOINT_DIR = "/tmp/transit-pulse-archiver-checkpoint"
# Event-date partitions use the service timezone (not wall-clock ingestion time).
ARCHIVE_DATE_TIMEZONE = "America/Los_Angeles"
RAW_ARCHIVE_COLUMNS = (
    "vehicle_id",
    "trip_id",
    "route_id",
    "latitude",
    "longitude",
    "timestamp",
    "current_stop_sequence",
    "stop_id",
    "current_status",
)
# Kafka connector Scala binary (_2.13) must match Spark 4.x; version must match PySpark.
SPARK_VERSION = pyspark.__version__

# Re-export shared join helpers under the historical streaming names.
join_stream_to_schedule_by_stop_id = join_positions_to_schedule_by_stop_id
join_stream_to_schedule_nearest_arrival = join_positions_to_schedule_nearest_arrival
join_stream_to_schedule = join_positions_to_schedule


def build_spark_session(
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> SparkSession:
    """Create a SparkSession tuned for local laptop execution against MinIO."""
    return (
        SparkSession.builder.appName("transit-pulse-delay-calculator")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(LOCAL_SHUFFLE_PARTITIONS))
        .config("spark.default.parallelism", str(LOCAL_SHUFFLE_PARTITIONS))
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_vehicle_positions_stream(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
) -> DataFrame:
    """Read and parse the vehicle-positions Kafka topic as a typed stream."""
    # failOnDataLoss=false: local Kafka is often force-recreated, which resets
    # offsets while Spark checkpoints still hold the old high-water marks.
    # Prefer DELAY_CALCULATOR_RESET_CHECKPOINT=true after a full Kafka wipe.
    raw_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw_kafka.select(from_json(col("value").cast("string"), VEHICLE_POSITION_SCHEMA).alias("event"))
        .select("event.*")
        .filter(col("vehicle_id").isNotNull())
    )


def load_schedule_lookup(
    spark: SparkSession,
    raw_bucket: str,
    ingestion_date: str,
) -> DataFrame:
    """Broadcast-wrapped schedule lookup for stream-static joins."""
    return broadcast(load_schedule_dataframe(spark, raw_bucket, ingestion_date))


def sample_static_trip_ids(
    spark: SparkSession,
    raw_bucket: str,
    ingestion_date: str,
    sample_count: int = TRIP_ID_DEBUG_SAMPLE_COUNT,
) -> list[str]:
    """Read-only sample of trip_id values from GTFS static trips.parquet."""
    trips_path = f"s3a://{raw_bucket}/gtfs-static/trips/{ingestion_date}/trips.parquet"
    rows = (
        spark.read.parquet(trips_path)
        .select(col("trip_id").cast("string").alias("trip_id"))
        .limit(sample_count)
        .collect()
    )
    return [row.trip_id for row in rows]


def collect_kafka_trip_id_samples(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
    sample_count: int = TRIP_ID_DEBUG_SAMPLE_COUNT,
    timeout_seconds: int = TRIP_ID_DEBUG_TIMEOUT_SECONDS,
) -> list[str]:
    """
    Separate short-lived streaming query: print raw parsed Kafka events and
    collect up to sample_count trip_id values as they arrive.
    """
    raw_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed_positions = (
        raw_kafka.select(from_json(col("value").cast("string"), VEHICLE_POSITION_SCHEMA).alias("event"))
        .select("event.*")
        .filter(col("vehicle_id").isNotNull())
    )
    trip_id_view = parsed_positions.select(
        col("vehicle_id"),
        col("trip_id"),
        col("route_id"),
        col("current_stop_sequence"),
    )

    collected: list[str] = []
    query_holder: dict[str, StreamingQuery | None] = {"query": None}

    def debug_trip_id_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        print(f"\n--- [trip_id debug] Kafka micro-batch {batch_id} ---")
        batch_df.show(truncate=False)

        if len(collected) >= sample_count:
            return

        remaining = sample_count - len(collected)
        new_rows = (
            batch_df.filter(col("trip_id").isNotNull())
            .select(col("trip_id").cast("string").alias("trip_id"))
            .limit(remaining)
            .collect()
        )
        collected.extend(row.trip_id for row in new_rows)

        query = query_holder["query"]
        if len(collected) >= sample_count and query is not None and query.isActive:
            logger.info("Collected %d Kafka trip_id samples; stopping debug stream", len(collected))

    debug_query = (
        trip_id_view.writeStream.outputMode("append")
        .foreachBatch(debug_trip_id_batch)
        .option("checkpointLocation", "/tmp/transit-pulse-trip-id-debug-checkpoint")
        .start()
    )
    query_holder["query"] = debug_query

    logger.info(
        "trip_id debug stream started (id=%s). Waiting for up to %d samples "
        "or %ds timeout. Ensure gtfs_realtime_producer is running.",
        debug_query.id,
        sample_count,
        timeout_seconds,
    )

    deadline = time.monotonic() + timeout_seconds
    try:
        while (
            debug_query.isActive
            and len(collected) < sample_count
            and time.monotonic() < deadline
        ):
            time.sleep(1)
    finally:
        if debug_query.isActive:
            debug_query.stop()
        logger.info("trip_id debug stream stopped (collected=%d)", len(collected))

    return collected


def print_trip_id_format_comparison(
    kafka_trip_ids: list[str],
    static_trip_ids: list[str],
) -> None:
    """Print Kafka vs static trip_id samples side by side for visual comparison."""
    print("\n=== trip_id format comparison (read-only diagnostic) ===")
    print(
        f"{'#':>3}  {'KAFKA (vehicle-positions)':<44}  "
        f"{'STATIC (trips.parquet)':<44}"
    )
    print("-" * 96)

    row_count = max(len(kafka_trip_ids), len(static_trip_ids), 1)
    for index in range(row_count):
        kafka_value = repr(kafka_trip_ids[index]) if index < len(kafka_trip_ids) else "(none)"
        static_value = repr(static_trip_ids[index]) if index < len(static_trip_ids) else "(none)"
        print(f"{index + 1:>3}  {kafka_value:<44}  {static_value:<44}")

    if not kafka_trip_ids:
        print(
            "\nNo Kafka trip_id samples collected. Start `make run-gtfs-realtime` "
            "and re-run with DELAY_CALCULATOR_DEBUG_TRIP_IDS=true."
        )


def run_trip_id_format_diagnostic(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    raw_bucket: str,
    ingestion_date: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
) -> None:
    """
    Temporary read-only diagnostic: compare trip_id string formats between the
    Kafka stream and GTFS static trips before changing join logic.
    """
    logger.info("Running trip_id format diagnostic (join logic unchanged)")

    static_trip_ids = sample_static_trip_ids(
        spark,
        raw_bucket,
        ingestion_date,
        sample_count=TRIP_ID_DEBUG_SAMPLE_COUNT,
    )
    logger.info("Loaded %d static trip_id samples from trips.parquet", len(static_trip_ids))

    kafka_trip_ids = collect_kafka_trip_id_samples(
        spark,
        kafka_bootstrap_servers,
        topic=topic,
        sample_count=TRIP_ID_DEBUG_SAMPLE_COUNT,
    )
    logger.info("Collected %d Kafka trip_id samples", len(kafka_trip_ids))

    print_trip_id_format_comparison(kafka_trip_ids, static_trip_ids)


def load_static_trip_id_membership_data(
    spark: SparkSession,
    raw_bucket: str,
    ingestion_date: str,
) -> tuple[set[str], int, int, str, str, list[str]]:
    """
    Load the full static trips.parquet trip_id column for membership checks.

    Returns (distinct_id_set, total_row_count, distinct_count, raw_dtype,
    string_dtype, sample_values).
    """
    trips_path = f"s3a://{raw_bucket}/gtfs-static/trips/{ingestion_date}/trips.parquet"
    trips_df = spark.read.parquet(trips_path)
    raw_dtype = trips_df.schema["trip_id"].dataType.simpleString()

    trips_as_string = trips_df.select(col("trip_id").cast("string").alias("trip_id"))
    string_dtype = trips_as_string.schema["trip_id"].dataType.simpleString()

    total_row_count = trips_df.count()
    distinct_rows = trips_as_string.distinct().collect()
    distinct_trip_ids = {row.trip_id for row in distinct_rows}
    sample_values = [row.trip_id for row in trips_as_string.limit(5).collect()]

    return (
        distinct_trip_ids,
        total_row_count,
        len(distinct_trip_ids),
        raw_dtype,
        string_dtype,
        sample_values,
    )


def batch_read_kafka_positions(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
    message_limit: int = STOP_FIELD_DIAGNOSTIC_BATCH_SIZE,
) -> DataFrame:
    """Batch-read parsed vehicle-position messages from Kafka (not streaming)."""
    raw_kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    return (
        raw_kafka.select(from_json(col("value").cast("string"), VEHICLE_POSITION_SCHEMA).alias("event"))
        .select("event.*")
        .filter(col("vehicle_id").isNotNull())
        .limit(message_limit)
    )


def batch_read_kafka_trip_ids(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
    message_limit: int = TRIP_ID_MEMBERSHIP_KAFKA_BATCH_SIZE,
) -> tuple[list[str], str]:
    """Batch-read Kafka messages (not streaming) and extract trip_id values."""
    parsed_messages = batch_read_kafka_positions(
        spark,
        kafka_bootstrap_servers,
        topic=topic,
        message_limit=message_limit,
    )
    kafka_dtype = parsed_messages.schema["trip_id"].dataType.simpleString()

    kafka_trip_ids = [
        row.trip_id
        for row in parsed_messages.select("trip_id")
        .filter(col("trip_id").isNotNull())
        .collect()
    ]
    return kafka_trip_ids, kafka_dtype


def print_stop_field_null_diagnostic(stats: dict[str, object], batch_limit: int) -> None:
    """Print null-rate summary for join-key fields in a Kafka batch."""
    lines = [
        "",
        "=== stop field null-rate diagnostic (read-only batch) ===",
        f"Kafka messages batch-read (limit={batch_limit}): {stats['total']}",
        (
            f"current_stop_sequence null: {stats['stop_sequence_null_count']}/{stats['total']} "
            f"({stats['stop_sequence_null_pct']:.1f}%)"
        ),
        (
            f"stop_id null:               {stats['stop_id_null_count']}/{stats['total']} "
            f"({stats['stop_id_null_pct']:.1f}%)"
        ),
    ]
    sample_stop_ids = stats["sample_stop_ids"]
    if sample_stop_ids:
        lines.append(f"Sample non-null stop_id values: {sample_stop_ids!r}")
    else:
        lines.append("Sample non-null stop_id values: (none in batch)")
    print("\n".join(lines), flush=True)


def resolve_join_strategy(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
    join_mode: JoinMode = "auto",
    batch_size: int = STOP_FIELD_DIAGNOSTIC_BATCH_SIZE,
    *,
    print_diagnostic: bool = True,
) -> str:
    """
    Batch-read Kafka messages, print stop-field null rates, and choose join mode.

    current_stop_sequence is retained in the stream schema for reference but is
    not used as a join key in either strategy.
    """
    positions = batch_read_kafka_positions(
        spark,
        kafka_bootstrap_servers,
        topic=topic,
        message_limit=batch_size,
    )
    stats = measure_stop_field_null_rates(positions)
    if print_diagnostic:
        print_stop_field_null_diagnostic(stats, batch_size)

    strategy = select_join_strategy(stats, join_mode=join_mode)
    logger.info(
        "Selected join strategy=%s (join_mode=%s, stop_id_null_pct=%.1f%%)",
        strategy,
        join_mode,
        stats["stop_id_null_pct"],
    )
    return strategy


def run_stop_field_null_diagnostic(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
    batch_size: int = STOP_FIELD_DIAGNOSTIC_BATCH_SIZE,
) -> dict[str, object]:
    """Read-only diagnostic for stop_id / current_stop_sequence null rates."""
    logger.info("Running stop field null-rate diagnostic (join logic unchanged)")
    positions = batch_read_kafka_positions(
        spark,
        kafka_bootstrap_servers,
        topic=topic,
        message_limit=batch_size,
    )
    stats = measure_stop_field_null_rates(positions)
    print_stop_field_null_diagnostic(stats, batch_size)
    return stats


def run_trip_id_membership_diagnostic(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    raw_bucket: str,
    ingestion_date: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
    kafka_batch_size: int = TRIP_ID_MEMBERSHIP_KAFKA_BATCH_SIZE,
) -> None:
    """
    One-time batch diagnostic: check whether Kafka trip_ids exist in the full
    static schedule set and confirm Spark dtypes align on both sides.
    """
    logger.info("Running trip_id membership diagnostic (join logic unchanged)")

    (
        static_trip_ids,
        static_row_count,
        static_distinct_count,
        static_raw_dtype,
        static_string_dtype,
        static_samples,
    ) = load_static_trip_id_membership_data(spark, raw_bucket, ingestion_date)

    kafka_trip_ids, kafka_dtype = batch_read_kafka_trip_ids(
        spark,
        kafka_bootstrap_servers,
        topic=topic,
        message_limit=kafka_batch_size,
    )

    unique_kafka_trip_ids = list(dict.fromkeys(kafka_trip_ids))
    matched = [trip_id for trip_id in unique_kafka_trip_ids if trip_id in static_trip_ids]
    unmatched = [trip_id for trip_id in unique_kafka_trip_ids if trip_id not in static_trip_ids]

    print("\n=== trip_id membership diagnostic (read-only batch) ===")
    print(f"Static trips.parquet total rows:        {static_row_count}")
    print(f"Static trips.parquet distinct trip_ids: {static_distinct_count}")
    print(f"Static trip_id sample values:           {static_samples!r}")
    print(f"Static trip_id Spark dtype (raw):       {static_raw_dtype}")
    print(f"Static trip_id Spark dtype (string):    {static_string_dtype}")
    print(f"Kafka trip_id Spark dtype (parsed):     {kafka_dtype}")
    print(
        f"Join dtypes match (Kafka vs static string): "
        f"{kafka_dtype == static_string_dtype}"
    )
    print(
        f"Kafka messages batch-read (limit={kafka_batch_size}): "
        f"{len(kafka_trip_ids)} with non-null trip_id"
    )
    print(
        f"{len(matched)} out of {len(unique_kafka_trip_ids)} unique Kafka trip_ids "
        f"found in static schedule"
    )

    if unmatched:
        examples = unmatched[:TRIP_ID_MEMBERSHIP_UNMATCHED_EXAMPLES]
        print(f"Example unmatched Kafka trip_ids ({len(examples)}):")
        for trip_id in examples:
            print(f"  - {trip_id!r}")
    else:
        print("All unique Kafka trip_ids in the batch matched the static schedule.")

    if not kafka_trip_ids:
        print(
            "No Kafka trip_id values in batch. Ensure `make run-gtfs-realtime` is "
            "running and messages exist in vehicle-positions."
        )


def build_idempotent_curated_writer(curated_bucket: str) -> Callable[[DataFrame, int], None]:
    """
    foreachBatch sink that overwrites only the partitions present in each batch.

    Dynamic partition overwrite makes reruns/backfills safe: reprocessing the
    same route_id + window_date replaces that slice instead of appending dupes.
    """

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        write_curated_route_delay_windows(batch_df, curated_bucket)

    return write_batch


def event_date_from_timestamp(timestamp_col=None):
    """Derive YYYY-MM-DD from the message timestamp in the service timezone."""
    if timestamp_col is None:
        timestamp_col = col("timestamp")
    return date_format(
        from_utc_timestamp(timestamp_col.cast("timestamp"), ARCHIVE_DATE_TIMEZONE),
        "yyyy-MM-dd",
    )


def build_raw_archive_writer(raw_bucket: str) -> Callable[[DataFrame, int], None]:
    """
    foreachBatch sink that appends raw vehicle positions under
    s3a://{raw_bucket}/vehicle-positions/{YYYY-MM-DD}/.

    Date comes from the event timestamp (Pacific service day), not ingestion
    wall-clock time, so historical_backfill date-range reads stay aligned.
    """

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        with_date = (
            batch_df.select(*RAW_ARCHIVE_COLUMNS)
            .filter(col("timestamp").isNotNull())
            .withColumn("date", event_date_from_timestamp())
            .filter(col("date").isNotNull())
        )
        if with_date.rdd.isEmpty():
            return

        dates = [row.date for row in with_date.select("date").distinct().collect()]
        for day in dates:
            day_df = with_date.filter(col("date") == day).select(*RAW_ARCHIVE_COLUMNS)
            output_path = f"s3a://{raw_bucket}/{VEHICLE_POSITIONS_PREFIX}/{day}/"
            day_df.write.mode("append").parquet(output_path)

    return write_batch


def resolve_checkpoint_dir(debug_console: bool) -> str:
    """Checkpoint path for the main windowed output stream."""
    if debug_console:
        return DEBUG_CHECKPOINT_DIR
    return os.environ.get("DELAY_CALCULATOR_CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR)


def resolve_archiver_checkpoint_dir() -> str:
    """Checkpoint path for the raw vehicle-position archive stream."""
    return os.environ.get(
        "DELAY_CALCULATOR_ARCHIVER_CHECKPOINT_DIR",
        DEFAULT_ARCHIVER_CHECKPOINT_DIR,
    )


def maybe_reset_checkpoint(checkpoint_dir: str, reset: bool) -> None:
    """
    Wipe a Structured Streaming checkpoint directory before starting.

    Use after Ctrl+C, overlapping local runs, or SparkConcurrentModificationException
    on the offsets log.
    """
    if not reset:
        return
    if os.path.isdir(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
        logger.info("Removed checkpoint directory: %s", checkpoint_dir)
    else:
        logger.info("Checkpoint reset requested; nothing to remove at %s", checkpoint_dir)


def start_raw_archive_stream(
    positions: DataFrame,
    raw_bucket: str,
    *,
    checkpoint_dir: str | None = None,
) -> StreamingQuery:
    """
    Archive parsed vehicle-position events to MinIO raw-zone Parquet.

    Runs as a second query on the same Kafka source DataFrame as the delay
    calculator, with an independent checkpoint so the sinks do not conflict.
    """
    checkpoint = checkpoint_dir or resolve_archiver_checkpoint_dir()
    logger.info(
        "Archiving raw vehicle positions to s3a://%s/%s/{date}/ "
        "(checkpoint=%s)",
        raw_bucket,
        VEHICLE_POSITIONS_PREFIX,
        checkpoint,
    )
    return (
        positions.writeStream.outputMode("append")
        .foreachBatch(build_raw_archive_writer(raw_bucket))
        .option("checkpointLocation", checkpoint)
        .queryName("vehicle-positions-raw-archive")
        .start()
    )


def start_windowed_output_stream(
    windowed: DataFrame,
    curated_bucket: str,
    *,
    debug_console: bool = False,
    checkpoint_dir: str | None = None,
) -> StreamingQuery:
    """
    Start the windowed delay output stream.

    Set debug_console=True to print each closed window batch to the terminal
    instead of writing Parquet to MinIO — useful for local smoke tests.
    """
    checkpoint = checkpoint_dir or resolve_checkpoint_dir(debug_console)
    writer = windowed.writeStream.outputMode("append").queryName(
        "route-delay-windows-curated"
    )

    if debug_console:
        logger.info(
            "DEBUG sink enabled: windowed batches will print to console "
            "(checkpoint=%s)",
            checkpoint,
        )
        return (
            writer.format("console")
            .option("truncate", "false")
            .option("numRows", 50)
            .option("checkpointLocation", checkpoint)
            .start()
        )

    logger.info(
        "Writing windowed batches to s3a://%s/route-delay-windows/ "
        "(checkpoint=%s)",
        curated_bucket,
        checkpoint,
    )
    return (
        writer.foreachBatch(build_idempotent_curated_writer(curated_bucket))
        .option("checkpointLocation", checkpoint)
        .start()
    )


def run_delay_calculator_job(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    raw_bucket: str,
    curated_bucket: str,
    ingestion_date: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
    debug_console: bool = False,
    debug_trip_ids: bool = False,
    debug_trip_id_membership: bool = False,
    debug_stop_fields: bool = False,
    join_mode: JoinMode = "auto",
    reset_checkpoint: bool = False,
) -> None:
    """Wire the streaming pipeline and block until the queries are stopped."""
    if debug_trip_ids:
        run_trip_id_format_diagnostic(
            spark,
            kafka_bootstrap_servers,
            raw_bucket,
            ingestion_date,
            topic=topic,
        )

    if debug_trip_id_membership:
        run_trip_id_membership_diagnostic(
            spark,
            kafka_bootstrap_servers,
            raw_bucket,
            ingestion_date,
            topic=topic,
        )

    join_strategy = resolve_join_strategy(
        spark,
        kafka_bootstrap_servers,
        topic=topic,
        join_mode=join_mode,
    )

    if debug_stop_fields:
        logger.info(
            "Stop field diagnostic complete; exiting "
            "(DELAY_CALCULATOR_DEBUG_STOP_FIELDS=true)"
        )
        return

    positions = read_vehicle_positions_stream(spark, kafka_bootstrap_servers, topic=topic)
    schedule_df = load_schedule_dataframe(spark, raw_bucket, ingestion_date)

    trip_schedule_bc = None
    schedule_for_join = schedule_df
    if join_strategy == JOIN_MODE_TRIP_ID_STOP_ID:
        schedule_for_join = broadcast(schedule_df)
    elif join_strategy == JOIN_MODE_TRIP_ID_NEAREST:
        trip_schedule_bc = build_trip_schedule_broadcast(spark, schedule_df)

    enriched = join_stream_to_schedule(
        positions,
        schedule_for_join,
        join_strategy,
        trip_schedule_bc=trip_schedule_bc,
    )
    delayed = compute_delay_seconds(enriched)
    windowed = aggregate_windowed_delays(delayed, with_watermark=True)

    curated_checkpoint_dir = resolve_checkpoint_dir(debug_console)
    archiver_checkpoint_dir = resolve_archiver_checkpoint_dir()
    maybe_reset_checkpoint(curated_checkpoint_dir, reset_checkpoint)
    maybe_reset_checkpoint(archiver_checkpoint_dir, reset_checkpoint)

    # Second sink on the pre-join positions stream: raw archive for backfill.
    archive_query = start_raw_archive_stream(
        positions,
        raw_bucket,
        checkpoint_dir=archiver_checkpoint_dir,
    )
    curated_query = start_windowed_output_stream(
        windowed,
        curated_bucket,
        debug_console=debug_console,
        checkpoint_dir=curated_checkpoint_dir,
    )

    logger.info(
        "Streaming queries started "
        "(archive id=%s status=%s; curated id=%s status=%s). "
        "Raw events land under s3a://%s/%s/{date}/; curated windows write "
        "after the watermark elapses. Press Ctrl+C to stop.",
        archive_query.id,
        archive_query.status,
        curated_query.id,
        curated_query.status,
        raw_bucket,
        VEHICLE_POSITIONS_PREFIX,
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in (archive_query, curated_query):
            if query.isActive:
                query.stop()
            logger.info(
                "Streaming query stopped (name=%s id=%s status=%s)",
                query.name,
                query.id,
                query.status,
            )


def main() -> None:
    """Run the delay calculator streaming job in local mode."""
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
    ingestion_date = os.environ.get("GTFS_STATIC_INGESTION_DATE")
    debug_console = os.environ.get("DELAY_CALCULATOR_DEBUG_CONSOLE", "").lower() in {
        "1",
        "true",
        "yes",
    }
    debug_trip_ids = os.environ.get("DELAY_CALCULATOR_DEBUG_TRIP_IDS", "").lower() in {
        "1",
        "true",
        "yes",
    }
    debug_trip_id_membership = os.environ.get(
        "DELAY_CALCULATOR_DEBUG_TRIP_ID_MEMBERSHIP", ""
    ).lower() in {
        "1",
        "true",
        "yes",
    }
    debug_stop_fields = os.environ.get("DELAY_CALCULATOR_DEBUG_STOP_FIELDS", "").lower() in {
        "1",
        "true",
        "yes",
    }
    join_mode_raw = os.environ.get("DELAY_CALCULATOR_JOIN_MODE", "auto").lower()
    if join_mode_raw not in {"auto", JOIN_MODE_TRIP_ID_STOP_ID, JOIN_MODE_TRIP_ID_NEAREST}:
        raise ValueError(
            "DELAY_CALCULATOR_JOIN_MODE must be one of: "
            "auto, trip_id_stop_id, trip_id_nearest"
        )
    join_mode: JoinMode = join_mode_raw  # type: ignore[assignment]
    reset_checkpoint = os.environ.get("DELAY_CALCULATOR_RESET_CHECKPOINT", "").lower() in {
        "1",
        "true",
        "yes",
    }

    if not kafka_bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS is not set")
    if not all([minio_endpoint, minio_access_key, minio_secret_key]):
        raise ValueError("MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set")
    if not ingestion_date:
        raise ValueError(
            "GTFS_STATIC_INGESTION_DATE is not set (YYYY-MM-DD folder under raw-zone/gtfs-static/)"
        )

    spark = build_spark_session(minio_endpoint, minio_access_key, minio_secret_key)
    try:
        run_delay_calculator_job(
            spark=spark,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            raw_bucket=os.environ.get("RAW_ZONE_BUCKET", RAW_BUCKET),
            curated_bucket=os.environ.get("CURATED_ZONE_BUCKET", CURATED_BUCKET),
            ingestion_date=ingestion_date,
            debug_console=debug_console,
            debug_trip_ids=debug_trip_ids,
            debug_trip_id_membership=debug_trip_id_membership,
            debug_stop_fields=debug_stop_fields,
            join_mode=join_mode,
            reset_checkpoint=reset_checkpoint,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
