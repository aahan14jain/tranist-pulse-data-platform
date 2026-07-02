"""
Spark Structured Streaming job: real-time delay vs GTFS schedule.

Reads vehicle-position JSON events from Kafka, joins each update against
broadcast GTFS static stop_times/trips (loaded from MinIO raw-zone by
gtfs_static_loader.py), and computes how late each vehicle is at its
current stop. Results are aggregated into 5-minute tumbling windows and
written idempotently to MinIO curated-zone Parquet.

Test coverage for Structured Streaming is intentionally deferred: verifying
this job end-to-end needs a memory-sink / checkpoint test harness, which is
flagged as a follow-up rather than silently skipping coverage.

Personal portfolio project; not affiliated with any transit agency.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Callable
from zoneinfo import ZoneInfo

import pyspark
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    coalesce,
    col,
    count,
    date_format,
    from_json,
    from_unixtime,
    lit,
    udf,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

VEHICLE_POSITIONS_TOPIC = "vehicle-positions"
RAW_BUCKET = "raw-zone"
CURATED_BUCKET = "curated-zone"
WATERMARK_DELAY = "5 minutes"
WINDOW_DURATION = "5 minutes"
LOCAL_SHUFFLE_PARTITIONS = 8
SERVICE_TIMEZONE = ZoneInfo("America/Los_Angeles")
# Kafka connector Scala binary (_2.13) must match Spark 4.x; version must match PySpark.
SPARK_VERSION = pyspark.__version__

# Explicit stream schema — inference on a live stream is slow and brittle
# when optional JSON fields appear/disappear across events.
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


@udf(LongType())
def gtfs_arrival_to_epoch(arrival_time: str | None, actual_timestamp: int | None) -> int | None:
    """
    Convert a GTFS stop_times arrival_time (HH:MM:SS, may exceed 24:00) to
    Unix epoch seconds using the service day implied by actual_timestamp.
    """
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
            "org.apache.hadoop:hadoop-aws:3.3.4,"
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
    raw_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
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
    """
    Load GTFS stop_times joined to trips for stream-static lookup keys.

    The joined table is small enough to broadcast against the Kafka stream.
    """
    stop_times_path = f"s3a://{raw_bucket}/gtfs-static/stop_times/{ingestion_date}/stop_times.parquet"
    trips_path = f"s3a://{raw_bucket}/gtfs-static/trips/{ingestion_date}/trips.parquet"

    stop_times = spark.read.parquet(stop_times_path)
    trips = spark.read.parquet(trips_path)

    schedule = stop_times.join(
        trips.select(
            col("trip_id").cast("string").alias("trip_id"),
            col("route_id").cast("string").alias("schedule_route_id"),
        ),
        on="trip_id",
        how="inner",
    ).select(
        col("trip_id").cast("string").alias("trip_id"),
        col("schedule_route_id"),
        col("stop_sequence").cast("integer").alias("stop_sequence"),
        col("arrival_time").cast("string").alias("arrival_time"),
    )

    return broadcast(schedule)


def join_stream_to_schedule(positions: DataFrame, schedule: DataFrame) -> DataFrame:
    """Stream-static join on trip_id + current stop sequence."""
    return positions.join(
        schedule,
        (positions.trip_id == schedule.trip_id)
        & (positions.current_stop_sequence == schedule.stop_sequence),
        how="left",
    ).drop(schedule.trip_id)


def compute_delay_seconds(enriched: DataFrame) -> DataFrame:
    """Attach scheduled_arrival_epoch and delay_seconds for each vehicle event."""
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


def aggregate_windowed_delays(delayed_events: DataFrame) -> DataFrame:
    """
    Apply event-time watermarking and 5-minute tumbling windows.

    Watermark (5 minutes):
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
    ).withWatermark("event_time", WATERMARK_DELAY)

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


def build_idempotent_curated_writer(curated_bucket: str) -> Callable[[DataFrame, int], None]:
    """
    foreachBatch sink that overwrites only the partitions present in each batch.

    Dynamic partition overwrite makes reruns/backfills safe: reprocessing the
    same route_id + window_date replaces that slice instead of appending dupes.
    """

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        output_path = f"s3a://{curated_bucket}/route-delay-windows"
        spark = batch_df.sparkSession
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        (
            batch_df.select(
                "route_id",
                "window_date",
                "window_start",
                "window_end",
                "avg_delay_seconds",
                "vehicle_event_count",
            )
            .write.mode("overwrite")
            .partitionBy("route_id", "window_date")
            .parquet(output_path)
        )

    return write_batch


def run_delay_calculator_job(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    raw_bucket: str,
    curated_bucket: str,
    ingestion_date: str,
    topic: str = VEHICLE_POSITIONS_TOPIC,
) -> None:
    """Wire the streaming pipeline and block until the query is stopped."""
    positions = read_vehicle_positions_stream(spark, kafka_bootstrap_servers, topic=topic)
    schedule = load_schedule_lookup(spark, raw_bucket, ingestion_date)

    enriched = join_stream_to_schedule(positions, schedule)
    delayed = compute_delay_seconds(enriched)
    windowed = aggregate_windowed_delays(delayed)

    query = (
        windowed.writeStream.outputMode("append")
        .foreachBatch(build_idempotent_curated_writer(curated_bucket))
        .option("checkpointLocation", "/tmp/transit-pulse-delay-calculator-checkpoint")
        .start()
    )

    query.awaitTermination()


def main() -> None:
    """Run the delay calculator streaming job in local mode."""
    load_dotenv()

    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
    ingestion_date = os.environ.get("GTFS_STATIC_INGESTION_DATE")

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
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
