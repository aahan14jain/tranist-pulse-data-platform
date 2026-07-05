"""Unit tests for shared join/delay computation (synthetic DataFrames only)."""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common.delay_logic import (
    JOIN_MODE_TRIP_ID_NEAREST,
    JOIN_MODE_TRIP_ID_STOP_ID,
    SERVICE_TIMEZONE,
    arrival_time_to_epoch,
    build_trip_schedule_broadcast,
    compute_delay_seconds,
    join_positions_to_schedule,
    join_positions_to_schedule_by_stop_id,
    join_positions_to_schedule_nearest_arrival,
    join_positions_to_schedule_nearest_arrival_sql,
    select_join_strategy,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-delay-logic")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _pacific_epoch(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(
        datetime(year, month, day, hour, minute, tzinfo=SERVICE_TIMEZONE).timestamp()
    )


POSITIONS_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), False),
        StructField("trip_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("stop_id", StringType(), True),
    ]
)

SCHEDULE_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), False),
        StructField("schedule_route_id", StringType(), True),
        StructField("schedule_stop_id", StringType(), True),
        StructField("stop_sequence", IntegerType(), True),
        StructField("arrival_time", StringType(), True),
    ]
)


def test_arrival_time_to_epoch_same_day() -> None:
    actual = _pacific_epoch(2026, 7, 1, 8, 5)
    scheduled = arrival_time_to_epoch("08:00:00", actual)
    assert scheduled == _pacific_epoch(2026, 7, 1, 8, 0)


def test_arrival_time_to_epoch_past_midnight() -> None:
    # GTFS allows hours >= 24 for trips that spill past midnight.
    # Service day is the event's local calendar date; 25:00:00 is 01:00 next day.
    actual = _pacific_epoch(2026, 7, 1, 23, 50)
    scheduled = arrival_time_to_epoch("25:00:00", actual)
    assert scheduled == _pacific_epoch(2026, 7, 2, 1, 0)


def test_arrival_time_to_epoch_missing_inputs() -> None:
    assert arrival_time_to_epoch(None, 1_000_000) is None
    assert arrival_time_to_epoch("08:00:00", None) is None
    assert arrival_time_to_epoch("bad", 1_000_000) is None


def test_join_positions_to_schedule_by_stop_id(spark: SparkSession) -> None:
    positions = spark.createDataFrame(
        [
            ("v1", "T1", "R1", _pacific_epoch(2026, 7, 1, 8, 2), "S1"),
            ("v2", "T1", "R1", _pacific_epoch(2026, 7, 1, 8, 12), "S2"),
            ("v3", "T9", "R9", _pacific_epoch(2026, 7, 1, 9, 0), "S9"),
        ],
        schema=POSITIONS_SCHEMA,
    )
    schedule = spark.createDataFrame(
        [
            ("T1", "R1", "S1", 1, "08:00:00"),
            ("T1", "R1", "S2", 2, "08:10:00"),
        ],
        schema=SCHEDULE_SCHEMA,
    )

    joined = join_positions_to_schedule_by_stop_id(positions, schedule)
    rows = {row.vehicle_id: row for row in joined.collect()}

    assert rows["v1"].arrival_time == "08:00:00"
    assert rows["v2"].arrival_time == "08:10:00"
    assert rows["v3"].arrival_time is None


def test_join_positions_to_schedule_nearest_arrival(spark: SparkSession) -> None:
    positions = spark.createDataFrame(
        [
            # Closer to 08:00 than 08:10.
            ("v1", "T1", "R1", _pacific_epoch(2026, 7, 1, 8, 1), None),
            # Closer to 08:10 than 08:00.
            ("v2", "T1", "R1", _pacific_epoch(2026, 7, 1, 8, 9), None),
        ],
        schema=POSITIONS_SCHEMA,
    )
    schedule = spark.createDataFrame(
        [
            ("T1", "R1", "S1", 1, "08:00:00"),
            ("T1", "R1", "S2", 2, "08:10:00"),
        ],
        schema=SCHEDULE_SCHEMA,
    )
    trip_schedule_bc = build_trip_schedule_broadcast(spark, schedule)

    joined = join_positions_to_schedule_nearest_arrival(positions, trip_schedule_bc)
    rows = {row.vehicle_id: row for row in joined.collect()}

    assert rows["v1"].schedule_stop_id == "S1"
    assert rows["v1"].scheduled_arrival_epoch == _pacific_epoch(2026, 7, 1, 8, 0)
    assert rows["v2"].schedule_stop_id == "S2"
    assert rows["v2"].scheduled_arrival_epoch == _pacific_epoch(2026, 7, 1, 8, 10)


def test_join_positions_to_schedule_nearest_arrival_sql(spark: SparkSession) -> None:
    """Batch SQL path must match broadcast-UDF nearest-stop semantics."""
    positions = spark.createDataFrame(
        [
            ("v1", "T1", "R1", _pacific_epoch(2026, 7, 1, 8, 1), None),
            ("v2", "T1", "R1", _pacific_epoch(2026, 7, 1, 8, 9), None),
        ],
        schema=POSITIONS_SCHEMA,
    )
    schedule = spark.createDataFrame(
        [
            ("T1", "R1", "S1", 1, "08:00:00"),
            ("T1", "R1", "S2", 2, "08:10:00"),
        ],
        schema=SCHEDULE_SCHEMA,
    )

    joined = join_positions_to_schedule_nearest_arrival_sql(positions, schedule)
    rows = {row.vehicle_id: row for row in joined.collect()}

    assert rows["v1"].schedule_stop_id == "S1"
    assert rows["v1"].scheduled_arrival_epoch == _pacific_epoch(2026, 7, 1, 8, 0)
    assert rows["v2"].schedule_stop_id == "S2"
    assert rows["v2"].scheduled_arrival_epoch == _pacific_epoch(2026, 7, 1, 8, 10)


def test_compute_delay_seconds_from_arrival_time(spark: SparkSession) -> None:
    actual_ts = _pacific_epoch(2026, 7, 1, 8, 5)
    enriched = spark.createDataFrame(
        [
            ("v1", "T1", "R1", actual_ts, "S1", "08:00:00", "R1"),
        ],
        schema=StructType(
            [
                StructField("vehicle_id", StringType(), False),
                StructField("trip_id", StringType(), True),
                StructField("route_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("stop_id", StringType(), True),
                StructField("arrival_time", StringType(), True),
                StructField("schedule_route_id", StringType(), True),
            ]
        ),
    )

    delayed = compute_delay_seconds(enriched).collect()
    assert len(delayed) == 1
    assert delayed[0].delay_seconds == pytest.approx(300.0)
    assert delayed[0].route_id == "R1"


def test_compute_delay_seconds_uses_schedule_route_when_missing(spark: SparkSession) -> None:
    actual_ts = _pacific_epoch(2026, 7, 1, 8, 0)
    enriched = spark.createDataFrame(
        [
            ("v1", "T1", None, actual_ts, "S1", "08:00:00", "R_SCHEDULE"),
        ],
        schema=StructType(
            [
                StructField("vehicle_id", StringType(), False),
                StructField("trip_id", StringType(), True),
                StructField("route_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("stop_id", StringType(), True),
                StructField("arrival_time", StringType(), True),
                StructField("schedule_route_id", StringType(), True),
            ]
        ),
    )

    delayed = compute_delay_seconds(enriched).collect()
    assert len(delayed) == 1
    assert delayed[0].route_id == "R_SCHEDULE"
    assert delayed[0].delay_seconds == pytest.approx(0.0)


def test_join_positions_to_schedule_dispatch_stop_id(spark: SparkSession) -> None:
    positions = spark.createDataFrame(
        [("v1", "T1", "R1", _pacific_epoch(2026, 7, 1, 8, 3), "S1")],
        schema=POSITIONS_SCHEMA,
    )
    schedule = spark.createDataFrame(
        [("T1", "R1", "S1", 1, "08:00:00")],
        schema=SCHEDULE_SCHEMA,
    )

    joined = join_positions_to_schedule(
        positions,
        schedule,
        JOIN_MODE_TRIP_ID_STOP_ID,
    )
    delayed = compute_delay_seconds(joined).collect()

    assert len(delayed) == 1
    assert delayed[0].delay_seconds == pytest.approx(180.0)


def test_join_positions_to_schedule_dispatch_nearest(spark: SparkSession) -> None:
    positions = spark.createDataFrame(
        [("v1", "T1", "R1", _pacific_epoch(2026, 7, 1, 8, 4), None)],
        schema=POSITIONS_SCHEMA,
    )
    schedule = spark.createDataFrame(
        [
            ("T1", "R1", "S1", 1, "08:00:00"),
            ("T1", "R1", "S2", 2, "08:10:00"),
        ],
        schema=SCHEDULE_SCHEMA,
    )
    trip_schedule_bc = build_trip_schedule_broadcast(spark, schedule)

    joined = join_positions_to_schedule(
        positions,
        schedule,
        JOIN_MODE_TRIP_ID_NEAREST,
        trip_schedule_bc=trip_schedule_bc,
    )
    delayed = compute_delay_seconds(joined).collect()

    assert len(delayed) == 1
    assert delayed[0].delay_seconds == pytest.approx(240.0)


def test_select_join_strategy_auto_prefers_stop_id_when_populated() -> None:
    assert (
        select_join_strategy({"stop_id_null_pct": 10.0}, join_mode="auto")
        == JOIN_MODE_TRIP_ID_STOP_ID
    )
    assert (
        select_join_strategy({"stop_id_null_pct": 80.0}, join_mode="auto")
        == JOIN_MODE_TRIP_ID_NEAREST
    )


def test_select_join_strategy_respects_explicit_mode() -> None:
    assert (
        select_join_strategy({"stop_id_null_pct": 99.0}, join_mode=JOIN_MODE_TRIP_ID_STOP_ID)
        == JOIN_MODE_TRIP_ID_STOP_ID
    )
    assert (
        select_join_strategy({"stop_id_null_pct": 0.0}, join_mode=JOIN_MODE_TRIP_ID_NEAREST)
        == JOIN_MODE_TRIP_ID_NEAREST
    )
