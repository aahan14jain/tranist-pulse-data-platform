"""Tests for ingestion.gtfs_realtime_producer."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests
from google.transit import gtfs_realtime_pb2 as gtfs
from kafka.errors import KafkaError

from ingestion.gtfs_realtime_producer import (
    build_feed_request_url,
    current_status_to_string,
    fetch_gtfs_realtime_feed,
    flatten_vehicle_entity,
    parse_feed_bytes,
    produce_vehicle_position,
    produce_vehicle_positions,
)
from tests.fixtures.gtfs_realtime import (
    build_feed_bytes,
    build_full_vehicle_entity,
    build_minimal_vehicle_entity,
)


def test_build_feed_request_url_appends_api_key() -> None:
    url = build_feed_request_url(
        "https://example.com/feed.pb",
        api_key="TEST",
    )
    assert url == "https://example.com/feed.pb?key=TEST"


def test_build_feed_request_url_preserves_existing_key() -> None:
    url = build_feed_request_url(
        "https://example.com/feed.pb?key=EXISTING",
        api_key="TEST",
    )
    assert url == "https://example.com/feed.pb?key=EXISTING"


def test_fetch_gtfs_realtime_feed_retries_on_429_then_succeeds() -> None:
    session = MagicMock()
    feed_bytes = build_feed_bytes([build_minimal_vehicle_entity()])

    rate_limited = MagicMock()
    rate_limited.status_code = 429

    success = MagicMock()
    success.status_code = 200
    success.content = feed_bytes
    success.raise_for_status.return_value = None

    session.get.side_effect = [rate_limited, success]

    with patch("ingestion.gtfs_realtime_producer.time.sleep"):
        result = fetch_gtfs_realtime_feed(
            "https://example.com/feed.pb",
            session=session,
            max_rate_limit_retries=3,
            rate_limit_backoff_seconds=0.01,
        )

    assert result == feed_bytes
    assert session.get.call_count == 2


def test_fetch_gtfs_realtime_feed_retries_on_401_then_succeeds() -> None:
    session = MagicMock()
    feed_bytes = build_feed_bytes([build_minimal_vehicle_entity()])

    unauthorized = MagicMock()
    unauthorized.status_code = 401

    success = MagicMock()
    success.status_code = 200
    success.content = feed_bytes
    success.raise_for_status.return_value = None

    session.get.side_effect = [unauthorized, success]

    with patch("ingestion.gtfs_realtime_producer.time.sleep"):
        result = fetch_gtfs_realtime_feed(
            "https://example.com/feed.pb",
            session=session,
            max_rate_limit_retries=3,
            rate_limit_backoff_seconds=0.01,
        )

    assert result == feed_bytes
    assert session.get.call_count == 2


def test_fetch_gtfs_realtime_feed_does_not_retry_other_http_errors() -> None:
    session = MagicMock()
    server_error = MagicMock()
    server_error.status_code = 500
    server_error.raise_for_status.side_effect = requests.HTTPError(response=server_error)
    session.get.return_value = server_error

    with pytest.raises(requests.HTTPError):
        fetch_gtfs_realtime_feed("https://example.com/feed.pb", session=session)

    session.get.assert_called_once()


def test_fetch_gtfs_realtime_feed_raises_after_rate_limit_retries_exhausted() -> None:
    session = MagicMock()
    rate_limited = MagicMock()
    rate_limited.status_code = 429
    rate_limited.raise_for_status.side_effect = requests.HTTPError(response=rate_limited)
    session.get.return_value = rate_limited

    with patch("ingestion.gtfs_realtime_producer.time.sleep"):
        with pytest.raises(requests.HTTPError):
            fetch_gtfs_realtime_feed(
                "https://example.com/feed.pb",
                session=session,
                max_rate_limit_retries=2,
                rate_limit_backoff_seconds=0.01,
            )

    assert session.get.call_count == 2


def test_current_status_to_string() -> None:
    assert current_status_to_string(gtfs.VehiclePosition.IN_TRANSIT_TO) == "IN_TRANSIT_TO"
    assert current_status_to_string(gtfs.VehiclePosition.STOPPED_AT) == "STOPPED_AT"


def test_flatten_vehicle_entity_full_fields() -> None:
    entity = build_full_vehicle_entity()
    record = flatten_vehicle_entity(entity)

    assert record is not None
    assert record["vehicle_id"] == "BUS-42"
    assert record["trip_id"] == "T001"
    assert record["route_id"] == "R01"
    assert record["latitude"] == pytest.approx(47.6062)
    assert record["longitude"] == pytest.approx(-122.3321)
    assert record["timestamp"] == 1719900000
    assert record["current_stop_sequence"] == 5
    assert record["stop_id"] == "001"
    assert record["current_status"] == "IN_TRANSIT_TO"


def test_flatten_vehicle_entity_minimal_fields() -> None:
    entity = build_minimal_vehicle_entity()
    record = flatten_vehicle_entity(entity)

    assert record == {
        "vehicle_id": "BUS-99",
        "latitude": pytest.approx(47.62),
        "longitude": pytest.approx(-122.32),
    }


def test_flatten_vehicle_entity_uses_entity_id_when_vehicle_id_missing() -> None:
    entity = build_minimal_vehicle_entity(vehicle_id=None, entity_id="entity-fallback")
    record = flatten_vehicle_entity(entity)

    assert record is not None
    assert record["vehicle_id"] == "entity-fallback"


def test_flatten_vehicle_entity_skips_deleted() -> None:
    entity = build_full_vehicle_entity()
    entity.is_deleted = True
    assert flatten_vehicle_entity(entity) is None


def test_flatten_vehicle_entity_skips_without_vehicle_payload() -> None:
    entity = gtfs.FeedEntity()
    entity.id = "alert-only"
    assert flatten_vehicle_entity(entity) is None


def test_parse_feed_bytes_multiple_entities() -> None:
    feed_bytes = build_feed_bytes(
        [
            build_full_vehicle_entity(entity_id="e1"),
            build_minimal_vehicle_entity(entity_id="e2", vehicle_id="BUS-99"),
        ]
    )

    records = parse_feed_bytes(feed_bytes)

    assert len(records) == 2
    assert records[0]["vehicle_id"] == "BUS-42"
    assert records[1]["vehicle_id"] == "BUS-99"


def test_produce_vehicle_position_retries_then_succeeds() -> None:
    producer = MagicMock()
    failing_future = MagicMock()
    failing_future.get.side_effect = KafkaError("broker unavailable")
    success_future = MagicMock()

    producer.send.side_effect = [failing_future, success_future]

    with patch("ingestion.gtfs_realtime_producer.time.sleep"):
        produce_vehicle_position(
            producer,
            "vehicle-positions",
            {"vehicle_id": "BUS-42", "latitude": 47.6},
            max_retries=2,
            base_backoff_seconds=0.01,
        )

    assert producer.send.call_count == 2
    producer.send.assert_called_with(
        "vehicle-positions",
        key="BUS-42",
        value={"vehicle_id": "BUS-42", "latitude": 47.6},
    )


def test_produce_vehicle_position_raises_after_exhausting_retries() -> None:
    producer = MagicMock()
    failing_future = MagicMock()
    failing_future.get.side_effect = KafkaError("broker unavailable")
    producer.send.return_value = failing_future

    with patch("ingestion.gtfs_realtime_producer.time.sleep"):
        with pytest.raises(KafkaError, match="Failed to produce vehicle_id=BUS-42"):
            produce_vehicle_position(
                producer,
                "vehicle-positions",
                {"vehicle_id": "BUS-42"},
                max_retries=2,
                base_backoff_seconds=0.01,
            )


def test_produce_vehicle_positions_skips_records_without_vehicle_id() -> None:
    producer = MagicMock()
    success_future = MagicMock()
    producer.send.return_value = success_future

    sent = produce_vehicle_positions(
        producer,
        [
            {"vehicle_id": "BUS-1", "latitude": 47.6},
            {"latitude": 47.7},
        ],
    )

    assert sent == 1
    producer.send.assert_called_once()
