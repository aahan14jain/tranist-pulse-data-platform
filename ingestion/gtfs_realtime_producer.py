"""
Poll GTFS-realtime vehicle positions and stream them to Kafka.

This is the real-time ingestion leg of Transit Pulse: it fetches protobuf
vehicle-position updates from a public agency feed, normalizes each update
into a flat JSON record, and publishes to the ``vehicle-positions`` Kafka
topic. A downstream Spark Structured Streaming job consumes that topic to
join live positions against the GTFS static schedule and compute
reliability metrics.

The default Puget Sound realtime URL points at OneBusAway. Local development
uses OneBusAway's public ``TEST`` API key because dedicated keys require a
~20 business-day approval window; a production deployment would swap in a
dedicated key via ``GTFS_REALTIME_API_KEY``.

Personal portfolio project using publicly available feeds (e.g. King County
Metro / OneBusAway); not affiliated with any transit agency.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

VEHICLE_POSITIONS_TOPIC = "vehicle-positions"
DEFAULT_POLL_INTERVAL_SECONDS = 30
DEFAULT_KAFKA_MAX_RETRIES = 5
DEFAULT_KAFKA_BACKOFF_SECONDS = 1.0
DEFAULT_FEED_FETCH_MAX_RETRIES = 4
DEFAULT_FEED_FETCH_BACKOFF_SECONDS = 5.0
RATE_LIMIT_STATUS_CODES = {401, 429}

_shutdown_requested = False


def _handle_shutdown_signal(signum: int, _frame: Any) -> None:
    global _shutdown_requested
    logger.info("Received signal %s; finishing current work and shutting down", signum)
    _shutdown_requested = True


def current_status_to_string(status: int) -> str:
    """Translate a VehiclePosition current_status enum to a readable string."""
    return gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(status)


def flatten_vehicle_entity(entity: gtfs_realtime_pb2.FeedEntity) -> dict[str, Any] | None:
    """
    Flatten a GTFS-realtime FeedEntity with a vehicle position into a dict.

    Optional protobuf fields are omitted when absent. Returns None when the
    entity cannot be keyed (no vehicle id) or has no vehicle position payload.
    """
    if entity.is_deleted:
        logger.debug("Skipping deleted entity id=%s", entity.id)
        return None

    if not entity.HasField("vehicle"):
        logger.debug("Skipping entity id=%s without vehicle position", entity.id)
        return None

    vehicle = entity.vehicle
    record: dict[str, Any] = {}

    if vehicle.HasField("vehicle") and vehicle.vehicle.HasField("id"):
        record["vehicle_id"] = vehicle.vehicle.id
    elif entity.id:
        record["vehicle_id"] = entity.id
        logger.info(
            "Entity id=%s missing vehicle.id; using FeedEntity.id as vehicle_id",
            entity.id,
        )
    else:
        logger.warning("Skipping entity with no vehicle_id")
        return None

    if vehicle.HasField("trip"):
        trip = vehicle.trip
        if trip.HasField("trip_id"):
            record["trip_id"] = trip.trip_id
        else:
            logger.debug("Entity id=%s missing trip.trip_id", entity.id)
        if trip.HasField("route_id"):
            record["route_id"] = trip.route_id
        else:
            logger.debug("Entity id=%s missing trip.route_id", entity.id)
    else:
        logger.debug("Entity id=%s missing trip descriptor", entity.id)

    if vehicle.HasField("position"):
        position = vehicle.position
        if position.HasField("latitude"):
            record["latitude"] = position.latitude
        if position.HasField("longitude"):
            record["longitude"] = position.longitude
    else:
        logger.debug("Entity id=%s missing position", entity.id)

    if vehicle.HasField("timestamp"):
        record["timestamp"] = vehicle.timestamp
    else:
        logger.debug("Entity id=%s missing timestamp", entity.id)

    if vehicle.HasField("current_stop_sequence"):
        record["current_stop_sequence"] = vehicle.current_stop_sequence
    else:
        logger.debug("Entity id=%s missing current_stop_sequence", entity.id)

    if vehicle.HasField("stop_id"):
        record["stop_id"] = vehicle.stop_id
    else:
        logger.debug("Entity id=%s missing stop_id", entity.id)

    if vehicle.HasField("current_status"):
        record["current_status"] = current_status_to_string(vehicle.current_status)
    else:
        logger.debug("Entity id=%s missing current_status", entity.id)

    return record


def parse_feed_bytes(feed_bytes: bytes) -> list[dict[str, Any]]:
    """Parse a GTFS-realtime FeedMessage and flatten vehicle position entities."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(feed_bytes)

    records: list[dict[str, Any]] = []
    for entity in feed.entity:
        flattened = flatten_vehicle_entity(entity)
        if flattened is not None:
            records.append(flattened)
    return records


def build_feed_request_url(feed_url: str, api_key: str | None = None) -> str:
    """Append a OneBusAway API key query param when one is configured."""
    if not api_key or "key=" in feed_url:
        return feed_url
    separator = "&" if "?" in feed_url else "?"
    return f"{feed_url}{separator}key={api_key}"


def fetch_gtfs_realtime_feed(
    url: str,
    session: requests.Session | None = None,
    max_rate_limit_retries: int = DEFAULT_FEED_FETCH_MAX_RETRIES,
    rate_limit_backoff_seconds: float = DEFAULT_FEED_FETCH_BACKOFF_SECONDS,
) -> bytes:
    """
    Download the raw GTFS-realtime protobuf payload.

    HTTP 401/429 responses from third-party feeds (e.g. OneBusAway rate limits
    on the public TEST key) trigger shorter in-cycle backoff-and-retry rather
    than waiting for the next scheduled poll. Other HTTP errors fail immediately.
    """
    http = session or requests.Session()

    for attempt in range(1, max_rate_limit_retries + 1):
        response = http.get(url, timeout=30)

        if response.status_code in RATE_LIMIT_STATUS_CODES:
            logger.warning(
                "GTFS-realtime feed returned HTTP %d (attempt %d/%d); "
                "backing off before retrying in this poll cycle",
                response.status_code,
                attempt,
                max_rate_limit_retries,
            )
            if attempt < max_rate_limit_retries and not _shutdown_requested:
                backoff = rate_limit_backoff_seconds * (2 ** (attempt - 1))
                time.sleep(backoff)
                continue
            response.raise_for_status()

        if response.status_code >= 400:
            logger.error(
                "GTFS-realtime feed request failed with HTTP %d; skipping this poll cycle",
                response.status_code,
            )
            response.raise_for_status()

        return response.content

    raise RuntimeError("GTFS-realtime feed fetch exhausted retries without a response")


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a Kafka producer configured for JSON vehicle-position events."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=0,
    )


def produce_vehicle_position(
    producer: KafkaProducer,
    topic: str,
    record: dict[str, Any],
    max_retries: int = DEFAULT_KAFKA_MAX_RETRIES,
    base_backoff_seconds: float = DEFAULT_KAFKA_BACKOFF_SECONDS,
) -> None:
    """Produce one vehicle position to Kafka with retry and exponential backoff."""
    vehicle_id = record["vehicle_id"]
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            future = producer.send(topic, key=vehicle_id, value=record)
            future.get(timeout=10)
            logger.debug("Produced vehicle_id=%s to topic=%s", vehicle_id, topic)
            return
        except KafkaError as exc:
            last_error = exc
            logger.error(
                "Kafka produce failed for vehicle_id=%s (attempt %d/%d): %s",
                vehicle_id,
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                backoff = base_backoff_seconds * (2 ** (attempt - 1))
                time.sleep(backoff)

    raise KafkaError(
        f"Failed to produce vehicle_id={vehicle_id} after {max_retries} attempts"
    ) from last_error


def produce_vehicle_positions(
    producer: KafkaProducer,
    records: list[dict[str, Any]],
    topic: str = VEHICLE_POSITIONS_TOPIC,
) -> int:
    """Produce a batch of vehicle positions; returns count successfully sent."""
    produced = 0
    for record in records:
        if "vehicle_id" not in record:
            logger.warning("Skipping record without vehicle_id: %s", record)
            continue
        produce_vehicle_position(producer, topic, record)
        produced += 1
    return produced


def run_polling_loop(
    feed_url: str,
    bootstrap_servers: str,
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
    topic: str = VEHICLE_POSITIONS_TOPIC,
    session: requests.Session | None = None,
    producer: KafkaProducer | None = None,
) -> None:
    """
    Poll the GTFS-realtime feed and publish vehicle positions until shutdown.

    Handles SIGINT/SIGTERM by flushing the Kafka producer before exit.
    """
    global _shutdown_requested
    _shutdown_requested = False

    signal.signal(signal.SIGINT, _handle_shutdown_signal)
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)

    owns_producer = producer is None
    kafka_producer = producer or create_kafka_producer(bootstrap_servers)
    http = session or requests.Session()

    logger.info(
        "Starting GTFS-realtime producer url=%s topic=%s interval=%ss",
        feed_url,
        topic,
        poll_interval_seconds,
    )

    try:
        while not _shutdown_requested:
            cycle_start = time.monotonic()
            try:
                feed_bytes = fetch_gtfs_realtime_feed(feed_url, session=http)
                records = parse_feed_bytes(feed_bytes)
                sent = produce_vehicle_positions(kafka_producer, records, topic=topic)
                logger.info("Poll complete: parsed=%d produced=%d", len(records), sent)
            except requests.HTTPError:
                logger.exception("GTFS-realtime HTTP error after in-cycle retries")
            except Exception:
                logger.exception("Poll cycle failed")

            elapsed = time.monotonic() - cycle_start
            sleep_seconds = max(0.0, poll_interval_seconds - elapsed)
            if sleep_seconds > 0 and not _shutdown_requested:
                time.sleep(sleep_seconds)
    finally:
        logger.info("Flushing Kafka producer before exit")
        kafka_producer.flush()
        if owns_producer:
            kafka_producer.close()


def main() -> None:
    """Run the GTFS-realtime vehicle positions producer."""
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    feed_url = os.environ.get("GTFS_REALTIME_URL")
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    api_key = os.environ.get("GTFS_REALTIME_API_KEY")
    poll_interval = int(
        os.environ.get("GTFS_REALTIME_POLL_INTERVAL_SECONDS", DEFAULT_POLL_INTERVAL_SECONDS)
    )

    if not feed_url:
        raise ValueError("GTFS_REALTIME_URL is not set")
    if not bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS is not set")

    run_polling_loop(
        feed_url=build_feed_request_url(feed_url, api_key),
        bootstrap_servers=bootstrap_servers,
        poll_interval_seconds=poll_interval,
    )


if __name__ == "__main__":
    main()
