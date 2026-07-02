"""GTFS-realtime protobuf fixtures for tests."""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2 as gtfs


def build_full_vehicle_entity(entity_id: str = "veh-1") -> gtfs.FeedEntity:
    """Build a FeedEntity with all flattened vehicle-position fields populated."""
    entity = gtfs.FeedEntity()
    entity.id = entity_id

    vehicle = entity.vehicle
    vehicle.vehicle.id = "BUS-42"
    vehicle.trip.trip_id = "T001"
    vehicle.trip.route_id = "R01"
    vehicle.position.latitude = 47.6062
    vehicle.position.longitude = -122.3321
    vehicle.timestamp = 1719900000
    vehicle.current_stop_sequence = 5
    vehicle.stop_id = "001"
    vehicle.current_status = gtfs.VehiclePosition.IN_TRANSIT_TO

    return entity


def build_minimal_vehicle_entity(
    entity_id: str = "veh-min",
    vehicle_id: str | None = "BUS-99",
) -> gtfs.FeedEntity:
    """Build a FeedEntity with only required fields for Kafka keying and position."""
    entity = gtfs.FeedEntity()
    entity.id = entity_id

    if vehicle_id is not None:
        entity.vehicle.vehicle.id = vehicle_id
    entity.vehicle.position.latitude = 47.62
    entity.vehicle.position.longitude = -122.32

    return entity


def build_feed_bytes(entities: list[gtfs.FeedEntity]) -> bytes:
    """Serialize a FeedMessage containing the given entities."""
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1719900000
    feed.entity.extend(entities)
    return feed.SerializeToString()
