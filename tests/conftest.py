"""Shared test fixtures for Transit Pulse."""

from __future__ import annotations

import io
import zipfile

import pytest

STOPS_CSV = """\
stop_id,stop_name,stop_lat,stop_lon
001,Downtown Station,47.6062,-122.3321
002,Capitol Hill,47.6205,-122.3212
003,University District,47.6587,-122.3120
"""

ROUTES_CSV = """\
route_id,route_short_name,route_long_name,route_type
R01,1,Route One Local,3
R02,2,Route Two Express,3
"""

TRIPS_CSV = """\
route_id,service_id,trip_id
R01,WEEKDAY,T001
R01,WEEKDAY,T002
R02,WEEKDAY,T003
"""

STOP_TIMES_CSV = """\
trip_id,stop_id,stop_sequence,arrival_time,departure_time
T001,001,1,08:00:00,08:00:00
T001,002,2,08:10:00,08:10:00
T001,003,3,08:25:00,08:25:00
T002,001,1,09:00:00,09:00:00
T002,002,2,09:10:00,09:10:00
T003,001,1,10:00:00,10:00:00
T003,003,2,10:20:00,10:20:00
"""


def build_minimal_gtfs_zip(
    stops: str = STOPS_CSV,
    routes: str = ROUTES_CSV,
    trips: str = TRIPS_CSV,
    stop_times: str = STOP_TIMES_CSV,
) -> bytes:
    """Build an in-memory GTFS static ZIP with 3 stops and 2 routes."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("stops.txt", stops)
        archive.writestr("routes.txt", routes)
        archive.writestr("trips.txt", trips)
        archive.writestr("stop_times.txt", stop_times)
    return buffer.getvalue()


@pytest.fixture
def minimal_gtfs_zip() -> bytes:
    return build_minimal_gtfs_zip()
