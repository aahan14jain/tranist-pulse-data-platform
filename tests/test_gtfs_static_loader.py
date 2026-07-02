"""Tests for ingestion.gtfs_static_loader."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.gtfs_static_loader import (
    GtfsValidationError,
    download_gtfs_zip,
    load_gtfs_static,
    parse_gtfs_zip,
    write_tables_to_minio,
)
from tests.conftest import build_minimal_gtfs_zip


def test_parse_gtfs_zip_row_counts_and_dtypes(minimal_gtfs_zip: bytes) -> None:
    tables = parse_gtfs_zip(minimal_gtfs_zip)

    assert set(tables) == {"stops", "routes", "trips", "stop_times"}
    assert len(tables["stops"]) == 3
    assert len(tables["routes"]) == 2
    assert len(tables["trips"]) == 3
    assert len(tables["stop_times"]) == 7

    assert pd.api.types.is_string_dtype(tables["stops"]["stop_id"])
    assert tables["stops"]["stop_id"].tolist() == ["001", "002", "003"]

    assert pd.api.types.is_string_dtype(tables["routes"]["route_id"])
    assert tables["routes"]["route_id"].tolist() == ["R01", "R02"]

    assert pd.api.types.is_string_dtype(tables["trips"]["trip_id"])
    assert pd.api.types.is_string_dtype(tables["trips"]["service_id"])

    assert pd.api.types.is_string_dtype(tables["stop_times"]["trip_id"])
    assert pd.api.types.is_string_dtype(tables["stop_times"]["stop_id"])
    assert tables["stop_times"]["stop_sequence"].dtype in (int, "int64")


def test_parse_gtfs_zip_finds_nested_csv_paths() -> None:
    zip_bytes = build_minimal_gtfs_zip()
    # Re-wrap with a subdirectory prefix like some agency feeds use.
    import io
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as source:
            for name in source.namelist():
                archive.writestr(f"gtfs/{name}", source.read(name))
    nested_zip = buffer.getvalue()

    tables = parse_gtfs_zip(nested_zip)
    assert len(tables["stops"]) == 3


def test_parse_gtfs_zip_missing_file_raises() -> None:
    zip_bytes = build_minimal_gtfs_zip()
    import io
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as source:
            for name in source.namelist():
                if name != "stops.txt":
                    archive.writestr(name, source.read(name))

    with pytest.raises(GtfsValidationError, match="Missing required GTFS file: stops.txt"):
        parse_gtfs_zip(buffer.getvalue())


def test_parse_gtfs_zip_missing_column_raises() -> None:
    bad_stops = "stop_id,stop_lat,stop_lon\n001,47.6,-122.3\n"
    zip_bytes = build_minimal_gtfs_zip(stops=bad_stops)

    with pytest.raises(
        GtfsValidationError,
        match=r"stops\.txt is missing required column\(s\): stop_name",
    ):
        parse_gtfs_zip(zip_bytes)


def test_parse_gtfs_zip_routes_missing_name_columns_raises() -> None:
    bad_routes = "route_id,route_type\nR01,3\n"
    zip_bytes = build_minimal_gtfs_zip(routes=bad_routes)

    with pytest.raises(
        GtfsValidationError,
        match=r"routes\.txt is missing required column\(s\): route_short_name or route_long_name",
    ):
        parse_gtfs_zip(zip_bytes)


def test_download_gtfs_zip_uses_session() -> None:
    session = MagicMock()
    response = MagicMock()
    response.content = b"zip-bytes"
    response.raise_for_status.return_value = None
    session.get.return_value = response

    result = download_gtfs_zip("https://example.com/gtfs.zip", session=session)

    assert result == b"zip-bytes"
    session.get.assert_called_once_with("https://example.com/gtfs.zip", timeout=120)


def test_write_tables_to_minio() -> None:
    s3 = MagicMock()
    tables = parse_gtfs_zip(build_minimal_gtfs_zip())

    keys = write_tables_to_minio(
        tables,
        s3_client=s3,
        bucket="raw-zone",
        ingestion_date="2026-07-01",
    )

    assert keys == {
        "stops": "gtfs-static/stops/2026-07-01/stops.parquet",
        "routes": "gtfs-static/routes/2026-07-01/routes.parquet",
        "trips": "gtfs-static/trips/2026-07-01/trips.parquet",
        "stop_times": "gtfs-static/stop_times/2026-07-01/stop_times.parquet",
    }
    assert s3.put_object.call_count == 4

    for call in s3.put_object.call_args_list:
        assert call.kwargs["Bucket"] == "raw-zone"
        assert call.kwargs["Key"].startswith("gtfs-static/")
        assert call.kwargs["Body"]  # non-empty parquet bytes


@patch("ingestion.gtfs_static_loader.write_tables_to_minio")
@patch("ingestion.gtfs_static_loader.download_gtfs_zip")
def test_load_gtfs_static_end_to_end_mocked(
    mock_download: MagicMock,
    mock_write: MagicMock,
    minimal_gtfs_zip: bytes,
) -> None:
    mock_download.return_value = minimal_gtfs_zip
    s3 = MagicMock()

    tables = load_gtfs_static(
        gtfs_url="https://example.com/gtfs.zip",
        ingestion_date="2026-07-01",
        s3_client=s3,
    )

    mock_download.assert_called_once()
    mock_write.assert_called_once_with(
        tables,
        s3_client=s3,
        ingestion_date="2026-07-01",
    )
    assert len(tables["stops"]) == 3
