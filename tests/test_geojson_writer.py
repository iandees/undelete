import json
from pathlib import Path

from daemon.geojson_writer import GeoJSONWriter


def _make_feature(osm_id=1, osm_type="node", lon=0.0, lat=0.0):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "osm_type": osm_type,
            "osm_id": osm_id,
            "version": 1,
            "tags": {},
            "deleted_by": "user",
            "deleted_uid": 1,
            "deleted_changeset": 1,
            "deleted_at": "2025-01-14T14:51:43Z",
        },
    }


def test_append_creates_daily_file(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    feature = _make_feature()
    writer.append(feature, date_str="2025-01-14")

    daily_file = tmp_path / "2025-01-14.geojsonl"
    assert daily_file.exists()

    lines = daily_file.read_text().strip().split("\n")
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["properties"]["osm_id"] == 1


def test_append_multiple_features(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    writer.append(_make_feature(osm_id=1), date_str="2025-01-14")
    writer.append(_make_feature(osm_id=2), date_str="2025-01-14")

    daily_file = tmp_path / "2025-01-14.geojsonl"
    lines = daily_file.read_text().strip().split("\n")
    assert len(lines) == 2


def test_get_today_geojson(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    writer.append(_make_feature(osm_id=1), date_str="2025-01-14")
    writer.append(_make_feature(osm_id=2), date_str="2025-01-14")

    fc = writer.get_feature_collection("2025-01-14")
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == 2


def test_list_daily_files(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    writer.append(_make_feature(), date_str="2025-01-13")
    writer.append(_make_feature(), date_str="2025-01-14")

    files = writer.list_daily_files()
    assert len(files) == 2
    assert "2025-01-13" in files
    assert "2025-01-14" in files
