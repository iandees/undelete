import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from pipeline.build_parquet import ParquetBuilder


def _make_feature(action, osm_type, osm_id, lon, lat, version=1, changeset=1,
                  user="test", uid=1, tags=None, old_tags=None,
                  old_geometry=None):
    feat = {
        "type": "Feature",
        "properties": {
            "action": action,
            "osm_type": osm_type,
            "osm_id": osm_id,
            "version": version,
            "changeset": changeset,
            "user": user,
            "uid": uid,
            "timestamp": "2025-01-01T00:00:00Z",
            "tags": tags or {},
            "old_tags": old_tags,
        },
        "geometry": {
            "type": "Point",
            "coordinates": [lon, lat],
        },
    }
    if old_geometry is not None:
        feat["properties"]["old_geometry"] = old_geometry
    return feat


def test_build_parquet_from_geojsonl(tmp_path):
    geojsonl_dir = tmp_path / "geojsonl"
    geojsonl_dir.mkdir()
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    features = [
        _make_feature("create", "node", 1, -77.0, 38.9),
        _make_feature("modify", "node", 2, -76.0, 39.0, version=2,
                       tags={"name": "Foo"}, old_tags={"name": "Bar"}),
        _make_feature("delete", "node", 3, -75.0, 40.0),
    ]
    geojsonl_file = geojsonl_dir / "2025-01-01.geojsonl"
    geojsonl_file.write_text("\n".join(json.dumps(f) for f in features))

    builder = ParquetBuilder(geojsonl_dir, parquet_dir)
    result = builder.build("2025-01-01")

    assert result is True
    parquet_path = parquet_dir / "date=2025-01-01" / "data.parquet"
    assert parquet_path.exists()

    table = pq.read_table(parquet_path)
    assert table.num_rows == 3
    assert set(table.column("action").to_pylist()) == {"create", "modify", "delete"}

    # Verify GeoParquet metadata
    geo_meta = json.loads(table.schema.metadata[b"geo"])
    assert geo_meta["version"] == "1.1.0"
    assert geo_meta["primary_column"] == "geometry"
    assert "covering" in geo_meta["columns"]["geometry"]

    # Verify tags are MAP type
    tags_col = table.column("tags")
    assert tags_col.type == pa.map_(pa.string(), pa.string())
    # The modify row has tags {"name": "Foo"}
    tag_maps = tags_col.to_pylist()
    assert any(("name", "Foo") in items for items in tag_maps)

    # Verify geometry is valid WKB
    for wkb_bytes in table.column("geometry").to_pylist():
        geom = shapely.from_wkb(wkb_bytes)
        assert geom.is_valid

    # Verify bbox column exists
    assert "bbox" in table.schema.names
    bbox_col = table.column("bbox").to_pylist()
    for bbox in bbox_col:
        assert "xmin" in bbox and "ymin" in bbox


def test_build_skips_if_unchanged(tmp_path):
    geojsonl_dir = tmp_path / "geojsonl"
    geojsonl_dir.mkdir()
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    feat = _make_feature("create", "node", 1, -77.0, 38.9)
    geojsonl_file = geojsonl_dir / "2025-01-01.geojsonl"
    geojsonl_file.write_text(json.dumps(feat))

    builder = ParquetBuilder(geojsonl_dir, parquet_dir)
    assert builder.build("2025-01-01") is True
    assert builder.build("2025-01-01") is False


def test_build_nonexistent_date(tmp_path):
    geojsonl_dir = tmp_path / "geojsonl"
    geojsonl_dir.mkdir()
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    builder = ParquetBuilder(geojsonl_dir, parquet_dir)
    assert builder.build("2099-12-31") is False


def test_old_geometry_preserved(tmp_path):
    geojsonl_dir = tmp_path / "geojsonl"
    geojsonl_dir.mkdir()
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    old_geom = {"type": "Point", "coordinates": [-78.0, 37.0]}
    feat = _make_feature("modify", "node", 10, -77.0, 38.0,
                         old_geometry=old_geom)
    geojsonl_file = geojsonl_dir / "2025-01-01.geojsonl"
    geojsonl_file.write_text(json.dumps(feat))

    builder = ParquetBuilder(geojsonl_dir, parquet_dir)
    builder.build("2025-01-01")

    table = pq.read_table(parquet_dir / "date=2025-01-01" / "data.parquet")
    assert table.num_rows == 1
    old_geom_wkb = table.column("old_geometry").to_pylist()[0]
    assert old_geom_wkb is not None
    restored = shapely.from_wkb(old_geom_wkb)
    assert restored.x == -78.0
    assert restored.y == 37.0


def test_tags_as_map(tmp_path):
    geojsonl_dir = tmp_path / "geojsonl"
    geojsonl_dir.mkdir()
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    feat = _make_feature("create", "node", 1, -77.0, 38.9,
                         tags={"building": "yes", "name": "Test Building"})
    geojsonl_file = geojsonl_dir / "2025-01-01.geojsonl"
    geojsonl_file.write_text(json.dumps(feat))

    builder = ParquetBuilder(geojsonl_dir, parquet_dir)
    builder.build("2025-01-01")

    table = pq.read_table(parquet_dir / "date=2025-01-01" / "data.parquet")
    tag_map = table.column("tags").to_pylist()[0]
    assert ("building", "yes") in tag_map
    assert ("name", "Test Building") in tag_map
