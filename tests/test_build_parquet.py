import json
from pathlib import Path

import geopandas as gpd
import shapely.wkb

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

    gdf = gpd.read_parquet(parquet_path)
    assert len(gdf) == 3
    assert list(gdf["action"]) == ["create", "modify", "delete"]
    assert gdf.crs.to_epsg() == 4326
    # geometry column should be valid shapely geometries
    assert all(gdf.geometry.is_valid)


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

    gdf = gpd.read_parquet(parquet_dir / "date=2025-01-01" / "data.parquet")
    assert len(gdf) == 1
    old_geom_wkb = gdf.iloc[0]["old_geometry"]
    assert old_geom_wkb is not None
    restored = shapely.wkb.loads(old_geom_wkb)
    assert restored.x == -78.0
    assert restored.y == 37.0
