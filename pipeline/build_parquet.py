import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import shape

logger = logging.getLogger(__name__)

# Target row group size in rows. With Hilbert sorting, each row group covers
# a compact spatial area. gpio recommends 10K-200K rows per group and 64-256MB
# per group. At ~2KB/row, 50K rows ≈ 100MB per group — within the sweet spot.
ROW_GROUP_SIZE = 50_000

PARQUET_SCHEMA = pa.schema([
    ("action", pa.string()),
    ("osm_type", pa.string()),
    ("osm_id", pa.int64()),
    ("version", pa.int32()),
    ("changeset", pa.int64()),
    ("user", pa.string()),
    ("uid", pa.int64()),
    ("timestamp", pa.string()),
    ("tags", pa.map_(pa.string(), pa.string())),
    ("old_tags", pa.map_(pa.string(), pa.string())),
    ("geometry", pa.binary()),
    ("old_geometry", pa.binary()),
    # Covering bbox columns for spatial filtering
    ("bbox", pa.struct([
        ("xmin", pa.float64()),
        ("xmax", pa.float64()),
        ("ymin", pa.float64()),
        ("ymax", pa.float64()),
    ])),
])


def _dict_to_map_items(d):
    """Convert a dict to a list of (key, value) tuples for pyarrow map columns."""
    if not d:
        return []
    return list(d.items())


def _build_geo_metadata(geometry_types):
    """Build GeoParquet 1.1.0 metadata for the file footer."""
    return json.dumps({
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": sorted(geometry_types),
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "datum": {
                        "type": "GeodeticReferenceFrame",
                        "name": "World Geodetic System 1984",
                        "ellipsoid": {
                            "name": "WGS 84",
                            "semi_major_axis": 6378137,
                            "inverse_flattening": 298.257223563,
                        },
                    },
                    "coordinate_system": {
                        "subtype": "ellipsoidal",
                        "axis": [
                            {"name": "Longitude", "abbreviation": "lon", "direction": "east", "unit": "degree"},
                            {"name": "Latitude", "abbreviation": "lat", "direction": "north", "unit": "degree"},
                        ],
                    },
                    "id": {"authority": "EPSG", "code": 4326},
                },
                "covering": {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    },
                },
            },
        },
    })


class ParquetBuilder:
    def __init__(self, geojsonl_dir: Path, parquet_dir: Path):
        self.geojsonl_dir = Path(geojsonl_dir)
        self.parquet_dir = Path(parquet_dir)
        self._mtimes: dict[str, float] = {}

    def build(self, date_str: str) -> bool:
        geojsonl_path = self.geojsonl_dir / f"{date_str}.geojsonl"

        if not geojsonl_path.exists():
            return False

        mtime = geojsonl_path.stat().st_mtime
        if self._mtimes.get(date_str) == mtime:
            return False

        # Parse all features
        actions = []
        osm_types = []
        osm_ids = []
        versions = []
        changesets = []
        users = []
        uids = []
        timestamps = []
        tags_list = []
        old_tags_list = []
        geometries = []
        old_geometries = []
        geom_objects = []  # shapely objects for Hilbert sorting

        with open(geojsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                feature = json.loads(line)
                props = feature["properties"]
                geom = shape(feature["geometry"])

                actions.append(props["action"])
                osm_types.append(props["osm_type"])
                osm_ids.append(props["osm_id"])
                versions.append(props["version"])
                changesets.append(props["changeset"])
                users.append(props["user"])
                uids.append(props["uid"])
                timestamps.append(props["timestamp"])
                tags_list.append(_dict_to_map_items(props.get("tags") or {}))
                old_tags_list.append(_dict_to_map_items(props["old_tags"]) if props.get("old_tags") else None)
                geometries.append(shapely.to_wkb(geom))

                old_geom_dict = props.get("old_geometry")
                if old_geom_dict is not None:
                    old_geometries.append(shapely.to_wkb(shape(old_geom_dict)))
                else:
                    old_geometries.append(None)

                geom_objects.append(geom)

        if not actions:
            return False

        # Hilbert sort for spatial clustering
        import geopandas as gpd
        hilbert_distances = gpd.GeoSeries(geom_objects).hilbert_distance()
        sort_idx = hilbert_distances.argsort().tolist()

        # Apply sort order to all columns
        actions = [actions[i] for i in sort_idx]
        osm_types = [osm_types[i] for i in sort_idx]
        osm_ids = [osm_ids[i] for i in sort_idx]
        versions = [versions[i] for i in sort_idx]
        changesets = [changesets[i] for i in sort_idx]
        users = [users[i] for i in sort_idx]
        uids = [uids[i] for i in sort_idx]
        timestamps = [timestamps[i] for i in sort_idx]
        tags_list = [tags_list[i] for i in sort_idx]
        old_tags_list = [old_tags_list[i] for i in sort_idx]
        geometries = [geometries[i] for i in sort_idx]
        old_geometries = [old_geometries[i] for i in sort_idx]
        geom_objects = [geom_objects[i] for i in sort_idx]

        # Compute bbox for each geometry
        bboxes = []
        geometry_types = set()
        for geom in geom_objects:
            bounds = geom.bounds  # (minx, miny, maxx, maxy)
            bboxes.append({"xmin": bounds[0], "ymin": bounds[1], "xmax": bounds[2], "ymax": bounds[3]})
            geometry_types.add(geom.geom_type)

        # Build pyarrow table
        table = pa.table(
            {
                "action": pa.array(actions, type=pa.string()),
                "osm_type": pa.array(osm_types, type=pa.string()),
                "osm_id": pa.array(osm_ids, type=pa.int64()),
                "version": pa.array(versions, type=pa.int32()),
                "changeset": pa.array(changesets, type=pa.int64()),
                "user": pa.array(users, type=pa.string()),
                "uid": pa.array(uids, type=pa.int64()),
                "timestamp": pa.array(timestamps, type=pa.string()),
                "tags": pa.array(tags_list, type=pa.map_(pa.string(), pa.string())),
                "old_tags": pa.array(old_tags_list, type=pa.map_(pa.string(), pa.string())),
                "geometry": pa.array(geometries, type=pa.binary()),
                "old_geometry": pa.array(old_geometries, type=pa.binary()),
                "bbox": pa.array(bboxes, type=pa.struct([
                    ("xmin", pa.float64()),
                    ("xmax", pa.float64()),
                    ("ymin", pa.float64()),
                    ("ymax", pa.float64()),
                ])),
            },
            schema=PARQUET_SCHEMA,
        )

        # Add GeoParquet metadata to the schema
        geo_meta = _build_geo_metadata(geometry_types)
        existing_meta = table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": geo_meta.encode("utf-8")}
        table = table.replace_schema_metadata(new_meta)

        # Write
        out_dir = self.parquet_dir / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"
        pq.write_table(
            table,
            out_path,
            compression="zstd",
            row_group_size=ROW_GROUP_SIZE,
        )

        n_groups = (len(actions) + ROW_GROUP_SIZE - 1) // ROW_GROUP_SIZE
        self._mtimes[date_str] = mtime
        logger.info("Built %s (%d features, %d row groups)", out_path, len(actions), n_groups)
        return True
