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

    def _read_chunk(self, f, chunk_size):
        """Read up to chunk_size features from an open GEOJSONL file.

        Returns (rows, geom_objects) where rows is a list of parsed row tuples
        and geom_objects is a list of shapely geometry objects.
        Returns empty lists at EOF.
        """
        rows = []
        geom_objects = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            feature = json.loads(line)
            props = feature["properties"]
            geom = shape(feature["geometry"])

            old_geom_dict = props.get("old_geometry")
            old_wkb = shapely.to_wkb(shape(old_geom_dict)) if old_geom_dict is not None else None

            rows.append((
                props["action"],
                props["osm_type"],
                props["osm_id"],
                props["version"],
                props["changeset"],
                props["user"],
                props["uid"],
                props["timestamp"],
                _dict_to_map_items(props.get("tags") or {}),
                _dict_to_map_items(props["old_tags"]) if props.get("old_tags") else None,
                shapely.to_wkb(geom),
                old_wkb,
            ))
            geom_objects.append(geom)

            if len(rows) >= chunk_size:
                break
        return rows, geom_objects

    def _chunk_to_table(self, rows, geom_objects):
        """Convert a chunk of rows + geom_objects into a sorted PyArrow table.

        Returns (table, geometry_types).
        """
        import geopandas as gpd

        # Hilbert sort for spatial clustering
        hilbert_distances = gpd.GeoSeries(geom_objects).hilbert_distance()
        sort_idx = hilbert_distances.argsort().tolist()
        del hilbert_distances

        rows = [rows[i] for i in sort_idx]
        geom_objects = [geom_objects[i] for i in sort_idx]
        del sort_idx

        # Compute bbox and collect geometry types
        bboxes = []
        geometry_types = set()
        for geom in geom_objects:
            bounds = geom.bounds
            bboxes.append({"xmin": bounds[0], "ymin": bounds[1], "xmax": bounds[2], "ymax": bounds[3]})
            geometry_types.add(geom.geom_type)
        del geom_objects

        table = pa.table(
            {
                "action": pa.array([r[0] for r in rows], type=pa.string()),
                "osm_type": pa.array([r[1] for r in rows], type=pa.string()),
                "osm_id": pa.array([r[2] for r in rows], type=pa.int64()),
                "version": pa.array([r[3] for r in rows], type=pa.int32()),
                "changeset": pa.array([r[4] for r in rows], type=pa.int64()),
                "user": pa.array([r[5] for r in rows], type=pa.string()),
                "uid": pa.array([r[6] for r in rows], type=pa.int64()),
                "timestamp": pa.array([r[7] for r in rows], type=pa.string()),
                "tags": pa.array([r[8] for r in rows], type=pa.map_(pa.string(), pa.string())),
                "old_tags": pa.array([r[9] for r in rows], type=pa.map_(pa.string(), pa.string())),
                "geometry": pa.array([r[10] for r in rows], type=pa.binary()),
                "old_geometry": pa.array([r[11] for r in rows], type=pa.binary()),
                "bbox": pa.array(bboxes, type=pa.struct([
                    ("xmin", pa.float64()),
                    ("xmax", pa.float64()),
                    ("ymin", pa.float64()),
                    ("ymax", pa.float64()),
                ])),
            },
            schema=PARQUET_SCHEMA,
        )
        return table, geometry_types

    def build(self, date_str: str) -> bool:
        geojsonl_path = self.geojsonl_dir / f"{date_str}.geojsonl"

        if not geojsonl_path.exists():
            return False

        mtime = geojsonl_path.stat().st_mtime
        if self._mtimes.get(date_str) == mtime:
            return False

        out_dir = self.parquet_dir / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        total_features = 0
        n_groups = 0
        all_geometry_types = set()
        writer = None

        try:
            with open(geojsonl_path) as f:
                while True:
                    rows, geom_objects = self._read_chunk(f, ROW_GROUP_SIZE)
                    if not rows:
                        break

                    table, geometry_types = self._chunk_to_table(rows, geom_objects)
                    all_geometry_types.update(geometry_types)
                    del rows, geom_objects

                    if writer is None:
                        # Add GeoParquet metadata — will be finalized on close
                        geo_meta = _build_geo_metadata(all_geometry_types)
                        meta = {b"geo": geo_meta.encode("utf-8")}
                        schema = table.schema.with_metadata(meta)
                        writer = pq.ParquetWriter(out_path, schema, compression="zstd")

                    writer.write_table(table)
                    total_features += len(table)
                    n_groups += 1
                    del table
        finally:
            if writer is not None:
                # Update geo metadata with all geometry types seen across chunks
                writer.close()

        if total_features == 0:
            return False

        self._mtimes[date_str] = mtime
        logger.info("Built %s (%d features, %d row groups)", out_path, total_features, n_groups)
        return True
