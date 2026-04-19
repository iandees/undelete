"""Build GeoParquet files from daily changeset JSONL files."""

import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import box

logger = logging.getLogger(__name__)

ROW_GROUP_SIZE = 50_000

CHANGESET_PARQUET_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("created_at", pa.string()),
    ("closed_at", pa.string()),
    ("open", pa.bool_()),
    ("num_changes", pa.int32()),
    ("user", pa.string()),
    ("uid", pa.int64()),
    ("comments_count", pa.int32()),
    ("tags", pa.map_(pa.string(), pa.string())),
    ("geometry", pa.binary()),
    ("bbox", pa.struct([
        ("xmin", pa.float64()),
        ("xmax", pa.float64()),
        ("ymin", pa.float64()),
        ("ymax", pa.float64()),
    ])),
])


def _dict_to_map_items(d):
    if not d:
        return []
    return list(d.items())


def _build_geo_metadata(geometry_types):
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


class ChangesetParquetBuilder:
    def __init__(self, jsonl_dir: Path, parquet_dir: Path):
        self.jsonl_dir = Path(jsonl_dir)
        self.parquet_dir = Path(parquet_dir)
        self._mtimes: dict[str, float] = {}

    def build(self, date_str: str) -> bool:
        jsonl_path = self.jsonl_dir / f"{date_str}.jsonl"

        if not jsonl_path.exists():
            return False

        mtime = jsonl_path.stat().st_mtime
        if self._mtimes.get(date_str) == mtime:
            return False

        # Read all entries, deduplicating by changeset ID (keep latest version).
        # We stream the file but must keep the dedup dict since the same
        # changeset can appear multiple times across replication files.
        by_id = {}
        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                cs = json.loads(line)
                by_id[cs["id"]] = cs

        if not by_id:
            return False

        out_dir = self.parquet_dir / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"

        # Process in chunks to limit peak memory
        import geopandas as gpd

        all_ids = list(by_id.keys())
        # Free the values we've already grabbed keys for — we'll pop as we go
        total = len(all_ids)
        n_groups = 0
        all_geometry_types = set()
        writer = None

        try:
            for chunk_start in range(0, total, ROW_GROUP_SIZE):
                chunk_keys = all_ids[chunk_start:chunk_start + ROW_GROUP_SIZE]

                rows = []
                geom_objects = []
                for cid in chunk_keys:
                    cs = by_id.pop(cid)  # pop to free memory as we go
                    if cs.get("min_lon") is not None:
                        geom = box(cs["min_lon"], cs["min_lat"], cs["max_lon"], cs["max_lat"])
                    else:
                        geom = shapely.Point(0, 0)

                    rows.append((
                        cs["id"],
                        cs["created_at"],
                        cs.get("closed_at", ""),
                        cs.get("open", False),
                        cs.get("num_changes", 0),
                        cs.get("user", ""),
                        cs.get("uid", 0),
                        cs.get("comments_count", 0),
                        _dict_to_map_items(cs.get("tags") or {}),
                        shapely.to_wkb(geom),
                    ))
                    geom_objects.append(geom)

                # Hilbert sort within chunk
                hilbert_distances = gpd.GeoSeries(geom_objects).hilbert_distance()
                sort_idx = hilbert_distances.argsort().tolist()
                del hilbert_distances
                rows = [rows[i] for i in sort_idx]
                geom_objects = [geom_objects[i] for i in sort_idx]
                del sort_idx

                bboxes = []
                geometry_types = set()
                for geom in geom_objects:
                    bounds = geom.bounds
                    bboxes.append({"xmin": bounds[0], "ymin": bounds[1], "xmax": bounds[2], "ymax": bounds[3]})
                    geometry_types.add(geom.geom_type)
                all_geometry_types.update(geometry_types)
                del geom_objects

                table = pa.table(
                    {
                        "id": pa.array([r[0] for r in rows], type=pa.int64()),
                        "created_at": pa.array([r[1] for r in rows], type=pa.string()),
                        "closed_at": pa.array([r[2] for r in rows], type=pa.string()),
                        "open": pa.array([r[3] for r in rows], type=pa.bool_()),
                        "num_changes": pa.array([r[4] for r in rows], type=pa.int32()),
                        "user": pa.array([r[5] for r in rows], type=pa.string()),
                        "uid": pa.array([r[6] for r in rows], type=pa.int64()),
                        "comments_count": pa.array([r[7] for r in rows], type=pa.int32()),
                        "tags": pa.array([r[8] for r in rows], type=pa.map_(pa.string(), pa.string())),
                        "geometry": pa.array([r[9] for r in rows], type=pa.binary()),
                        "bbox": pa.array(bboxes, type=pa.struct([
                            ("xmin", pa.float64()),
                            ("xmax", pa.float64()),
                            ("ymin", pa.float64()),
                            ("ymax", pa.float64()),
                        ])),
                    },
                    schema=CHANGESET_PARQUET_SCHEMA,
                )
                del rows, bboxes

                if writer is None:
                    geo_meta = _build_geo_metadata(all_geometry_types)
                    meta = {b"geo": geo_meta.encode("utf-8")}
                    schema = table.schema.with_metadata(meta)
                    writer = pq.ParquetWriter(out_path, schema, compression="zstd")

                writer.write_table(table)
                n_groups += 1
                del table
        finally:
            if writer is not None:
                writer.close()

        self._mtimes[date_str] = mtime
        logger.info("Built %s (%d changesets, %d row groups)", out_path, total, n_groups)
        return True
