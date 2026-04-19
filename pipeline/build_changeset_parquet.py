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

        # Read all entries, deduplicating by changeset ID (keep latest version)
        by_id = {}
        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                cs = json.loads(line)
                by_id[cs["id"]] = cs

        ids = []
        created_ats = []
        closed_ats = []
        opens = []
        num_changes_list = []
        users = []
        uids = []
        comments_counts = []
        tags_list = []
        geometries = []
        geom_objects = []

        for cs in by_id.values():
            ids.append(cs["id"])
            created_ats.append(cs["created_at"])
            closed_ats.append(cs.get("closed_at", ""))
            opens.append(cs.get("open", False))
            num_changes_list.append(cs.get("num_changes", 0))
            users.append(cs.get("user", ""))
            uids.append(cs.get("uid", 0))
            comments_counts.append(cs.get("comments_count", 0))
            tags_list.append(_dict_to_map_items(cs.get("tags") or {}))

            # Build geometry from bbox if available
            if cs.get("min_lon") is not None:
                geom = box(cs["min_lon"], cs["min_lat"], cs["max_lon"], cs["max_lat"])
            else:
                # No bbox — use null island point as placeholder
                geom = shapely.Point(0, 0)

            geometries.append(shapely.to_wkb(geom))
            geom_objects.append(geom)

        if not ids:
            return False

        # Hilbert sort for spatial clustering
        import geopandas as gpd
        hilbert_distances = gpd.GeoSeries(geom_objects).hilbert_distance()
        sort_idx = hilbert_distances.argsort().tolist()

        # Apply sort order
        ids = [ids[i] for i in sort_idx]
        created_ats = [created_ats[i] for i in sort_idx]
        closed_ats = [closed_ats[i] for i in sort_idx]
        opens = [opens[i] for i in sort_idx]
        num_changes_list = [num_changes_list[i] for i in sort_idx]
        users = [users[i] for i in sort_idx]
        uids = [uids[i] for i in sort_idx]
        comments_counts = [comments_counts[i] for i in sort_idx]
        tags_list = [tags_list[i] for i in sort_idx]
        geometries = [geometries[i] for i in sort_idx]
        geom_objects = [geom_objects[i] for i in sort_idx]

        # Compute bbox for each geometry
        bboxes = []
        geometry_types = set()
        for geom in geom_objects:
            bounds = geom.bounds
            bboxes.append({"xmin": bounds[0], "ymin": bounds[1], "xmax": bounds[2], "ymax": bounds[3]})
            geometry_types.add(geom.geom_type)

        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "created_at": pa.array(created_ats, type=pa.string()),
                "closed_at": pa.array(closed_ats, type=pa.string()),
                "open": pa.array(opens, type=pa.bool_()),
                "num_changes": pa.array(num_changes_list, type=pa.int32()),
                "user": pa.array(users, type=pa.string()),
                "uid": pa.array(uids, type=pa.int64()),
                "comments_count": pa.array(comments_counts, type=pa.int32()),
                "tags": pa.array(tags_list, type=pa.map_(pa.string(), pa.string())),
                "geometry": pa.array(geometries, type=pa.binary()),
                "bbox": pa.array(bboxes, type=pa.struct([
                    ("xmin", pa.float64()),
                    ("xmax", pa.float64()),
                    ("ymin", pa.float64()),
                    ("ymax", pa.float64()),
                ])),
            },
            schema=CHANGESET_PARQUET_SCHEMA,
        )

        geo_meta = _build_geo_metadata(geometry_types)
        existing_meta = table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": geo_meta.encode("utf-8")}
        table = table.replace_schema_metadata(new_meta)

        out_dir = self.parquet_dir / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"
        pq.write_table(
            table,
            out_path,
            compression="zstd",
            row_group_size=ROW_GROUP_SIZE,
        )

        n_groups = (len(ids) + ROW_GROUP_SIZE - 1) // ROW_GROUP_SIZE
        self._mtimes[date_str] = mtime
        logger.info("Built %s (%d changesets, %d row groups)", out_path, len(ids), n_groups)
        return True
