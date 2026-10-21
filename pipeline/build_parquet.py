import json
import logging
import os
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
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


def _shape(geojson):
    """Like shapely.geometry.shape, but tolerates single-point LineStrings
    written by older versions of the parser."""
    if geojson["type"] == "LineString" and len(geojson["coordinates"]) == 1:
        geojson = {"type": "Point", "coordinates": geojson["coordinates"][0]}
    return shape(geojson)


_TYPES = ["node", "way", "relation"]
_ACTIONS = ["create", "modify", "delete"]
_ID_BITS, _VERSION_BITS = 40, 20


def _pack_keys(osm_types, actions, versions, ids):
    """Pack (osm_type, action, version, osm_id) into one exact uint64 per row.

    Merging has to know which rows a parquet already holds. Packing the identity into 64 bits
    (2 + 2 + 20 + 40) lets a day with millions of rows be checked with a numpy array of ~8 bytes
    a row instead of Python tuples, and there are no hash collisions to worry about.
    """
    types = np.fromiter((_TYPES.index(t) for t in osm_types), dtype=np.uint64)
    acts = np.fromiter((_ACTIONS.index(a) for a in actions), dtype=np.uint64)
    versions = np.asarray(versions, dtype=np.uint64)
    ids = np.asarray(ids, dtype=np.uint64)
    if len(ids) and (ids.max() >> np.uint64(_ID_BITS) or versions.max() >> np.uint64(_VERSION_BITS)):
        raise ValueError("osm_id or version too large to pack into a merge key")
    return (types << np.uint64(62)) | (acts << np.uint64(60)) \
        | (versions << np.uint64(_ID_BITS)) | ids


def _table_keys(table):
    """Packed keys for an arrow table that has the element identity columns."""
    types = pc.index_in(table["osm_type"], value_set=pa.array(_TYPES)).to_numpy(zero_copy_only=False)
    acts = pc.index_in(table["action"], value_set=pa.array(_ACTIONS)).to_numpy(zero_copy_only=False)
    return ((types.astype(np.uint64) << np.uint64(62)) | (acts.astype(np.uint64) << np.uint64(60))
            | (table["version"].to_numpy().astype(np.uint64) << np.uint64(_ID_BITS))
            | table["osm_id"].to_numpy().astype(np.uint64))


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
            geom = _shape(feature["geometry"])

            old_geom_dict = props.get("old_geometry")
            old_wkb = shapely.to_wkb(_shape(old_geom_dict)) if old_geom_dict is not None else None

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

    def built_mtime(self, date_str: str):
        """mtime of the raw file version the parquet currently reflects (None if never built)."""
        return self._mtimes.get(date_str)

    def merge(self, date_str: str, base_path: Path) -> int | None:
        """Add the raw file's rows to an existing parquet, keeping everything already in it.

        Used for a day whose raw file is not the complete history: rows that arrive after the
        day was published. Building fresh from them would replace the published file with just
        those rows. Rows already in `base_path` (same type, id, version and action) are
        skipped, so merging is idempotent and also correct if the raw file is the whole day.

        Returns the number of rows added, or None if there is no raw file. When rows were
        added the merged result is written to this builder's output path (atomically);
        when none were, nothing is written.
        """
        geojsonl_path = self.geojsonl_dir / f"{date_str}.geojsonl"
        if not geojsonl_path.exists():
            return None
        mtime = geojsonl_path.stat().st_mtime

        out_dir = self.parquet_dir / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "data.parquet"
        tmp_path = out_dir / "data.parquet.tmp"

        base = pq.ParquetFile(base_path)
        known = np.empty(0, dtype=np.uint64)
        types = set()
        # The footer's key/value metadata, not the arrow schema: a merged file records its
        # geometry types there when the writer closes
        base_geo = (base.metadata.metadata or {}).get(b"geo")
        if base_geo:
            types.update(json.loads(base_geo)["columns"]["geometry"].get("geometry_types", []))
        for i in range(base.num_row_groups):
            known = np.concatenate([known, _table_keys(base.read_row_group(
                i, columns=["osm_type", "action", "version", "osm_id"]))])
        known = np.unique(known)

        added = 0
        writer = None
        try:
            with open(geojsonl_path) as f:
                while True:
                    rows, geoms = self._read_chunk(f, ROW_GROUP_SIZE)
                    if not rows:
                        break
                    # rows are (action, osm_type, osm_id, version, ...)
                    keys = _pack_keys([r[1] for r in rows], [r[0] for r in rows],
                                      [r[3] for r in rows], [r[2] for r in rows])
                    _, first = np.unique(keys, return_index=True)       # drop repeats within the chunk
                    fresh = np.zeros(len(rows), dtype=bool)
                    fresh[first] = True
                    fresh &= ~np.isin(keys, known)                      # and rows already published
                    if not fresh.any():
                        continue
                    keep = np.flatnonzero(fresh)
                    table, geom_types = self._chunk_to_table([rows[i] for i in keep],
                                                             [geoms[i] for i in keep])
                    types.update(geom_types)
                    known = np.union1d(known, keys[keep])
                    if writer is None:
                        writer = pq.ParquetWriter(tmp_path, PARQUET_SCHEMA, compression="zstd")
                        for i in range(base.num_row_groups):            # keep what was published
                            writer.write_table(base.read_row_group(i).cast(PARQUET_SCHEMA))
                    writer.write_table(table)
                    added += len(table)
        except BaseException:
            if writer is not None:
                writer.close()
            tmp_path.unlink(missing_ok=True)
            raise

        self._mtimes[date_str] = mtime
        if writer is None:
            return 0                                                    # nothing new: leave it alone
        writer.add_key_value_metadata({"geo": _build_geo_metadata(types)})
        writer.close()
        os.replace(tmp_path, out_path)
        logger.info("Merged %d new features into %s", added, out_path)
        return added
