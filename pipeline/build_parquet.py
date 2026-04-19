import json
import logging
from pathlib import Path

import geopandas as gpd
import shapely.geometry
import shapely.wkb

logger = logging.getLogger(__name__)

# Row group size controls spatial filtering granularity.
# Smaller = more row groups = better bbox skip rate, but more Parquet overhead.
ROW_GROUP_SIZE = 10_000


def _geohash_sort_key(geom):
    """Return a geohash-like interleaved bit string for spatial sorting.

    Sorts by interleaving lat/lon bits so nearby geometries cluster together.
    This ensures each row group covers a small spatial extent, making
    bbox-based row group skipping effective.
    """
    c = geom.centroid
    # Normalize to 0-1 range
    x = (c.x + 180) / 360
    y = (c.y + 90) / 180
    # Interleave 16 bits of x and y for a 32-bit spatial key
    key = 0
    ix = int(x * 65536)
    iy = int(y * 65536)
    for i in range(16):
        bit = 1 << (15 - i)
        key = (key << 2) | ((1 if ix & bit else 0) << 1) | (1 if iy & bit else 0)
    return key


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

        rows = []
        with open(geojsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                feature = json.loads(line)
                props = feature["properties"]
                geom = shapely.geometry.shape(feature["geometry"])

                old_geom_dict = props.get("old_geometry")
                if old_geom_dict is not None:
                    old_geom_bytes = shapely.wkb.dumps(
                        shapely.geometry.shape(old_geom_dict)
                    )
                else:
                    old_geom_bytes = None

                rows.append({
                    "action": props["action"],
                    "osm_type": props["osm_type"],
                    "osm_id": props["osm_id"],
                    "version": props["version"],
                    "changeset": props["changeset"],
                    "user": props["user"],
                    "uid": props["uid"],
                    "timestamp": props["timestamp"],
                    "tags": json.dumps(props.get("tags") or {}),
                    "old_tags": json.dumps(props["old_tags"]) if props.get("old_tags") is not None else None,
                    "geometry": geom,
                    "old_geometry": old_geom_bytes,
                })

        if not rows:
            return False

        gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

        # Sort spatially so each row group covers a small geographic area
        gdf["_spatial_key"] = gdf.geometry.apply(_geohash_sort_key)
        gdf = gdf.sort_values("_spatial_key").drop(columns=["_spatial_key"]).reset_index(drop=True)

        out_dir = self.parquet_dir / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(
            out_dir / "data.parquet",
            write_covering_bbox=True,
            row_group_size=ROW_GROUP_SIZE,
        )

        self._mtimes[date_str] = mtime
        logger.info("Built %s (%d features, %d row groups)",
                     out_dir / "data.parquet", len(gdf),
                     (len(gdf) + ROW_GROUP_SIZE - 1) // ROW_GROUP_SIZE)
        return True
