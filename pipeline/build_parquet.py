import json
from pathlib import Path

import geopandas as gpd
import shapely.geometry
import shapely.wkb


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

        gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

        out_dir = self.parquet_dir / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(out_dir / "data.parquet")

        self._mtimes[date_str] = mtime
        return True
