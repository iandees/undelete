"""Write deleted OSM features to daily line-delimited GeoJSON files."""

import json
from pathlib import Path


class GeoJSONWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def append(self, feature: dict, date_str: str):
        """Append a GeoJSON feature to the daily file for the given date."""
        daily_file = self.output_dir / f"{date_str}.geojsonl"
        with open(daily_file, "a") as f:
            f.write(json.dumps(feature, separators=(",", ":")) + "\n")

    def list_daily_files(self) -> dict[str, Path]:
        """Return a dict mapping date strings to their file paths."""
        result = {}
        for f in sorted(self.output_dir.glob("*.geojsonl")):
            date_str = f.stem
            result[date_str] = f
        return result
