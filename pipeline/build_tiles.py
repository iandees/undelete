"""Build PMTiles from daily GeoJSON files using tippecanoe."""

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


class TileBuilder:
    def __init__(self, deletions_dir: Path, tiles_dir: Path):
        self.deletions_dir = Path(deletions_dir)
        self.tiles_dir = Path(tiles_dir)
        self.tiles_dir.mkdir(parents=True, exist_ok=True)

    def build_daily_tiles(self) -> list[str]:
        """Build PMTiles for any daily GeoJSON files that are new or updated.
        Returns list of date strings that were built."""
        built = []
        for geojsonl_file in sorted(self.deletions_dir.glob("*.geojsonl")):
            date_str = geojsonl_file.stem
            pmtiles_file = self.tiles_dir / f"{date_str}.pmtiles"
            if pmtiles_file.exists():
                if pmtiles_file.stat().st_mtime > geojsonl_file.stat().st_mtime:
                    continue
            self._run_tippecanoe(geojsonl_file, pmtiles_file)
            built.append(date_str)
        return built

    def build_tiles(self, input_file: Path, output_file: Path):
        """Build PMTiles from a GeoJSON file."""
        self._run_tippecanoe(input_file, output_file)

    def _run_tippecanoe(self, input_file: Path, output_file: Path):
        """Run tippecanoe on a single GeoJSON file."""
        cmd = [
            "tippecanoe", "--force", "--no-tile-size-limit", "--no-progress-indicator",
            "-o", str(output_file), "-l", "deletions",
            "--minimum-zoom=0",
            "--no-feature-limit",
            "--drop-rate=1",
            "--extend-zooms-if-still-dropping",
            str(input_file),
        ]
        logger.info("Building tiles: %s -> %s", input_file.name, output_file.name)
        subprocess.run(cmd, check=True)
